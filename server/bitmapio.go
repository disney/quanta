package server

//
// This file contains the bitmap I/O and low level persistance functions for the bitmap server.
//

import (
	"fmt"
	u "github.com/araddon/gou"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Partition - Description of partition
type Partition struct {
	Index			string
	Field			string
	RowIDOrBits		int64
	Time			time.Time
	TQType			string
	HasStrings		bool
	IsPK			bool
}

// PartitionOperation - Partition operation
type PartitionOperation struct {
	*Partition
	RemoveOnly		bool
	newPath			string
}

func (m *BitmapIndex) NewPartitionOperation(p *Partition, removeOnly bool) *PartitionOperation {

	m.tableCacheLock.RLock()
	defer m.tableCacheLock.RUnlock()
	table := m.tableCache[p.Index]
	if table == nil {
		u.Errorf("NewPartitionOperation: assertion fail table is nil")
		return nil
	}
	pka, err := table.GetPrimaryKeyInfo()
	if err != nil {
		u.Errorf("NewPartitionOperation: assertion fail GetPrimaryKeyInfo: %v", err)
		return nil
	}
	p.IsPK =  p.Field == pka[0].FieldName
	return &PartitionOperation{Partition: p, RemoveOnly: removeOnly}
}

// Persist a standard bitmap field to disk
func (m *BitmapIndex) saveCompleteBitmap(bm *StandardBitmap, indexName, fieldName string, rowID int64,
		ts time.Time) error {

	data, err := bm.Bits.MarshalBinary()
	if err != nil {
		return err
	}

	fd, err := m.openCompleteFile(indexName, fieldName, rowID, ts, bm.TQType)
	if err == nil {
		if _, err := fd[0].Write(data); err != nil {
			return err
		}
		if err := fd[0].Close(); err != nil {
			return err
		}
		return nil
	}
	return err
}

// Persist a BSI field to disk
func (m *BitmapIndex) saveCompleteBSI(bsi *BSIBitmap, indexName, fieldName string, bits int,
		ts time.Time) error {

	if fds, err := m.openCompleteFile(indexName, fieldName, int64(bits*-1), ts,
		bsi.TQType); err == nil {
		data, err := bsi.MarshalBinary()
		if err != nil {
			return err
		}

		for i := 1; i < len(data); i++ {
			if _, err := fds[i].Write(data[i]); err != nil {
				return err
			}
			if err := fds[i].Close(); err != nil {
				return err
			}
		}
		// Write out EBM
		if _, err := fds[0].Write(data[0]); err != nil {
			return err
		}
		if err := fds[0].Close(); err != nil {
			return err
		}
	} else {
		return err
	}
	return nil
}

// Move data from active use to the archive directory path
func (m *BitmapIndex) executeOperation(aop *PartitionOperation) error {

	oldPath := m.generateBitmapFilePath(aop.Partition, false)
	newPath := m.generateBitmapFilePath(aop.Partition, true)
	aop.newPath = newPath

	if aop.RowIDOrBits >= 0 {
		if aop.RemoveOnly {
			return os.Remove(oldPath)
		}
		return os.Rename(oldPath, newPath)
	}
	if err := filepath.Walk(oldPath, aop.perform); err != nil {
		return err
	}
	if aop.HasStrings {
		var iname string
		oldPath, iname = m.generateStringsFilePath(aop, false)
		localKV := m.Node.GetNodeService("KVStore").(*KVStore)
		localKV.closeStore(iname)
		newPath, _ = m.generateStringsFilePath(aop, true)
		if err := filepath.Walk(oldPath, aop.perform); err != nil {
			return err
		}
	}
	return nil
}

func (po *PartitionOperation) perform(path string, info os.FileInfo, err error) error {

	if err != nil {
		return err
	}
	if info.IsDir() {
		return nil
	}
	if po.RemoveOnly {
		return os.Remove(path)
	}
	return os.Rename(path, po.newPath+sep+info.Name())
}

// Figure out the appropriate file path given type BSI/Standard and applicable time quantum
func (m *BitmapIndex) generateBitmapFilePath(aop *Partition, isArchivePath bool) string {

	// field is a BSI if rowIDOrBits < 0
	leafDir := "bsi"
	if aop.RowIDOrBits >= 0 {
		leafDir = fmt.Sprintf("%d", aop.RowIDOrBits)
	}
	baseDir := m.dataDir + sep + "bitmap" + sep + aop.Index + sep + aop.Field + sep + leafDir
	if isArchivePath {
		baseDir = m.dataDir + sep + "archive" + sep + aop.Index + sep + aop.Field + sep + leafDir
	}
	fname := "default"
	if aop.TQType == "YMD" {
		fname = aop.Time.Format(timeFmt)
	}
	if aop.TQType == "YMDH" {
		baseDir = baseDir + sep + fmt.Sprintf("%d%02d%02d", aop.Time.Year(), aop.Time.Month(), aop.Time.Day())
		fname = aop.Time.Format(timeFmt)
	}
	if leafDir == "bsi" {
		baseDir = baseDir + sep + fname
		fname = ""
	}
	os.MkdirAll(baseDir, 0755)
	return baseDir + sep + fname
}

// Figure out the appropriate file path for backing strings file
func (m *BitmapIndex) generateStringsFilePath(aop *PartitionOperation, isArchivePath bool) (string, string) {

	baseDir := m.dataDir + sep + "index" + sep + aop.Index + sep + aop.Field + sep
	if isArchivePath {
		baseDir = m.dataDir + sep + "archive" + sep + aop.Index + sep + aop.Field + sep + "strings"
	}

	fname := "default"
	if aop.TQType == "YMD" {
		fname = aop.Time.Format(timeFmt)
	}
	if aop.TQType == "YMDH" {
		baseDir = baseDir + sep + fmt.Sprintf("%d%02d%02d", aop.Time.Year(), aop.Time.Month(), aop.Time.Day())
		fname = aop.Time.Format(timeFmt)
	}
	os.MkdirAll(baseDir, 0755)
	return baseDir + sep + fname, fmt.Sprintf("%s%s%s%s%s", aop.Index, sep, aop.Field, sep, fname)
}

// Return open file descriptor(s) for writing
func (m *BitmapIndex) openCompleteFile(index, field string, rowIDOrBits int64, ts time.Time,
		tqType string) ([]*os.File, error) {

	// if the bitmap file is a BSI (rowidOrBits < 0) then return an array of open file handles in low
	// to high bit significance order.  For BSI, RowIDOrBits is the number of bits as a negative value.
	// For StandardBitmap just use this value as rowID
	operation := &Partition{Index: index, Field: field, Time: ts, TQType: tqType, RowIDOrBits: rowIDOrBits}
	path := m.generateBitmapFilePath(operation, false)
	var err error
	f := make([]*os.File, 1)
	numFiles := 1
	i := 0
	if rowIDOrBits < 0 {
		// Open numfiles + 1 (extra one for EBM)
		numFiles = int(rowIDOrBits*-1) + 1
		i = 1
		f = make([]*os.File, numFiles)
		// EBM is at fd[0]
		f[0], err = os.OpenFile(path+sep+"EBM", os.O_CREATE|os.O_WRONLY, 0666)
	}
	for ; i < numFiles; i++ {
		fpath := path
		if numFiles > 1 {
			fpath = path + sep + fmt.Sprintf("%d", i)
		}
		f[i], err = os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}

// Called during server startup.  Iterates filesystem and loads up fragement queue.
func (m *BitmapIndex) readBitmapFiles(fragQueue chan *BitmapFragment) error {

	m.fragFileLock.Lock()
	defer m.fragFileLock.Unlock()

	baseDir := m.dataDir + sep + "bitmap"

	var fragMap = make(map[string]map[string]map[int64]map[int64]*BitmapFragment)

	err := filepath.Walk(baseDir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			bf := &BitmapFragment{ModTime: info.ModTime(), IsInit: true}

			data, err := ioutil.ReadFile(path)
			if err != nil {
				u.Errorf("readBitmapFiles: ioutil.ReadFile - %v", err)
				return err
			}

			trPath := strings.Replace(path, baseDir+sep, "", 1)

			s := strings.Split(trPath, sep)
			if len(s) < 4 {
				err := fmt.Errorf("readBitmapFiles: Could not parse path [%s]", path)
				u.Error(err)
				return err
			}
			bf.IndexName = s[0]
			bf.FieldName = s[1]
			attr, _ := m.getFieldConfig(bf.IndexName, bf.FieldName)
			var tq string
			if attr != nil {
				tq = attr.TimeQuantumType
			}
			bf.IsBSI = m.isBSI(bf.IndexName, bf.FieldName)

			if bf.IsBSI {
				if tq != "" {
					ts, err := time.Parse(timeFmt, s[len(s)-2])
					if err != nil {
						err := fmt.Errorf("readBitmapFiles: %s[%s] Could not parse '%s' Time[%s] - %v",
							bf.IndexName, bf.FieldName, s[len(s)-2], tq, err)
						u.Error(err)
						return err
					}
					bf.Time = ts
				} else if s[len(s)-2] == "default" {
					bf.Time = time.Unix(0, 0)
				}
				bitSliceIndex := -1
				if s[len(s)-1] == "EBM" {
					bitSliceIndex = 0
				} else {
					val, err := strconv.ParseInt(s[len(s)-1], 10, 64)
					if err != nil {
						err := fmt.Errorf("readBitmapFiles: Could not parse BSI Bit file - %v", err)
						u.Error(err)
						return err
					}
					bitSliceIndex = int(val)
				}

				if _, ok := fragMap[bf.IndexName]; !ok {
					fragMap[bf.IndexName] = make(map[string]map[int64]map[int64]*BitmapFragment)
				}
				if _, ok := fragMap[bf.IndexName][bf.FieldName]; !ok {
					fragMap[bf.IndexName][bf.FieldName] = make(map[int64]map[int64]*BitmapFragment)
				}
				if _, ok := fragMap[bf.IndexName][bf.FieldName][int64(-1)]; !ok {
					fragMap[bf.IndexName][bf.FieldName][int64(-1)] = make(map[int64]*BitmapFragment)
				}
				if existFrag, ok := fragMap[bf.IndexName][bf.FieldName][int64(-1)][bf.Time.UnixNano()]; !ok {
					if bitSliceIndex == -1 {
						err := fmt.Errorf("readBitmapFiles: Should not be here bitslice must be zero here")
						u.Error(err)
						return err
					}
					// first bitslice start at bf.BitData[1].  bf.BitData[0] = EBM
					bf.BitData = make([][]byte, 65)
					bf.BitData[bitSliceIndex] = data
					fragMap[bf.IndexName][bf.FieldName][int64(-1)][bf.Time.UnixNano()] = bf
				} else {
					// merge in new bits
					existFrag.BitData[bitSliceIndex] = data
					existFrag.ModTime = info.ModTime().Add(time.Second * -10)
				}
			} else {
				bf.RowIDOrBits, err = strconv.ParseInt(s[2], 10, 64)
				if err != nil {
					err := fmt.Errorf("readBitmapFiles: Could not parse RowID - %v", err)
					u.Error(err)
					return err
				}
				if s[len(s)-1] == "default" {
					bf.Time = time.Unix(0, 0)
				} else {
					ts, err := time.Parse(timeFmt, s[len(s)-1])
					if err != nil {
						err := fmt.Errorf("readBitmapFiles: %s[%s] Could not parse '%s' - %v",
							bf.IndexName, bf.FieldName, s[len(s)-1], err)
						u.Error(err)
						return err
					}
					bf.Time = ts
				}
				if _, ok := fragMap[bf.IndexName]; !ok {
					fragMap[bf.IndexName] = make(map[string]map[int64]map[int64]*BitmapFragment)
				}
				if _, ok := fragMap[bf.IndexName][bf.FieldName]; !ok {
					fragMap[bf.IndexName][bf.FieldName] = make(map[int64]map[int64]*BitmapFragment)
				}
				rID := bf.RowIDOrBits
				if _, ok := fragMap[bf.IndexName][bf.FieldName][rID]; !ok {
					fragMap[bf.IndexName][bf.FieldName][rID] = make(map[int64]*BitmapFragment)
				}
				if _, ok := fragMap[bf.IndexName][bf.FieldName][rID][bf.Time.UnixNano()]; !ok {
					bf.BitData = [][]byte{data}
					fragMap[bf.IndexName][bf.FieldName][rID][bf.Time.UnixNano()] = bf
				} else {
					err := fmt.Errorf("readBitmapFiles: Should not be here for standard bitmaps! [%s/%s]",
						bf.IndexName, bf.FieldName)
					u.Error(err)
					return err
				}
			}
			return nil
		})
	if err != nil {
		u.Errorf("filepath.Walk - %v", err)
		return err
	}

	if len(fragMap) == 0 {
		return nil
	}

	for _, index := range fragMap {
		for _, field := range index {
			for _, ts := range field {
				for _, frag := range ts {
					fragQueue <- frag
				}
			}
		}
	}
	return nil
}

// Purge a partition from cache
func (m *BitmapIndex) purgePartition(aop *Partition) {

	t := aop.Time.UnixNano()
	if aop.RowIDOrBits > 0 {
		rowID := uint64(aop.RowIDOrBits)
		m.bitmapCacheLock.Lock()
		defer m.bitmapCacheLock.Unlock()
		if _, ok := m.bitmapCache[aop.Index][aop.Field][rowID][t]; ok {
			delete(m.bitmapCache[aop.Index][aop.Field][rowID], t)
		} 
	} else {
		m.bsiCacheLock.Lock()
		defer m.bsiCacheLock.Unlock()
		if _, ok := m.bsiCache[aop.Index][aop.Field][t]; ok {
			delete(m.bsiCache[aop.Index][aop.Field], t)
		} 
	}
}

