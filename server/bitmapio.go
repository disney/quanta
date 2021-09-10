package server

//
// This file contains the bitmap I/O and low level persistance functions for the bitmap server.
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

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
func (m *BitmapIndex) archiveOrTruncateData(index, field string, rowIDOrBits int64, ts time.Time,
	tqType string, archive bool) error {

	oldPath := m.generateFilePath(index, field, rowIDOrBits, ts, tqType, false)
	newPath := m.generateFilePath(index, field, rowIDOrBits, ts, tqType, true)

	if rowIDOrBits >= 0 {
        if archive {
		    return os.Rename(oldPath, newPath)
        } else {
		    return os.Remove(oldPath)
        }
	}

	return filepath.Walk(oldPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
        if archive {
		    return os.Rename(path, newPath+sep+info.Name())
        } else {
		    return os.Remove(path)
        }
	})
}

// Figure out the appropriate file path given type BSI/Standard and applicable time quantum
func (m *BitmapIndex) generateFilePath(index, field string, rowIDOrBits int64, ts time.Time,
	tqType string, archive bool) string {

	// field is a BSI if rowIDOrBits < 0
	leafDir := "bsi"
	if rowIDOrBits >= 0 {
		leafDir = fmt.Sprintf("%d", rowIDOrBits)
	}
	baseDir := m.dataDir + sep + "bitmap" + sep + index + sep + field + sep + leafDir
	if archive {
		baseDir = m.dataDir + sep + "archive" + sep + index + sep + field + sep + leafDir
	}
	fname := "default"
	if tqType == "YMD" {
		fname = ts.Format(timeFmt)
	}
	if tqType == "YMDH" {
		baseDir = baseDir + sep + fmt.Sprintf("%d%02d%02d", ts.Year(), ts.Month(), ts.Day())
		fname = ts.Format(timeFmt)
	}
	if leafDir == "bsi" {
		baseDir = baseDir + sep + fname
		fname = ""
	}
	os.MkdirAll(baseDir, 0755)
	return baseDir + sep + fname
}

// Return open file descriptor(s) for writing
func (m *BitmapIndex) openCompleteFile(index, field string, rowIDOrBits int64, ts time.Time,
	tqType string) ([]*os.File, error) {

	// if the bitmap file is a BSI (rowidOrBits < 0) then return an array of open file handles in low
	// to high bit significance order.  For BSI, RowIDOrBits is the number of bits as a negative value.
	// For StandardBitmap just use this value as rowID
	path := m.generateFilePath(index, field, rowIDOrBits, ts, tqType, false)
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
				log.Printf("readBitmapFiles: ioutil.ReadFile - %v", err)
				return err
			}

			trPath := strings.Replace(path, baseDir+sep, "", 1)

			s := strings.Split(trPath, sep)
			if len(s) < 4 {
				err := fmt.Errorf("readBitmapFiles: Could not parse path [%s]", path)
				log.Println(err)
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
						log.Println(err)
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
						log.Println(err)
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
						log.Println(err)
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
					log.Println(err)
					return err
				}
				if s[len(s)-1] == "default" {
					bf.Time = time.Unix(0, 0)
				} else {
					ts, err := time.Parse(timeFmt, s[len(s)-1])
					if err != nil {
						err := fmt.Errorf("readBitmapFiles: %s[%s] Could not parse '%s' - %v",
							bf.IndexName, bf.FieldName, s[len(s)-1], err)
						log.Println(err)
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
					log.Println(err)
					return err
				}
			}
			return nil
		})
	if err != nil {
		log.Printf("filepath.Walk - %v", err)
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
