package server

//
// This file contains the main processing flows for the bitmap server.
//

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/Jeffail/tunny"
	"github.com/RoaringBitmap/roaring/roaring64"
	u "github.com/araddon/gou"
	pb "github.com/disney/quanta/grpc"
	"github.com/disney/quanta/shared"
	"github.com/golang/protobuf/ptypes/empty"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	timeFmt = "2006-01-02T15"
)

//
// BitmapIndex - Main state structure for bitmap indices.
//
// bitmapCache - In memory storage for "standard" bitmaps.
// bsiCache - In memory storage for BSI values.
// fragQueue - All cache mutation operations pass through a fragment queue (including server startup reads)
// workers - Count of worker threads assigned to process mutations.
// setBitThreads - Used to identify when incoming API SetBatch calls have fallen to zero triggering writes.
// writeSignal - Channel used by setBitThreads to initiate write operations to persist cache items.
// tableCache - Schema metadata cache (essentially same YAML file used by loader).
//
type BitmapIndex struct {
	*Node
	expireDays      int
	bitmapCache     map[string]map[string]map[uint64]map[int64]*StandardBitmap
	bitmapCacheLock sync.RWMutex
	bsiCache        map[string]map[string]map[int64]*BSIBitmap
	bsiCacheLock    sync.RWMutex
	fragQueue       chan *BitmapFragment
	workers         int
	fragFileLock    sync.Mutex
	setBitThreads   *CountTrigger
	writeSignal     chan bool
	tableCache      map[string]*shared.BasicTable
	tableCacheLock  sync.RWMutex
}

// NewBitmapIndex - Construct and initialize bitmap server state.
func NewBitmapIndex(node *Node, expireDays int) *BitmapIndex {

	e := &BitmapIndex{Node: node}
	e.expireDays = expireDays
	e.tableCache = make(map[string]*shared.BasicTable)
	configPath := e.dataDir + sep + "config"
	schemaPath := ""          // this is normally an empty string forcing schema to come from Consul
	if e.ServicePort == 0 { // In-memory test harness
		schemaPath = configPath // read schema from local config yaml
		_ = filepath.Walk(configPath,
			func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() {
					index := info.Name()
					if _, err := os.Stat(path + sep + "schema.yaml"); err != nil {
						return nil
					}
					if table, err := shared.LoadSchema(schemaPath, index, nil); err != nil {
						u.Errorf("ERROR: Could not load schema for %s - %v", index, err)
						os.Exit(1)
					} else {
						e.tableCache[index] = table
						u.Infof("Index %s initialized.", index)
					}
				}
				return nil
			})
	} else { // Normal (from Consul) initialization
		var tables []string
		err := shared.Retry(5, 2*time.Second, func() (err error) {
			tables, err = shared.GetTables(e.consul)
			return
		})
		if err != nil {
			u.Errorf("could not load table schema, GetTables error %v", err)
			os.Exit(1)
		}
		for _, table := range tables {
			if t, err := shared.LoadSchema(schemaPath, table, e.consul); err != nil {
				u.Errorf("could not load schema for %s - %v", table, err)
				os.Exit(1)
			} else {
				e.tableCache[table] = t
				u.Infof("Table %s initialized.", table)
			}
		}
	}

	pb.RegisterBitmapIndexServer(e.server, e)
	return e
}

// Init - Initialization
func (m *BitmapIndex) Init() error {

	//m.fragQueue = make(chan *BitmapFragment, 20000000)
	m.fragQueue = make(chan *BitmapFragment, 10000000)
	m.bitmapCache = make(map[string]map[string]map[uint64]map[int64]*StandardBitmap)
	m.bsiCache = make(map[string]map[string]map[int64]*BSIBitmap)
	m.workers = 20
	m.writeSignal = make(chan bool, 1)
	m.setBitThreads = NewCountTrigger(m.writeSignal)

	for i := 0; i < m.workers; i++ {
		go m.batchProcessLoop(i + 1)
	}

	// Read files from disk
	m.readBitmapFiles(m.fragQueue)

	if m.expireDays > 0 {
		u.Infof("Starting data expiration thread - expiration after %d days.", m.expireDays)
		go m.expireProcessLoop(m.expireDays)
	} else {
		u.Info("Data expiration thread disabled.")
	}
	return nil
}

// Shutdown - Shut down and clean up.
func (m *BitmapIndex) Shutdown() {
}

// BatchMutate API call (used by client SetBit call for bulk loading data)
func (m *BitmapIndex) BatchMutate(stream pb.BitmapIndex_BatchMutateServer) error {

	m.setBitThreads.Add(1)
	defer m.setBitThreads.Add(-1)

	for {
		kv, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&empty.Empty{})
		}
		if err != nil {
			return err
		}
		if kv == nil {
			return fmt.Errorf("KV Pair must not be nil")
		}
		if kv.Key == nil || len(kv.Key) == 0 {
			return fmt.Errorf("key must be specified")
		}

		s := strings.Split(kv.IndexPath, "/")
		if len(s) != 2 {
			err = fmt.Errorf("IndexPath %s not valid", kv.IndexPath)
			u.Errorf("%s", err)
			return err
		}
		indexName := s[0]
		fieldName := s[1]

		// Silently ignore non-existing fields for now
		_, err = m.getFieldConfig(indexName, fieldName)
		if err != nil {
			continue
		}

		rowIDOrBits := int64(binary.LittleEndian.Uint64(kv.Key))
		isBSI := m.isBSI(indexName, fieldName)
		ts := time.Unix(0, kv.Time)

		select {
		case m.fragQueue <- newBitmapFragment(indexName, fieldName, rowIDOrBits, ts, kv.Value,
			isBSI, kv.IsClear, false):
		default:
			// Fragment queue is full
			u.Errorf("BatchMutate: fragment queue is full!")
		}
	}
}

// StandardBitmap is just a wrapper around the roaring libraries for simple bitmap fields.
type StandardBitmap struct {
	Bits        *roaring64.Bitmap
	ModTime     time.Time
	PersistTime time.Time
	Lock        sync.RWMutex
	TQType      string
	Exclusive   bool
}

func (m *BitmapIndex) newStandardBitmap(index, field string) *StandardBitmap {

	attr, err := m.getFieldConfig(index, field)
	var timeQuantumType string
	var exclusive bool
	if err == nil {
		timeQuantumType = attr.TimeQuantumType
		exclusive = attr.Exclusive
	}
	return &StandardBitmap{Bits: roaring64.NewBitmap(), ModTime: time.Now(),
		TQType: timeQuantumType, Exclusive: exclusive}
}

// BSIBitmap represents integer values
type BSIBitmap struct {
	*roaring64.BSI
	ModTime        time.Time
	PersistTime    time.Time
	Lock           sync.RWMutex
	TQType         string
	sequencerQueue *SequencerQueue
}

func (m *BitmapIndex) newBSIBitmap(index, field string) *BSIBitmap {

	attr, err := m.getFieldConfig(index, field)
	var minValue, maxValue int64
	var timeQuantumType string
	if err == nil {
		timeQuantumType = attr.TimeQuantumType
		minValue = int64(attr.MinValue)
		maxValue = int64(attr.MaxValue)
	}
	var seq *SequencerQueue
	//if attr.Parent.PrimaryKey != "" && attr.Parent.PrimaryKey == attr.FieldName {
	if attr.Parent.PrimaryKey != "" {
		pkInfo, _ := attr.Parent.GetPrimaryKeyInfo()
		if attr.FieldName == pkInfo[0].FieldName {
			// If compound key, sequencer installed on first key attr
			seq = NewSequencerQueue()
			if maxValue == 0 {
				maxValue = math.MaxInt64
			}
		}
	}
	return &BSIBitmap{BSI: roaring64.NewBSI(maxValue, minValue),
		TQType: timeQuantumType, ModTime: time.Now(), sequencerQueue: seq}
}

// BitmapFragment is just a work unit for cache mutation operations.
type BitmapFragment struct {
	IndexName   string
	FieldName   string
	RowIDOrBits int64     // Row ID or BSI bit count number (negative values = BSI bitcount)
	Time        time.Time // Time for time quantum
	BitData     [][]byte
	ModTime     time.Time // Modification time stamp
	IsBSI       bool
	IsClear     bool // Is this a clear operation?  Othewise set bits.
	IsUpdate    bool
	IsInit      bool // Is this fragment part of init disk read?
}

func newBitmapFragment(index, field string, rowIDOrBits int64, ts time.Time, f [][]byte,
	isBSI, isClear, isUpdate bool) *BitmapFragment {
	return &BitmapFragment{IndexName: index, FieldName: field, RowIDOrBits: rowIDOrBits, Time: ts,
		BitData: f, ModTime: time.Now(), IsBSI: isBSI, IsClear: isClear, IsUpdate: isUpdate}
}

// Lookup field metadata (time quantum, exclusivity)
func (m *BitmapIndex) getFieldConfig(index, field string) (*shared.BasicAttribute, error) {

	m.tableCacheLock.RLock()
	defer m.tableCacheLock.RUnlock()
	table := m.tableCache[index]
	attr, err := table.GetAttribute(field)
	if err != nil {
		return nil, fmt.Errorf("getFieldConfig ERROR: Non existent attribute %s for index %s was referenced",
			field, index)
	}
	if attr.TimeQuantumType == "" && table.TimeQuantumType != "" {
		attr.TimeQuantumType = table.TimeQuantumType
	}
	return attr, nil
}

// Check metadata - Is the field a BSI?
func (m *BitmapIndex) isBSI(index, field string) bool {

	m.tableCacheLock.RLock()
	defer m.tableCacheLock.RUnlock()
	table := m.tableCache[index]
	attr, err := table.GetAttribute(field)
	if err != nil {
		u.Errorf("attribute %s for index %s does not exist", field, index)
	}
	return attr.IsBSI()
}

//
// Worker thread.
//
// Read entries from fragment queue that were uploaded by the client SetBit/SetValue
// operations. If there is a write signal then call the persistence code.  Write signals
// are triggered when fragment queue activity tails off.  This serves to prioritize memory updates
// over disk I/O (which occurs asynchronously).
//
// The weird repetition in the select statement below is a go hack for prioritizing work.  Select
// case processing order is non-deterministic.
//
func (m *BitmapIndex) batchProcessLoop(threadID int) {

	for {
		// This is a way to make sure that the fraq queue has priority over persistence.
		select {
		case frag := <-m.fragQueue:
			if frag.IsBSI {
				m.updateBSICache(frag)
			} else {
				m.updateBitmapCache(frag)
			}
			continue
		default: // Don't block
		}

		select {
		case frag := <-m.fragQueue:
			if frag.IsBSI {
				m.updateBSICache(frag)
			} else {
				m.updateBitmapCache(frag)
			}
			continue
		case <-m.writeSignal:
			go m.checkPersistBitmapCache(false)
			go m.checkPersistBSICache(false)
			continue
		case <-time.After(time.Second * 10):
			m.checkPersistBitmapCache(true)
			m.checkPersistBSICache(true)
		}
	}
}

/*
 * Expiration process thread.
 *
 * Wake up on interval and run data expiration process.
 */
func (m *BitmapIndex) expireProcessLoop(days int) {

	select {
	case <-time.After(time.Minute * 10):
		m.expireOrTruncate(days, false)
	}
}

// Updates to the standard bitmap field cache
func (m *BitmapIndex) updateBitmapCache(f *BitmapFragment) {

	// If the rowID exists then merge in the new set of bits
	start := time.Now()
	newBm := m.newStandardBitmap(f.IndexName, f.FieldName)
	newBm.ModTime = f.ModTime
	if f.IsInit {
		newBm.PersistTime = f.ModTime
	}
	if len(f.BitData) != 1 {
		u.Errorf("updateBitmapCache - Index out of range %d, Index = %s, Field = %s",
			len(f.BitData), f.IndexName, f.FieldName)
	}
	if err := newBm.Bits.UnmarshalBinary(f.BitData[0]); err != nil {
		u.Errorf("updateBitmapCache - UnmarshalBinary error - %v", err)
		return
	}
	rowID := uint64(f.RowIDOrBits)
	m.bitmapCacheLock.Lock()
	if newBm.Exclusive && !f.IsClear && f.IsUpdate {
		//Handle exclusive "updates"
		m.clearAllRows(f.IndexName, f.FieldName, f.Time.UnixNano(), newBm.Bits)
	}
	if _, ok := m.bitmapCache[f.IndexName][f.FieldName][rowID][f.Time.UnixNano()]; !ok && f.IsUpdate {
		// Silently ignore attempts to update data not in local cache that is not in hashKey
		// because updates are sent to all nodes
		hashKey := fmt.Sprintf("%s/%s/%d/%s", f.IndexName, f.FieldName, rowID, f.Time.Format(timeFmt))
		if !m.Member(hashKey) { // not here and not a member
			m.bitmapCacheLock.Unlock()
			return
		}
	}
	if _, ok := m.bitmapCache[f.IndexName]; !ok {
		m.bitmapCache[f.IndexName] = make(map[string]map[uint64]map[int64]*StandardBitmap)
	}
	if _, ok := m.bitmapCache[f.IndexName][f.FieldName]; !ok {
		m.bitmapCache[f.IndexName][f.FieldName] = make(map[uint64]map[int64]*StandardBitmap)
	}
	if _, ok := m.bitmapCache[f.IndexName][f.FieldName][rowID]; !ok {
		m.bitmapCache[f.IndexName][f.FieldName][rowID] = make(map[int64]*StandardBitmap)
	}
	if existBm, ok := m.bitmapCache[f.IndexName][f.FieldName][rowID][f.Time.UnixNano()]; !ok {
		m.bitmapCache[f.IndexName][f.FieldName][rowID][f.Time.UnixNano()] = newBm
		m.bitmapCacheLock.Unlock()
	} else {
		// Lock de-escalation
		existBm.Lock.Lock()
		m.bitmapCacheLock.Unlock()
		if f.IsClear {
			roaring64.ClearBits(newBm.Bits, existBm.Bits)
		} else {
			existBm.Bits = roaring64.ParOr(0, existBm.Bits, newBm.Bits)
		}
		existBm.ModTime = f.ModTime
		if f.IsInit {
			existBm.PersistTime = f.ModTime
		}
		existBm.Lock.Unlock()
	}
	elapsed := time.Since(start)
	if elapsed.Nanoseconds() > (1000000 * 25) {
		u.Debugf("updateBitmapCache [%s/%s/%d/%s] done in %v.\n", f.IndexName, f.FieldName,
			rowID, f.Time.Format(timeFmt), elapsed)
	}
}

// ClearParams struct encapsulates parameters to the clear bits worker pool.
type ClearParams struct {
	FoundSet *roaring64.Bitmap
	Target   *StandardBitmap
}

func (m *BitmapIndex) clearAllRows(index, field string, ts int64, nbm *roaring64.Bitmap) {

	var wg sync.WaitGroup

	if f, ok := m.bitmapCache[index][field]; ok {
		for _, tm := range f {
			if bm, ok2 := tm[ts]; ok2 {
				wg.Add(1)
				go func(b *StandardBitmap) {
					defer wg.Done()
					b.Lock.Lock()
					defer b.Lock.Unlock()
					roaring64.ClearBits(nbm, b.Bits)
					b.ModTime = time.Now()
				}(bm)
			} else {
				continue
			}
		}
	}
	wg.Wait()
}

func (m *BitmapIndex) clearAll(index string, start, end int64, nbm *roaring64.Bitmap) {

	m.bitmapCacheLock.Lock()
	m.bsiCacheLock.Lock()
	defer m.bitmapCacheLock.Unlock()
	defer m.bsiCacheLock.Unlock()

	numCPUs := runtime.NumCPU()

	pool := tunny.NewFunc(numCPUs, func(payload interface{}) interface{} {
		params := payload.(ClearParams)
		//u.Debugf("ClearBits %s.%s ROW: %d - %s", params.Index, params.Field, params.RowID, time.Unix(0, params.Timestamp).Format(timeFmt))
		roaring64.ClearBits(params.FoundSet, params.Target.Bits)
		params.Target.ModTime = time.Now()
		return nil
	})
	defer pool.Close()

	if fm, ok := m.bitmapCache[index]; ok {
		for _, rm := range fm {
			for _, tm := range rm {
				for ts, bitmap := range tm {
					if ts < start || ts > end {
						continue
					}
					pool.Process(ClearParams{FoundSet: nbm, Target: bitmap})
					//, Index: index, Field: fname, RowID: rowID, Timestamp: ts})
				}
			}
		}
	}

	if fm, ok := m.bsiCache[index]; ok {
		for _, tm := range fm {
			for ts, bsi := range tm {
				if ts < start || ts > end {
					continue
				}
				bsi.ClearValues(nbm)
			}
		}
	}
}

// Updates to the standard BSI field value cache
func (m *BitmapIndex) updateBSICache(f *BitmapFragment) {

	start := time.Now()
	newBSI := m.newBSIBitmap(f.IndexName, f.FieldName)
	newBSI.ModTime = f.ModTime
	if f.IsInit {
		newBSI.PersistTime = f.ModTime
	}

	if err := newBSI.UnmarshalBinary(f.BitData); err != nil {
		u.Errorf("updateBSICache - UnmarshalBinary error - %v", err)
		return
	}

	m.bsiCacheLock.Lock()
	if _, ok := m.bsiCache[f.IndexName][f.FieldName][f.Time.UnixNano()]; !ok && f.IsUpdate {
		// Silently ignore attempts to update data not in local cache that is not in hashKey
		// because updates are sent to all nodes
		hashKey := fmt.Sprintf("%s/%s/%s", f.IndexName, f.FieldName, f.Time.Format(timeFmt))
		if !m.Member(hashKey) { // not here and not a member
			m.bsiCacheLock.Unlock()
			return
		}
	}
	if _, ok := m.bsiCache[f.IndexName]; !ok {
		m.bsiCache[f.IndexName] = make(map[string]map[int64]*BSIBitmap)
	}
	if _, ok := m.bsiCache[f.IndexName][f.FieldName]; !ok {
		m.bsiCache[f.IndexName][f.FieldName] = make(map[int64]*BSIBitmap)
	}
	if existBm, ok := m.bsiCache[f.IndexName][f.FieldName][f.Time.UnixNano()]; !ok {
		m.bsiCache[f.IndexName][f.FieldName][f.Time.UnixNano()] = newBSI
		m.bsiCacheLock.Unlock()
	} else {
		// Lock de-escalation
		existBm.Lock.Lock()
		m.bsiCacheLock.Unlock()
		existBm.ParOr(0, newBSI.BSI)
		existBm.ModTime = f.ModTime
		if f.IsInit {
			existBm.PersistTime = f.ModTime
		}
		existBm.Lock.Unlock()
	}
	elapsed := time.Since(start)
	if elapsed.Nanoseconds() > (1000000 * 75) {
		u.Debugf("updateBSICache [%s/%s/%s] done in %v.\n", f.IndexName, f.FieldName,
			f.Time.Format(timeFmt), elapsed)
	}
}

// Truncate - Truncate the in-memory data cache for a given index
func (m *BitmapIndex) Truncate(index string) {

	m.bitmapCacheLock.Lock()
	m.bsiCacheLock.Lock()
	defer m.bitmapCacheLock.Unlock()
	defer m.bsiCacheLock.Unlock()

	fm := m.bitmapCache[index]
	for _, rm := range fm {
		for _, tm := range rm {
			for ts := range tm {
				delete(tm, ts)
			}
		}
	}
	bm := m.bsiCache[index]
	for _, tm := range bm {
		for ts := range tm {
			delete(tm, ts)
		}
	}
}

func (m *BitmapIndex) expireOrTruncate(days int, truncate bool) {

	m.bitmapCacheLock.Lock()
	m.bsiCacheLock.Lock()
	defer m.bitmapCacheLock.Unlock()
	defer m.bsiCacheLock.Unlock()
	expiration := time.Duration(24*days) * time.Hour

	for indexName, fm := range m.bitmapCache {
		for fieldName, rm := range fm {
			for rowID, tm := range rm {
				for ts, bitmap := range tm {
					endDate := time.Unix(0, ts).Add(expiration)
					if endDate.After(time.Now()) {
						if err := m.archiveOrTruncateData(indexName, fieldName, int64(rowID), time.Unix(0, ts),
							bitmap.TQType, truncate); err != nil {
						} else {
							delete(tm, ts)
						}
					} else if truncate {
						delete(tm, ts)
					}
				}
			}
		}
	}

	for indexName, fm := range m.bsiCache {
		for fieldName, tm := range fm {
			for ts, bsi := range tm {
				endDate := time.Unix(0, ts).Add(expiration)
				if endDate.After(time.Now()) {
					if err := m.archiveOrTruncateData(indexName, fieldName, -1, time.Unix(0, ts),
						bsi.TQType, truncate); err != nil {
					} else {
						delete(tm, ts)
					}
				} else if truncate {
					delete(tm, ts)
				}
			}
		}
	}
}

func (m *BitmapIndex) truncateCaches(index string) {

	m.bitmapCacheLock.Lock()
	m.bsiCacheLock.Lock()
	defer m.bitmapCacheLock.Unlock()
	defer m.bsiCacheLock.Unlock()

	fm, _ := m.bitmapCache[index]
	if fm != nil {
		for _, rm := range fm {
			for _, tm := range rm {
				for ts := range tm {
					delete(tm, ts)
				}
			}
		}
	}

	xm, _ := m.bsiCache[index]
	if xm != nil {
		for _, tm := range xm {
			for ts := range tm {
				delete(tm, ts)
			}
		}
	}
}

// Iterate standard bitmap cache looking for potential writes (dirty data)
func (m *BitmapIndex) checkPersistBitmapCache(forceSync bool) {

	if m.ServicePort == 0 {
		return // test mode, persistence disabled
	}

	m.bitmapCacheLock.RLock()
	defer m.bitmapCacheLock.RUnlock()

	writeCount := 0
	start := time.Now()
	for indexName, index := range m.bitmapCache {
		for fieldName, field := range index {
			for rowID, ts := range field {
				for t, bitmap := range ts {
					bitmap.Lock.Lock()
					if bitmap.ModTime.After(bitmap.PersistTime) {
						if err := m.saveCompleteBitmap(bitmap, indexName, fieldName, int64(rowID),
							time.Unix(0, t)); err != nil {
							u.Errorf("saveCompleteBitmap failed! - %v", err)
							bitmap.Lock.Unlock()
							continue
						}
						writeCount++
						bitmap.PersistTime = time.Now()
					}
					bitmap.Lock.Unlock()
				}
			}
		}
	}

	elapsed := time.Since(start)
	if writeCount > 0 {
		if forceSync {
			u.Debugf("Persist [timer expired] %d files done in %v", writeCount, elapsed)
		} else {
			u.Debugf("Persist [edge triggered] %d files done in %v", writeCount, elapsed)
		}
	}
}

// Iterate BSI cache looking for potential writes (dirty data)
func (m *BitmapIndex) checkPersistBSICache(forceSync bool) {

	if m.ServicePort == 0 {
		return // test mode persistence disabled
	}

	m.bsiCacheLock.RLock()
	defer m.bsiCacheLock.RUnlock()

	writeCount := 0
	start := time.Now()
	for indexName, index := range m.bsiCache {
		for fieldName, field := range index {
			for t, bsi := range field {
				bsi.Lock.Lock()
				if bsi.ModTime.After(bsi.PersistTime) {
					if err := m.saveCompleteBSI(bsi, indexName, fieldName, int(bsi.BitCount()),
						time.Unix(0, t)); err != nil {
						u.Errorf("saveCompleteBSI failed! - %v", err)
						bsi.Lock.Unlock()
						return
					}
					writeCount++
					bsi.PersistTime = time.Now()
				}
				bsi.Lock.Unlock()
			}
		}
	}

	elapsed := time.Since(start)
	if writeCount > 0 {
		if forceSync {
			u.Debugf("Persist BSI [timer expired] %d files done in %v", writeCount, elapsed)
		} else {
			u.Debugf("Persist BSI [edge triggered] %d files done in %v", writeCount, elapsed)
		}
	}
}

// BulkClear - Batch "delete".
func (m *BitmapIndex) BulkClear(ctx context.Context, req *pb.BulkClearRequest) (*empty.Empty, error) {

	if req.Index == "" {
		return &empty.Empty{}, fmt.Errorf("index not specified for bulk clear criteria")
	}

	foundSet := roaring64.NewBitmap()
	if err := foundSet.UnmarshalBinary(req.FoundSet); err != nil {
		return &empty.Empty{}, err
	}
	m.clearAll(req.Index, int64(req.FromTime), int64(req.ToTime), foundSet)
	return &empty.Empty{}, nil

}

// Update - Process Updates.
func (m *BitmapIndex) Update(ctx context.Context, req *pb.UpdateRequest) (*empty.Empty, error) {

	if req.Index == "" {
		return &empty.Empty{}, fmt.Errorf("index not specified for update criteria")
	}
	if req.Field == "" {
		return &empty.Empty{}, fmt.Errorf("field not specified for update criteria")
	}
	if req.ColumnId == 0 {
		return &empty.Empty{}, fmt.Errorf("column ID not specified for update criteria")
	}

	// Silently ignore non-existing fields for now
	_, err := m.getFieldConfig(req.Index, req.Field)
	if err != nil {
		return &empty.Empty{}, err
	}

	isBSI := m.isBSI(req.Index, req.Field)
	ts := time.Unix(0, req.Time)
	var frag *BitmapFragment

	if isBSI {
		bsi := roaring64.NewDefaultBSI()
		bsi.SetValue(req.ColumnId, req.RowIdOrValue)
		ba, err := bsi.MarshalBinary()
		if err != nil {
			return &empty.Empty{}, err
		}
		frag = newBitmapFragment(req.Index, req.Field, int64(bsi.BitCount()*-1), ts, ba, isBSI,
			false, true)
	} else {
		bm := roaring64.NewBitmap()
		bm.Add(req.ColumnId)
		buf, err := bm.ToBytes()
		if err != nil {
			return &empty.Empty{}, err
		}
		ba := make([][]byte, 1)
		ba[0] = buf
		frag = newBitmapFragment(req.Index, req.Field, req.RowIdOrValue, ts, ba, isBSI, false, true)
	}

	select {
	case m.fragQueue <- frag:
	default:
		// Fragment queue is full
		u.Errorf("Update: fragment queue is full!")
	}
	return &empty.Empty{}, nil
}

// CheckoutSequence returns another batch of column IDs to the client.
func (m *BitmapIndex) CheckoutSequence(ctx context.Context,
	req *pb.CheckoutSequenceRequest) (*pb.CheckoutSequenceResponse, error) {

	if req.Index == "" {
		return nil, fmt.Errorf("index not specified for sequencer checkout")
	}
	if req.PkField == "" {
		return nil, fmt.Errorf("PK field not specified for sequencer checkout")
	}

	if req.ReservationSize <= 0 {
		return nil, fmt.Errorf("PK field not specified for sequencer checkout")
	}

	m.bsiCacheLock.Lock()
	if _, ok := m.bsiCache[req.Index]; !ok {
		m.bsiCache[req.Index] = make(map[string]map[int64]*BSIBitmap)
	}
	if _, ok := m.bsiCache[req.Index][req.PkField]; !ok {
		m.bsiCache[req.Index][req.PkField] = make(map[int64]*BSIBitmap)
	}
	targetBSI, ok := m.bsiCache[req.Index][req.PkField][req.Time]
	if !ok {
		targetBSI = m.newBSIBitmap(req.Index, req.PkField)
		m.bsiCache[req.Index][req.PkField][req.Time] = targetBSI
	}
	targetBSI.Lock.Lock()
	defer targetBSI.Lock.Unlock()
	m.bsiCacheLock.Unlock()

	/*
	   if !ok {
	       return nil, fmt.Errorf("cannot find BSI for %s [%s] (TS %d)", req.Index, req.PkField, req.Time)
	   }
	*/

	// Get the maximum column id from EBM
	var maxColID uint64
	if targetBSI.GetExistenceBitmap().GetCardinality() > 0 {
		maxColID = targetBSI.GetExistenceBitmap().Maximum()
	}

	// Purge any sequencers that are complete
	targetBSI.sequencerQueue.Purge(maxColID)

	// Get largest checked out maximum. if queue is empty (max = 0), the new start is the maximum column id + 1
	var nextSeqStart uint64
	maxSeq := targetBSI.sequencerQueue.Maximum()
	if maxSeq == 0 {
		if maxColID == 0 {
			// if time quantum enabled then add the timestamp to the starting sequence
			if targetBSI.TQType != "" {
				nextSeqStart = uint64(req.Time) + 1
			} else {
				nextSeqStart = 1
			}
		} else {
			nextSeqStart = maxColID + 1
		}
	} else {
		nextSeqStart = maxSeq + 1
	}
	targetBSI.sequencerQueue.Push(shared.NewSequencer(nextSeqStart, int(req.ReservationSize)))
	res := &pb.CheckoutSequenceResponse{Start: nextSeqStart, Count: req.ReservationSize}
	//u.Debugf("SERVER RESPONSE [Start %d, Count %d] Queue depth = %d", res.Start, res.Count, targetBSI.sequencerQueue.Len())
	return res, nil

}

// CountTrigger sends a message when counter reaches zero
type CountTrigger struct {
	num     int
	lock    sync.Mutex
	trigger chan bool
}

// NewCountTrigger constructs a CountTrigger
func NewCountTrigger(t chan bool) *CountTrigger {
	return &CountTrigger{trigger: t}
}

//
// Add function provides thread safe addition of counter value based on input parameter.
// If counter falls to zero then a value will be sent to trigger channel.
//
func (c *CountTrigger) Add(n int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.num += n
	if c.num == 0 {
		select {
		case c.trigger <- true:
			return
			//default:
			//    return
		}
	}
}

// TableOperation - Process TableOperations.
func (m *BitmapIndex) TableOperation(ctx context.Context, req *pb.TableOperationRequest) (*empty.Empty, error) {

	if req.Table == "" {
		return &empty.Empty{}, fmt.Errorf("table not specified for table operation")
	}

	switch req.Operation {
	case pb.TableOperationRequest_DEPLOY:
		if table, err := shared.LoadSchema("", req.Table, m.consul); err != nil {
			u.Errorf("could not load schema for %s - %v", req.Table, err)
			os.Exit(1)
		} else {
			m.tableCacheLock.Lock()
			defer m.tableCacheLock.Unlock()
			m.tableCache[req.Table] = table
			u.Infof("schema for %s re-loaded and initialized", req.Table)
		}
	case pb.TableOperationRequest_DROP:
		m.tableCacheLock.Lock()
		defer m.tableCacheLock.Unlock()
		delete(m.tableCache, req.Table)
		m.Truncate(req.Table)
		tableDir := m.dataDir + sep + "bitmap" + sep + req.Table
		if err := os.RemoveAll(tableDir); err != nil {
			u.Infof("error dropping table %s directory - %v", req.Table, err)
		} else {
			u.Infof("Table %s dropped.", req.Table)
		}
	case pb.TableOperationRequest_TRUNCATE:
		m.tableCacheLock.Lock()
		defer m.tableCacheLock.Unlock()
		m.Truncate(req.Table)
		tableDir := m.dataDir + sep + "bitmap" + sep + req.Table
		if err := os.RemoveAll(tableDir); err != nil {
			u.Errorf("error truncating table %s directory - %v", req.Table, err)
		} else {
			u.Infof("Table %s truncated.", req.Table)
		}
	default:
		return &empty.Empty{}, fmt.Errorf("unknown operation type for table operation request")
	}

	return &empty.Empty{}, nil
}
