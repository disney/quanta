package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/akrylysov/pogreb"
	u "github.com/araddon/gou"
	pb "github.com/disney/quanta/grpc"
	"github.com/disney/quanta/shared"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	"golang.org/x/sync/singleflight"
)

var (
	// Ensure KVStore implements NodeService
	_ NodeService = (*BitmapIndex)(nil)
)

const (
	maxOpenHours = 1.0
)

// KVStore - Server side state for KVStore service.
type KVStore struct {
	*Node
	storeCache     map[string]*cacheEntry
	storeCacheLock sync.RWMutex
	enumGuard      singleflight.Group
	exit           chan bool
	cleanupLatency int64 // current cleanup thread duration (Prometheus)
}

type cacheEntry struct {
	db         *pogreb.DB
	accessTime time.Time
}

// NewKVStore - Construct server side state.
func NewKVStore(node *Node) *KVStore {

	e := &KVStore{Node: node}
	e.exit = make(chan bool, 1)
	e.storeCache = make(map[string]*cacheEntry)
	pb.RegisterKVStoreServer(node.server, e)
	return e
}

// Init - Initialize.
func (m *KVStore) Init() error {

	if m.Node.consul == nil {
		return nil
	}

	start := time.Now()

	tables, err := shared.GetTables(m.Node.consul)
	if err != nil {
		return err
	}

	/*
		lastDay := time.Now().AddDate(0, 0, -1)
	*/

	dbList := make([]string, 0)
	for _, table := range tables {
		tPath := m.Node.dataDir + sep + "index" + sep + table
		os.MkdirAll(tPath, 0755)
		err := filepath.Walk(tPath,
			func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				/*
								if info.ModTime().Before(lastDay) {
					                return nil
								}
				*/
				if strings.HasPrefix(path, ".SK") {
					return nil
				}
				if !strings.HasSuffix(path, "/00000.psg") {
					return nil
				}
				index := table + sep + strings.ReplaceAll(filepath.Dir(path), tPath+"/", "")
				dbList = append(dbList, index)
				return nil
			})
		if err != nil {
			return fmt.Errorf("cannot initialize kv store service: %v", err)
		}
	}

	wg := sync.WaitGroup{}
	var gotError error

	for _, v := range dbList {
		wg.Add(1)
		go func(index string) {
			defer wg.Done()
			_, err := m.getStore(index)
			if err != nil {
				gotError = fmt.Errorf("cannot initialize kv store service: %v", err)
			}
		}(v)
		// u.Infof("KVStore Init Opening [%s]", v)
		// _, err := m.getStore(v)
		// if err != nil {
		// 	return fmt.Errorf("cannot initialize kv store service: %v", err)
		// }
	}

	wg.Wait()

	if gotError != nil {
		return gotError
	}

	elapsed1 := time.Since(start)

	go m.cleanupProcessLoop()

	elapsed2 := time.Since(start)

	fmt.Println(m.hashKey, "KVStore Init elapsed1", elapsed1, "elapsed2", elapsed2)

	return nil
}

// background thread to check and close cached DB entries that haven't been accessed in over 24 hours.
func (m *KVStore) cleanupProcessLoop() {

	for {
		select {
		case _, open := <-m.exit:
			if !open {
				return
			}
		default:
		}
		select {
		case <-time.After(time.Second * 10):
			clusterState, _, _ := m.GetClusterState()
			if m.State == Active && clusterState == shared.Green {
				m.cleanup()
			}
		}
	}
}

// Scan open cache entries and close out indices
func (m *KVStore) cleanup() {

	u.Debug(m.hashKey, " KVStore cleanup")
	m.storeCacheLock.Lock()
	cacheCopy := maps.Clone(m.storeCache) // shallow copy
	m.storeCacheLock.Unlock()

	start := time.Now()

	for k, v := range cacheCopy {
		if time.Since(v.accessTime).Hours() >= maxOpenHours {
			u.Debugf("Closed %v due to inactivity.", k)
			m.closeStore(k)
		}
	}

	elapsed := time.Since(start)
	m.cleanupLatency = elapsed.Milliseconds()
}

// Shutdown service.
func (m *KVStore) Shutdown() {

	u.Debug(m.hashKey, " KVStore Shutdown")

	m.storeCacheLock.Lock()
	defer m.storeCacheLock.Unlock()
	for k, v := range m.storeCache {
		u.Infof("%s Sync and close [%s]", m.hashKey, k)
		v.db.Sync() // waitGroup?
		v.db.Close()
	}
	m.exit <- true
	// ?? close(m.exit)
}

// JoinCluster - Join the cluster
func (m *KVStore) JoinCluster() {
}

func (m *KVStore) getStore(index string) (db *pogreb.DB, err error) {

	m.storeCacheLock.Lock()
	defer m.storeCacheLock.Unlock()

	//m.storeCacheLock.RLock()
	var ok bool
	var ce *cacheEntry
	if ce, ok = m.storeCache[index]; ok {
		//m.storeCacheLock.RUnlock()
		db = ce.db
		ce.accessTime = time.Now()
		return
	}
	/* TODO: This is a potential performance optimization, but it's not clear if it's necessary.
	m.storeCacheLock.RUnlock()

	m.storeCacheLock.Lock()
	defer m.storeCacheLock.Unlock()
	*/
	path := m.Node.dataDir + sep + "index" + sep + index
	// fmt.Println(m.hashKey, "KVStore getStore", path)
	db, err = pogreb.Open(path, nil)
	if err == nil {
		m.storeCache[index] = &cacheEntry{db: db, accessTime: time.Now()}
	} else {
		err = fmt.Errorf("while opening [%s] - %v", index, err)
	}
	return
}

func (m *KVStore) closeStore(index string) {

	m.storeCacheLock.Lock()
	defer m.storeCacheLock.Unlock()
	var ok bool
	var ce *cacheEntry
	if ce, ok = m.storeCache[index]; ok {
		ce.db.Close()
		delete(m.storeCache, index)
	}
}

// Put - Insert a new key
func (m *KVStore) Put(ctx context.Context, kv *pb.IndexKVPair) (*empty.Empty, error) {

	if kv == nil {
		return &empty.Empty{}, fmt.Errorf("KV Pair must not be nil")
	}
	if kv.Key == nil || len(kv.Key) == 0 {
		return &empty.Empty{}, fmt.Errorf("Key must be specified")
	}
	if kv.IndexPath == "" {
		return &empty.Empty{}, fmt.Errorf("Index must be specified")
	}
	db, err := m.getStore(kv.IndexPath)
	if err != nil {
		return &empty.Empty{}, err
	}
	err = db.Put(kv.Key, kv.Value[0])
	if err != nil {
		return &empty.Empty{}, err
	}
	return &empty.Empty{}, nil
}

// Lookup a key
func (m *KVStore) Lookup(ctx context.Context, kv *pb.IndexKVPair) (*pb.IndexKVPair, error) {
	if kv == nil {
		return &pb.IndexKVPair{}, fmt.Errorf("KV Pair must not be nil")
	}
	if kv.Key == nil || len(kv.Key) == 0 {
		return &pb.IndexKVPair{}, fmt.Errorf("Key must be specified")
	}
	if kv.IndexPath == "" {
		return &pb.IndexKVPair{}, fmt.Errorf("Index must be specified")
	}
	db, err := m.getStore(kv.IndexPath)
	if err != nil {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, 0)
		kv.Value = [][]byte{b}
		return kv, fmt.Errorf("Error opening %s - %v", kv.IndexPath, err)
	}
	if db == nil {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, 0)
		kv.Value = [][]byte{b}
		return kv, fmt.Errorf("DB is nil %s", kv.IndexPath)
	}
	val, err := db.Get(kv.Key)
	if err != nil {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, 0)
		kv.Value = [][]byte{b}
		return kv, err
	}
	kv.Value = [][]byte{val}
	return kv, nil
}

// BatchPut - Insert a batch of entries.
func (m *KVStore) BatchPut(stream pb.KVStore_BatchPutServer) error {

	updatedMap := make(map[string]*pogreb.DB, 0) // local cache of DBs updated

	defer func() {
		for _, v := range updatedMap {
			v.Sync()
		}
	}()

	var putCount int32
	for {
		kv, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&empty.Empty{})
		}
		if kv == nil {
			return fmt.Errorf("KV Pair must not be nil")
		}
		if kv.IndexPath == "" {
			return fmt.Errorf("Index must be specified")
		}
		db, err2 := m.getStore(kv.IndexPath)
		if err2 != nil {
			return err2
		}
		if _, found := updatedMap[kv.IndexPath]; !found {
			updatedMap[kv.IndexPath] = db
		}
		if kv.Key == nil || len(kv.Key) == 0 {
			return fmt.Errorf("Key must be specified")
		}
		if kv.Value == nil || len(kv.Value) == 0 {
			return fmt.Errorf("Value must be specified")
		}
		if db == nil {
			return fmt.Errorf("DB is nil for [%s]", kv.IndexPath)
		}
		if err := db.Put(kv.Key, kv.Value[0]); err != nil {
			return err
		}
		putCount++
	}
}

// BatchLookup - Lookup a batch of keys and return values.
func (m *KVStore) BatchLookup(stream pb.KVStore_BatchLookupServer) error {

	for {
		kv, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if kv == nil {
			return fmt.Errorf("KV Pair must not be nil")
		}
		if kv.IndexPath == "" {
			return fmt.Errorf("Index must be specified")
		}
		db, err := m.getStore(kv.IndexPath)
		if err != nil {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, 0)
			kv.Value[0] = b
			return err
		}
		val, err := db.Get(kv.Key)
		if err != nil {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, 0)
			kv.Value[0] = b
			return err
		}
		kv.Value[0] = val
		if err := stream.Send(kv); err != nil {
			return err
		}
	}
}

// Items - Iterate over all items.
// called by grpc stream
func (m *KVStore) Items(index *wrappers.StringValue, stream pb.KVStore_ItemsServer) error {

	if index.Value == "" {
		return fmt.Errorf("Index must be specified")
	}
	db, err := m.getStore(index.Value)
	if err != nil {
		return err
	}

	it := db.Items()
	for {
		key, val, err := it.Next()
		if err != nil {
			if err != pogreb.ErrIterationDone {
				return err
			}
			break
		}
		if err := stream.Send(&pb.IndexKVPair{IndexPath: index.Value, Key: key,
			Value: [][]byte{val}}); err != nil {
			return err
		}
	}
	return nil
}

// PutStringEnum - Insert a new enumeration value and return the new enumeration key (integer sequence).
func (m *KVStore) PutStringEnum(ctx context.Context, se *pb.StringEnum) (*wrappers.UInt64Value, error) {

	if se == nil {
		return &wrappers.UInt64Value{}, fmt.Errorf("StringEnum  must not be nil")
	}
	if se.Value == "" || len(se.Value) == 0 {
		return &wrappers.UInt64Value{}, fmt.Errorf("Value must be specified")
	}
	if se.IndexPath == "" {
		return &wrappers.UInt64Value{}, fmt.Errorf("Index must be specified")
	}
	db, err := m.getStore(se.IndexPath)
	if err != nil {
		return &wrappers.UInt64Value{}, err
	}

	// Guard against multiple requests updating the same enumeration group.
	v, err, _ := m.enumGuard.Do(se.IndexPath, func() (interface{}, error) {

		var greatestRowID uint64
		eMap := make(map[string]uint64)

		it := db.Items()
		for {
			key, v, err := it.Next()
			if err != nil {
				if err != pogreb.ErrIterationDone {
					return 0, err
				}
				break
			}
			r := binary.LittleEndian.Uint64(v)
			if r > greatestRowID {
				greatestRowID = r
			}
			eMap[string(key)] = r
		}

		if rowID, found := eMap[se.Value]; found {
			return rowID, nil
		}
		greatestRowID++

		defer db.Sync()
		return greatestRowID, db.Put(shared.ToBytes(se.Value), shared.ToBytes(greatestRowID))
	})

	if err != nil {
		return &wrappers.UInt64Value{}, err
	}
	return &wrappers.UInt64Value{Value: v.(uint64)}, nil
}

// DeleteIndicesWithPrefix - Close and delete all indices with a specific prefix
func (m *KVStore) DeleteIndicesWithPrefix(ctx context.Context,
	req *pb.DeleteIndicesWithPrefixRequest) (*empty.Empty, error) {

	if req.Prefix == "" {
		return &empty.Empty{}, fmt.Errorf("Index prefix must be specified")
	}

	u.Infof("Deleting index files for prefix %v, retain enums = %v", req.Prefix, req.RetainEnums)

	// Interate over storeCache and close everything currently open for this prefix
	m.storeCacheLock.Lock()
	for k, v := range m.storeCache {
		if strings.HasPrefix(k, req.Prefix + sep) {
			v.db.Sync()
			v.db.Close()
			delete(m.storeCache, k)
			u.Infof("Sync and close [%s]", k)
		}
	}
	m.storeCacheLock.Unlock()

	// retrieve list of indices matching the prefix from the filesystem
	baseDir := m.Node.dataDir + sep + "index" + sep + req.Prefix
	files, err := os.ReadDir(baseDir)
	if err != nil {
		return &empty.Empty{}, fmt.Errorf("DeleteIndicesWithPrefix: %v", err)
	}
	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		k := baseDir + sep + file.Name()
		if !req.RetainEnums {
			if err := os.RemoveAll(baseDir); err != nil {
				return &empty.Empty{}, fmt.Errorf("DeleteIndicesWithPrefix retain is false: error [%v]", err)
			} else {
				u.Infof("Deleted [%s]", k)
			}
		} else {
			if !strings.HasSuffix(k, "StringEnum") {
				if err := os.RemoveAll(k); err != nil {
					return &empty.Empty{}, fmt.Errorf("DeleteIndicesWithPrefix retain is true:error [%v]", err)
				} else {
					u.Infof("Deleted [%s]", k)
				}
			}
		}
	}
	return &empty.Empty{}, nil
}

// IndexInfo - Get information about an index.
func (m *KVStore) IndexInfo(ctx context.Context, req *pb.IndexInfoRequest) (*pb.IndexInfoResponse, error) {

	res := &pb.IndexInfoResponse{}
	if req == nil {
		return res, fmt.Errorf("request must not be nil")
	}
	if req.IndexPath == "" {
		return res, fmt.Errorf("IndexPath must be specified")
	}

	filePath := m.Node.dataDir + sep + "index" + sep + req.IndexPath
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return res, nil
	}

	var err error
	var ce *cacheEntry
	var db *pogreb.DB
	m.storeCacheLock.RLock()
	ce, res.WasOpen = m.storeCache[req.IndexPath]
	m.storeCacheLock.RUnlock()
	if ce == nil {
		db, err = pogreb.Open(filePath, nil)
		if err != nil {
			return res, fmt.Errorf("IndexInfo:Open err - %v", err)
		}
		defer db.Close()
	} else {
		db = ce.db
	}
	res.FileSize, err = db.FileSize()
	if err != nil {
		return res, fmt.Errorf("IndexInfo:FileSize err - %v", err)
	}
	res.Count = db.Count()
	metrics := db.Metrics()
	res.Puts = metrics.Puts.Value()
	res.Gets = metrics.Gets.Value()
	res.Dels = metrics.Dels.Value()
	res.HashCollisions = metrics.HashCollisions.Value()
	return res, nil
}
