package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/akrylysov/pogreb"
	u "github.com/araddon/gou"
	pb "github.com/disney/quanta/grpc"
	"github.com/disney/quanta/shared"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	"golang.org/x/sync/singleflight"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	// Ensure KVStore implements NodeService
	_ NodeService = (*BitmapIndex)(nil)
)

// KVStore - Server side state for KVStore service.
type KVStore struct {
	*Node
	storeCache     map[string]*pogreb.DB
	storeCacheLock sync.RWMutex
	enumGuard      singleflight.Group
}

// NewKVStore - Construct server side state.
func NewKVStore(node *Node) *KVStore {

	e := &KVStore{Node: node}
	e.storeCache = make(map[string]*pogreb.DB)
	pb.RegisterKVStoreServer(node.server, e)
	return e
}

// Init - Initialize.
func (m *KVStore) Init() error {

	dbList := make([]string, 0)
	err := filepath.Walk(m.Node.dataDir + sep + "index",
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !strings.HasSuffix(path, "/00000.psg") {
				return nil
			}
			dbPath, _ := filepath.Split(path)
			l := strings.Split(dbPath, string(os.PathSeparator))
			if len(l) == 0 {
				return nil
			}
			if l[len(l)-2] != "search.dat" && l[len(l)-2] != "UserRoles" {
				indexName := l[len(l)-3] + sep + l[len(l)-2]
				dbList = append(dbList, indexName)
			}
			return nil
		})
	if err != nil {
		return fmt.Errorf("cannot initialize kv store service: %v", err)
	}

	for _, v := range dbList {
		u.Infof("Opening [%s]", v)
		if _, err := m.getStore(v); err != nil {
			return fmt.Errorf("cannot initialize kv store service: %v", err)
		}
	}
	return nil
}

// Shutdown service.
func (m *KVStore) Shutdown() {
	m.storeCacheLock.Lock()
	defer m.storeCacheLock.Unlock()
	for k, v := range m.storeCache {
		u.Infof("Sync and close [%s]", k)
		v.Sync()
		v.Close()
	}
}

// JoinCluster - Join the cluster
func (m *KVStore) JoinCluster() {
}

func (m *KVStore) getStore(index string) (db *pogreb.DB, err error) {

	m.storeCacheLock.RLock()
	var ok bool
	if db, ok = m.storeCache[index]; ok {
		m.storeCacheLock.RUnlock()
		return
	}
	m.storeCacheLock.RUnlock()

	m.storeCacheLock.Lock()
	defer m.storeCacheLock.Unlock()
	db, err = pogreb.Open(m.Node.dataDir+sep+"index"+sep+index, nil)
	if err == nil {
		m.storeCache[index] = db
	} else {
		err = fmt.Errorf("while opening [%s] - %v", index, err)
	}
	return
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

	updatedMap := make(map[string]*pogreb.DB, 0)    // local cache of DBs updated

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
	m.storeCacheLock.Lock()
	defer m.storeCacheLock.Unlock()
	for k, v := range m.storeCache {
		if strings.HasPrefix(k, req.Prefix) {
			v.Sync()
			v.Close()
			delete(m.storeCache, k)
			if req.RetainEnums && strings.HasSuffix(k, "StringEnum") {
				u.Infof("Sync and close [%s]", k)
				continue
			}
			if err := os.RemoveAll(m.Node.dataDir + sep + "index" + sep + k); err != nil {
				return &empty.Empty{}, fmt.Errorf("DeleteIndicesWithPrefix error [%v]", err)
			}
			u.Infof("Sync, close, and delete [%s]", k)
		}
	}
	if !req.RetainEnums {
		if err := os.RemoveAll(m.Node.dataDir + sep + "index" + sep + req.Prefix); err != nil {
			return &empty.Empty{}, fmt.Errorf("DeleteIndicesWithPrefix error [%v]", err)
		}
	}
	return &empty.Empty{}, nil
}
