package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/akrylysov/pogreb"
	u "github.com/araddon/gou"
	pb "github.com/disney/quanta/grpc"
	"github.com/disney/quanta/shared"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	"os"
	"reflect"
	"strings"
	"time"
)

// SyncStatus - Sync handshake.
/*  SyncStatusRequest
    Index       string    // Table name.
    Field       string    // Field name.
    RowId       int64     // RowID value (ignored for BSI types)
    Time        int64     // Timestamp for data partition key.
    Cardinality uint64    // Local item cardinality to be checked against remote item.
    BsiChecksum int64     // Local BSI sum to be checked against remote item (BSI only).
    ModTime     int64     // Local modification timestamp to be checked against remote item.
    SendData    bool      // If true and the remote check fails, the remote will return bitmap/bsi data.
*/
/*  SyncStatusResponse
    Ok          bool      // Local and remote data matches.
    Cardinality uint64    // Remote cardinality
    BsiChecksum int64     // Remote BSI sum.
    ModTime     int64     // Remote modification timestamp.
    Data        [][]byte  // Remote data (if applicable).
*/
func (m *BitmapIndex) SyncStatus(ctx context.Context, req *pb.SyncStatusRequest) (*pb.SyncStatusResponse, error) {

	if req.Index == "" {
		return nil, fmt.Errorf("index not specified for sync status")
	}
	if req.Field == "" {
		return nil, fmt.Errorf("field not specified for sync status")
	}

	// Silently ignore non-existing fields for now.  TODO: Re-evaluate this.
	attr, err := m.getFieldConfig(req.Index, req.Field)
	if err != nil {
		return nil, err
	}

	tq := attr.Parent.TimeQuantumType
	if tq != "" && req.Time == 0 {
		return nil, fmt.Errorf("time not specified for sync status and time quantum is enabled for %s", req.Index)
	}

	isBSI := m.isBSI(req.Index, req.Field)
	response := &pb.SyncStatusResponse{}
	var skew time.Duration
	reqTime := time.Unix(0, req.ModTime)
	if isBSI {
		v := m.bsiCache[req.Index][req.Field][req.Time]
		if v == nil {
			return response, nil
		}
		v.Lock.RLock()
		defer v.Lock.RUnlock()
		sum, card := v.Sum(v.GetExistenceBitmap())

		response.Cardinality = uint64(card)
		response.ModTime = v.ModTime.UnixNano()
		response.BSIChecksum = int64(sum)
		if v.ModTime.After(reqTime) {
			skew = v.ModTime.Sub(reqTime)
		} else {
			skew = reqTime.Sub(v.ModTime)
		}
		cardDiff := response.Cardinality - req.Cardinality
		sumDiff := response.BSIChecksum - req.BSIChecksum
		response.Ok = cardDiff == 0 && sumDiff == 0 && skew.Milliseconds() < 100
		if !response.Ok && req.SendData {
			ba, err := v.MarshalBinary()
			if err != nil {
				return response, err
			}
			response.Data = ba
		}
	} else {
		v := m.bitmapCache[req.Index][req.Field][req.RowId][req.Time]
		if v == nil {
			return response, nil
		}
		v.Lock.RLock()
		defer v.Lock.RUnlock()
		response.Cardinality = v.Bits.GetCardinality()
		response.ModTime = v.ModTime.UnixNano()
		if v.ModTime.After(reqTime) {
			skew = v.ModTime.Sub(reqTime)
		} else {
			skew = reqTime.Sub(v.ModTime)
		}
		cardDiff := response.Cardinality - req.Cardinality
		response.Ok = cardDiff == 0 && skew.Milliseconds() < 100
		if !response.Ok && req.SendData {
			buf, err := v.Bits.ToBytes()
			if err != nil {
				return response, err
			}
			ba := make([][]byte, 1)
			ba[0] = buf
			response.Data = ba
		}
	}

	return response, nil
}

// Synchronize - Connect to peer to push deltas (new nodes receive data)
func (m *BitmapIndex) Synchronize(ctx context.Context, req *wrappers.StringValue) (*empty.Empty, error) {

	// This is the entire synchronization flow.  Connect to new node and push data.
	newNodeID := req.Value

	// TODO: Re-evaluate
	ci := m.GetClientIndexForNodeID(newNodeID)
	if ci == -1 {
		u.Errorf("GetClientForNodeID = %s failed.", newNodeID)
		os.Exit(1)
	}
	targetIP := m.ClientConnections()[ci].Target()

	cx, cancel := context.WithTimeout(context.Background(), shared.Deadline)
	defer cancel()

	// Invoke status IP on new node.
	status, err := m.Admin[ci].Status(cx, &empty.Empty{})
	if err != nil {
		return &empty.Empty{}, fmt.Errorf(fmt.Sprintf("%v.Status(_) = _, %v, node = %s\n", m.Admin[ci], err, targetIP))
	}

	// Verify that client stub IP (targetIP) is the same as the new nodes IP returned by status.
	if !strings.HasPrefix(targetIP, status.LocalIP) {
		return &empty.Empty{}, fmt.Errorf("Stub IP %v does not match new node (remote) = %v", targetIP, status.LocalIP)
	}

	u.Infof("New joining node %s is requesting a sync push.", newNodeID)

	peerClient := m.Conn.GetService("BitmapIndex").(*shared.BitmapIndex)
	newNode := peerClient.Client(ci) // <- bitmap peer client for new node.
	peerKVClient := m.Conn.GetService("KVStore").(*shared.KVStore)
	newKVClient := peerKVClient.Client(ci) // <- kvStore peer client for new node.

	// Push UserRoles
	err = m.simpleKVPush(peerKVClient, newKVClient, "UserRoles", reflect.String, reflect.String)
	if err != nil {
		u.Errorf("simpleKVPush: error pushing UserRoles - %v", err)
	}

	// Iterate over BSI cache
	for indexName, index := range m.bsiCache {
		// Perform table level pre-processing here.
		for fieldName, field := range index {
			attr, err := m.getFieldConfig(indexName, fieldName)
			if err != nil {
				return &empty.Empty{},
					fmt.Errorf("Synchronize field metadata lookup failed for %s.%s", indexName, fieldName)
			}
			for t, bsi := range field {
				// Should this item be pushed to new node?
				key := fmt.Sprintf("%s/%s/%s", indexName, fieldName, time.Unix(0, t).Format(timeFmt))
				nMap := m.getNodeMapForKey(newNodeID, key)
				replica, found := nMap[newNodeID]
				if !found {
					continue // nope soldier on
				}
				u.Infof("Key %s should be replica %d on node %s.", key, replica, newNodeID)
				// invoke status check
				bsi.Lock.RLock()
				sum, card := bsi.Sum(bsi.GetExistenceBitmap())
				reqs := &pb.SyncStatusRequest{Index: indexName, Field: fieldName, Time: t, SendData: true,
					Cardinality: card,
					BSIChecksum: sum,
					ModTime:     bsi.ModTime.UnixNano(),
				}
				res, err := newNode.SyncStatus(cx, reqs)
				bsi.Lock.RUnlock()
				if err != nil {
					return &empty.Empty{},
						fmt.Errorf(fmt.Sprintf("%v.SyncStatus(_) = _, %v, node = %s\n", newNode, err, targetIP))
				}
				if res.Ok {
					u.Infof("No differences for key %s.", key)
					continue // data matches
				}
				// Unmarshal data from response
				resBsi := roaring64.NewBSI(int64(attr.MaxValue), int64(attr.MinValue))
				if len(res.Data) != 1 && res.Cardinality > 0 {
					return &empty.Empty{},
						fmt.Errorf("deserialize sync response - BSI index out of range %d, Index = %s, Field = %s",
							len(res.Data), indexName, fieldName)
				}
				if len(res.Data) == 1 && res.Cardinality > 0 {
					if err := resBsi.UnmarshalBinary(res.Data); err != nil {
						return &empty.Empty{},
							fmt.Errorf("deserialize sync reponse - BSI UnmarshalBinary error - %v", err)
					}
				}
				// Calculate the diff
				// TODO: How do we handle sequencer queue?
				// What about value differences where BSIs intersect?
				pushDiff := roaring64.AndNot(bsi.GetExistenceBitmap(), resBsi.GetExistenceBitmap())
				pullDiff := roaring64.AndNot(resBsi.GetExistenceBitmap(), bsi.GetExistenceBitmap())
				pushBSI := bsi.NewBSIRetainSet(pushDiff)
				pullBSI := resBsi.NewBSIRetainSet(pullDiff)

				if pushDiff.GetCardinality() > 0 {
					u.Infof("Pushing server diff for key %s, local = %d, remote (new) = %d, delta = %d.\n", key,
						bsi.GetExistenceBitmap().GetCardinality(), resBsi.GetExistenceBitmap().GetCardinality(),
						pushDiff.GetCardinality())
					err := m.pushBSIDiff(peerClient, newNode, indexName, fieldName, t, pushBSI)
					if err != nil {
						return &empty.Empty{}, fmt.Errorf("pushBSIDiff failed - %v", err)
					}
				}
				// "OR" in the localDiff
				if pullDiff.GetCardinality() > 0 {
					u.Infof("Merging remote diff for key %s, local = %d, remote (new) = %d, delta = %d.\n", key,
						bsi.GetExistenceBitmap().GetCardinality(), resBsi.GetExistenceBitmap().GetCardinality(),
						pullDiff.GetCardinality())
					err := m.mergeBSIDiff(indexName, fieldName, t, pullBSI)
					if err != nil {
						return &empty.Empty{}, fmt.Errorf("mergeBSIDiff failed - %v", err)
					}
				}
				// Process backing strings for StringHashBSI
				if attr.MappingStrategy == "StringHashBSI" {
					if err := m.syncStringBackingStore(peerKVClient, newKVClient, indexName, fieldName, t,
						pushDiff, pullDiff); err != nil {
						u.Errorf("String backing store sync failed for '%s' - %v", key, err)
					}
				}
			}
		}
		// Perform table level post-processing after all attributes are sorted.
		// Table level index checking.  Indices are always BSIs
		m.tableCacheLock.RLock()
		table := m.tableCache[indexName]
		if table == nil {
			// Should never end up here
			m.tableCacheLock.RUnlock()
			u.Errorf("Assertion failed looking up table %s, exiting.", indexName)
			os.Exit(1)
		}
		m.tableCacheLock.RUnlock()
		// Process PK Index
		pkIndex := fmt.Sprintf("%s%s%s.PK", indexName, sep, table.PrimaryKey)
		err := m.simpleKVPush(peerKVClient, newKVClient, pkIndex, reflect.String, reflect.Uint64)
		if err != nil {
			u.Errorf("simpleKVPush: error pushing PK %s for table %s - %v", pkIndex, indexName, err)
		}
		// Process SK Indices if any
		for _, v := range strings.Split(table.SecondaryKeys, ",") {
			skIndex := fmt.Sprintf("%s%s%s.SK", indexName, sep, v)
			err := m.simpleKVPush(peerKVClient, newKVClient, skIndex, reflect.String, reflect.Uint64)
			if err != nil {
				u.Errorf("simpleKVPush: error pushing SK %s for table %s - %v", skIndex, indexName, err)
			}
		}

	}

	// Iterate over standard bitmap cache.
	for indexName, index := range m.bitmapCache {
		for fieldName, field := range index {
			// If field is StringEnum, sync metadata
			if attr, err := m.getFieldConfig(indexName, fieldName); err == nil {
				if attr.MappingStrategy == "StringEnum" {
					if err := m.syncEnumMetadata(peerKVClient, newKVClient, indexName, fieldName); err != nil {
						u.Errorf("StringEnum metadata sync failed for '%s.%s' - %v", indexName, fieldName, err)
					}
				}
			}
			for rowID, ts := range field {
				for t, bitmap := range ts {
					// Should this item be pushed to new node?
					key := fmt.Sprintf("%s/%s/%d/%s", indexName, fieldName, rowID, time.Unix(0, t).Format(timeFmt))
					nMap := m.getNodeMapForKey(newNodeID, key)
					replica, found := nMap[newNodeID]
					if !found {
						continue // nope soldier on
					}
					u.Infof("Key %s should be replica %d on node %s.", key, replica, newNodeID)
					// invoke status check
					bitmap.Lock.RLock()
					reqs := &pb.SyncStatusRequest{Index: indexName, Field: fieldName, RowId: rowID, Time: t, SendData: true,
						Cardinality: bitmap.Bits.GetCardinality(),
						ModTime:     bitmap.ModTime.UnixNano(),
					}
					res, err := newNode.SyncStatus(cx, reqs)
					bitmap.Lock.RUnlock()
					if err != nil {
						return &empty.Empty{},
							fmt.Errorf(fmt.Sprintf("%v.SyncStatus(_) = _, %v, node = %s\n", newNode, err, targetIP))
					}
					if res.Ok {
						u.Infof("No differences for key %s.", key)
						continue // data matches
					}
					// Unmarshal data from response
					resBm := roaring64.NewBitmap()
					if len(res.Data) != 1 && res.Cardinality > 0 {
						return &empty.Empty{},
							fmt.Errorf("deserialize sync response - Index out of range %d, Index = %s, Field = %s, rowID = %d",
								len(res.Data), indexName, fieldName, rowID)
					}
					if len(res.Data) == 1 && res.Cardinality > 0 {
						if err := resBm.UnmarshalBinary(res.Data[0]); err != nil {
							return &empty.Empty{},
								fmt.Errorf("deserialize sync reponse - UnmarshalBinary error - %v", err)
						}
					}
					// Calculate the diff
					pushDiff := roaring64.AndNot(bitmap.Bits, resBm)
					pullDiff := roaring64.AndNot(resBm, bitmap.Bits)

					if pushDiff.GetCardinality() > 0 {
						u.Infof("Pushing server diff for key %s, local = %d, remote (new) = %d, delta = %d.\n", key,
							bitmap.Bits.GetCardinality(), resBm.GetCardinality(), pushDiff.GetCardinality())
						err := m.pushBitmapDiff(peerClient, newNode, indexName, fieldName, rowID, t, pushDiff)
						if err != nil {
							return &empty.Empty{}, fmt.Errorf("pushBitmapDiff failed - %v", err)
						}
					}
					// "OR" in the localDiff
					if pullDiff.GetCardinality() > 0 {
						u.Infof("Merging remote diff for key %s, local = %d, remote (new) = %d, delta = %d.\n", key,
							bitmap.Bits.GetCardinality(), resBm.GetCardinality(), pullDiff.GetCardinality())
						err := m.mergeBitmapDiff(indexName, fieldName, rowID, t, pullDiff)
						if err != nil {
							return &empty.Empty{}, fmt.Errorf("pullBitmapDiff failed - %v", err)
						}
					}
				}
			}
		}
	}

	return &empty.Empty{}, nil
}

// Return a map of weighted nodes for a given lookup key
func (m *BitmapIndex) getNodeMapForKey(newNodeID, key string) map[string]int {

	newHashTable := m.Conn.GetHashTableWithNewNodes([]string{newNodeID})
	nodeKeys := newHashTable.GetN(m.Conn.Replicas, newNodeID)
	nodeMap := make(map[string]int, m.Conn.Replicas)
	for i, v := range nodeKeys {
		nodeMap[v] = i
	}
	return nodeMap
}

func (m *BitmapIndex) pushBitmapDiff(peerClient *shared.BitmapIndex, newNode pb.BitmapIndexClient, index, field string,
	rowID uint64, ts int64, diff *roaring64.Bitmap) error {

	batch := make(map[string]map[string]map[uint64]map[int64]*roaring64.Bitmap, 0)
	tm := make(map[int64]*roaring64.Bitmap, 0)
	tm[ts] = diff
	rm := make(map[uint64]map[int64]*roaring64.Bitmap, 0)
	rm[rowID] = tm
	fm := make(map[string]map[uint64]map[int64]*roaring64.Bitmap, 0)
	fm[field] = rm
	batch[index] = fm
	// cleanup memory on exit
	defer func() {
		delete(tm, ts)
		delete(rm, rowID)
		delete(fm, field)
		delete(batch, index)
	}()

	err := peerClient.BatchMutateNode(false, newNode, batch)
	if err != nil {
		return fmt.Errorf("pushBitmapDiff failed - %v", err)
	}
	return nil
}

// mergeBitmapDiff - "OR" in remote difference to local bitmap cache.
func (m *BitmapIndex) mergeBitmapDiff(index, field string, rowID uint64, ts int64, diff *roaring64.Bitmap) error {

	m.bitmapCacheLock.Lock()
	// TODO:  How we will handle "exclusive" fields?
	sbm, ok := m.bitmapCache[index][field][rowID][ts]
	if !ok {
		m.bitmapCacheLock.Unlock()
		return fmt.Errorf("mergeBitmapDiff failed cache item missing for index = %s, field = %s, rowID = %d, time = %d",
			index, field, rowID, ts)
	}
	sbm.Lock.Lock()
	defer sbm.Lock.Unlock()
	m.bitmapCacheLock.Unlock()
	sbm.Bits = roaring64.ParOr(0, sbm.Bits, diff)
	sbm.ModTime = time.Now()

	return nil
}

// synchronize StringEnum metadata with remote.
func (m *BitmapIndex) syncEnumMetadata(peerKV *shared.KVStore, remoteKV pb.KVStoreClient, index, field string) error {

	localKV := m.Node.GetNodeService("KVStore").(*KVStore)
	kvPath := fmt.Sprintf("%s%s%s.StringEnum", index, sep, field)
	db, err := localKV.getStore(kvPath)
	if err != nil {
		return fmt.Errorf("syncEnumMetadata:getStore failed for %s.%s - %v", index, field, err)
	}

	localBatch := make(map[interface{}]interface{}, 0)
	it := db.Items()
	for {
		key, val, err := it.Next()
		if err != nil {
			if err != pogreb.ErrIterationDone {
				return fmt.Errorf("syncEnumMetadata:db.Items failed for %s.%s - %v", index, field, err)
			}
			break
		}
		localBatch[string(key)] = binary.LittleEndian.Uint64(val)
	}

	var remoteBatch, pushBatch, pullBatch map[interface{}]interface{}
	remoteBatch, err = peerKV.NodeItems(remoteKV, kvPath, reflect.String, reflect.Uint64)
	if err != nil {
		return fmt.Errorf("syncEnumMetadata:remoteKV.Items failed for %s.%s - %v", index, field, err)
	}

	// Iterate local batch and create push batch
	// This creates new values where they are missing but what happens if key/value pairs have different values?
	for k, v := range localBatch {
		if rval, found := remoteBatch[k]; found {
			// TODO: Make sure rowIDs match
			if rval != v {
				u.Infof("StringEnum metadata For %s.%s remote has value %d, local has %d for %s", index, field, rval, v, k)
			}
			continue
		}
		u.Infof("StringEnum metadata For %s.%s remote is missing %s:%d", index, field, k, v)
		pushBatch[k] = v
	}

	// Iterate remote batch and create pull batch
	for k, v := range remoteBatch {
		if lval, found := localBatch[k]; found {
			// TODO: Make sure rowIDs match
			if lval != v {
				u.Infof("StringEnum metadata For %s.%s local has value %d, remote has %d for %s", index, field, lval, v, k)
			}
			continue
		}
		u.Infof("StringEnum metadata For %s.%s local is missing %s:%d", index, field, k, v)
		pullBatch[k] = v
	}

	// TODO: Pass this as a parameter
	verifyOnly := true
	if verifyOnly {
		return nil
	}

	// Begin writes

	// Push to remote
	err = peerKV.BatchPutNode(remoteKV, kvPath, pushBatch)
	if err != nil {
		return fmt.Errorf("syncEnumMetadata:remoteKV.BatchPut failed for %s.%s - %v", index, field, err)
	}

	// Update local
	defer db.Sync()
	for k, v := range pullBatch {
		if err := db.Put(shared.ToBytes(k), shared.ToBytes(v)); err != nil {
			return fmt.Errorf("syncEnumMetadata:db.Put failed for %s.%s - %v", index, field, err)
		}
	}

	return nil
}

func (m *BitmapIndex) pushBSIDiff(peerClient *shared.BitmapIndex, newNode pb.BitmapIndexClient, index, field string,
	ts int64, diff *roaring64.BSI) error {

	batch := make(map[string]map[string]map[int64]*roaring64.BSI, 0)
	tm := make(map[int64]*roaring64.BSI, 0)
	tm[ts] = diff
	fm := make(map[string]map[int64]*roaring64.BSI, 0)
	fm[field] = tm
	batch[index] = fm
	// cleanup memory on exit
	defer func() {
		delete(tm, ts)
		delete(fm, field)
		delete(batch, index)
	}()

	err := peerClient.BatchSetValueNode(newNode, batch)
	if err != nil {
		return fmt.Errorf("pushBSIDiff failed - %v", err)
	}
	return nil
}

// mergeBSIDiff - set the remote difference to local bsi cache.
func (m *BitmapIndex) mergeBSIDiff(index, field string, ts int64, diff *roaring64.BSI) error {

	m.bsiCacheLock.Lock()
	sbsi, ok := m.bsiCache[index][field][ts]
	if !ok {
		m.bsiCacheLock.Unlock()
		return fmt.Errorf("mergeBSIDiff failed cache item missing for index = %s, field = %s, time = %d",
			index, field, ts)
	}
	sbsi.Lock.Lock()
	defer sbsi.Lock.Unlock()
	m.bsiCacheLock.Unlock()
	sbsi.ClearValues(diff.GetExistenceBitmap())
	sbsi.ParOr(0, diff)
	sbsi.ModTime = time.Now()

	return nil
}

// Synchronize string backing store with remote.
func (m *BitmapIndex) syncStringBackingStore(peerKV *shared.KVStore, remoteKV pb.KVStoreClient, index, field string,
	ts int64, pushDiff, pullDiff *roaring64.Bitmap) error {

	timeStr := time.Unix(0, ts).Format(timeFmt)

	localKV := m.Node.GetNodeService("KVStore").(*KVStore)
	kvPath := fmt.Sprintf("%s%s%s%s%s", index, sep, field, sep, timeStr)
	db, err := localKV.getStore(kvPath)
	if err != nil {
		return fmt.Errorf("syncStringBackingStore:getStore failed for %s.%s.%s - %v", index, field, timeStr, err)
	}

	// TODO: Pass this as a parameter
	verifyOnly := true

	// Iterate over columnID values in the remote existence bitmap diff and lookup values in local backing store
	pushBatch := make(map[interface{}]interface{}, pushDiff.GetCardinality())
	foundCount := 0
	for _, v := range pushDiff.ToArray() {
		val, err := db.Get(shared.ToBytes(v))
		if err != nil {
			return fmt.Errorf("syncStringBackingStore:db.Get - %v", err)
		}
		if val != nil {
			foundCount++
			pushBatch[v] = string(val)
		}
	}

	// Iterate over columnID values in the local existence bitmap diff and lookup values in the remote backing store
	pullBatch := make(map[interface{}]interface{}, pullDiff.GetCardinality())
	for _, v := range pullDiff.ToArray() {
		pullBatch[v] = ""
	}

	pullBatch, err = peerKV.BatchLookupNode(remoteKV, kvPath, pullBatch)
	if err != nil {
		return fmt.Errorf("syncStringBackingStore:remoteKV.BatchLookupNode failed for %s.%s.%s - %v", index, field,
			timeStr, err)
	}

	if verifyOnly {
		if foundCount > 0 {
			u.Infof("Remote is missing %d backing strings for %s.%s.%s.", foundCount, index, field, timeStr)
		}
		if len(pullBatch) > 0 {
			u.Infof("Local is missing %d backing strings for %s.%s.%s.", len(pullBatch),
				index, field, timeStr)
		}
		return nil
	}

	// Begin writes

	// Push to remote
	if foundCount > 0 {
		err = peerKV.BatchPutNode(remoteKV, kvPath, pushBatch)
		if err != nil {
			return fmt.Errorf("syncStringBackingStore:remoteKV.BatchPut failed for %s.%s.%s - %v", index, field, timeStr, err)
		}
	}

	// Update local
	if len(pullBatch) > 0 {
		defer db.Sync()
		for k, v := range pullBatch {
			if err := db.Put(shared.ToBytes(k), shared.ToBytes(v)); err != nil {
				return fmt.Errorf("syncStringBackingStore:db.Put failed for %s.%s.%s - %v", index, field, timeStr, err)
			}
		}
	}

	return nil
}

// Synchronize index (PK/SK)  backing store with remote.
func (m *BitmapIndex) simpleKVPush(peerKV *shared.KVStore, remoteKV pb.KVStoreClient, kvPath string,
	keyType, valType reflect.Kind) error {

	localKV := m.Node.GetNodeService("KVStore").(*KVStore)
	db, err := localKV.getStore(kvPath)
	if err != nil {
		return fmt.Errorf("simpleKVPush:getStore failed for %s - %v", kvPath, err)
	}

	pushBatch := make(map[interface{}]interface{}, 0)
	it := db.Items()
	for {
		key, val, err := it.Next()
		if err != nil {
			if err != pogreb.ErrIterationDone {
				return fmt.Errorf("simpleKVPush:db.Items failed for %s - %v", kvPath, err)
			}
			break
		}
		pushBatch[shared.UnmarshalValue(keyType, key)] = shared.UnmarshalValue(valType, val)
	}
	if len(pushBatch) > 0 {
		err = peerKV.BatchPutNode(remoteKV, kvPath, pushBatch)
		if err != nil {
			return fmt.Errorf("simpleKVPush:remoteKV.BatchPut failed for %s - %v", kvPath, err)
		}
	}

	return nil
}
