package quanta

// Simple KV store wrapper around the pogreb library.   Network enablement via gRPC with
// consistent hashing for scale.  The primary purpose of this is to create a backing store
// for Pym strings.   Also, indexing of high cardinality attributes where fine grained
// access is required.

import (
	"context"
	"fmt"
	pb "github.com/disney/quanta/grpc"
	"github.com/disney/quanta/shared"
	"github.com/golang/protobuf/ptypes/wrappers"
	"golang.org/x/sync/errgroup"
	"io"
	"reflect"
)

// KVStore API wrapper
type KVStore struct {
	*Conn
	client []pb.KVStoreClient
}

// NewKVStore - Construct KVStore service endpoint.
func NewKVStore(conn *Conn) *KVStore {

	clients := make([]pb.KVStoreClient, len(conn.clientConn))
	for i := 0; i < len(conn.clientConn); i++ {
		clients[i] = pb.NewKVStoreClient(conn.clientConn[i])
	}
	return &KVStore{Conn: conn, client: clients}
}

// Put a new attribute
func (c *KVStore) Put(indexPath string, k interface{}, v interface{}, pathIsKey bool) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	key := k
	if pathIsKey {
		key = indexPath
	}
	replicaClients, indices := c.selectNodes(key, false)
	if len(replicaClients) == 0 {
		return fmt.Errorf("%v.Put(_) = _, %v: ", c.client, " no available nodes!")
	}
	// Iterate over replica client list and perform Put operation
	for i, client := range replicaClients {
		_, err := client.Put(ctx, &pb.IndexKVPair{IndexPath: indexPath, Key: shared.ToBytes(k),
			Value: [][]byte{shared.ToBytes(v)}})
		if err != nil {
			return fmt.Errorf("%v.Put(_) = _, %v: [%s]", c.client, err, c.Conn.clientConn[indices[i]].Target())
		}
	}
	return nil
}

func (c *KVStore) splitBatch(batch map[interface{}]interface{}, replicas int) []map[interface{}]interface{} {

	c.Conn.nodeMapLock.RLock()
	defer c.Conn.nodeMapLock.RUnlock()

	batches := make([]map[interface{}]interface{}, len(c.client))
	for i := range batches {
		batches[i] = make(map[interface{}]interface{}, 0)
	}
	for k, v := range batch {
		nodeKeys := c.Conn.hashTable.GetN(replicas, shared.ToString(k))
		/*
		   if len(nodeKeys) == 0 {
		       return fmt.Errorf("%v.splitBatch(_) = _, %v: key: %v", c.client, " no available nodes!", k)
		   }
		*/
		// Iterate over node key list and collate into batches
		for _, nodeKey := range nodeKeys {
			if i, ok := c.Conn.nodeMap[nodeKey]; ok {
				batches[i][k] = v
			}
		}
	}
	return batches
}

// BatchPut - Insert a batch of attributes.
func (c *KVStore) BatchPut(indexPath string, batch map[interface{}]interface{}, pathIsKey bool) error {

	batches := make([]map[interface{}]interface{}, len(c.client))
	for i := range batches {
		batches[i] = make(map[interface{}]interface{}, 0)
	}
	if pathIsKey {
		nodeKeys := c.Conn.hashTable.GetN(c.Conn.Replicas, indexPath)
		for _, nodeKey := range nodeKeys {
			if i, ok := c.Conn.nodeMap[nodeKey]; ok {
				batches[i] = batch
			}
		}
	} else {
		batches = c.splitBatch(batch, c.Replicas)
	}

	done := make(chan error)
	defer close(done)
	count := len(batches)
	for i, v := range batches {
		go func(client pb.KVStoreClient, idx string, m map[interface{}]interface{}) {
			done <- c.batchPut(client, idx, m)
		}(c.client[i], indexPath, v)
	}
	for {
		err := <-done
		if err != nil {
			return err
		}
		count--
		if count == 0 {
			break
		}
	}
	return nil
}

func (c *KVStore) batchPut(client pb.KVStoreClient, index string, batch map[interface{}]interface{}) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	b := make([]*pb.IndexKVPair, len(batch))
	i := 0
	for k, v := range batch {
		b[i] = &pb.IndexKVPair{IndexPath: index, Key: shared.ToBytes(k), Value: [][]byte{shared.ToBytes(v)}}
		i++
	}
	stream, err := client.BatchPut(ctx)
	if err != nil {
		return fmt.Errorf("%v.BatchPut(_) = _, %v: ", c.client, err)
	}

	for i := 0; i < len(b); i++ {
		if err := stream.Send(b[i]); err != nil {
			return fmt.Errorf("%v.Send(%v) = %v", stream, b[i], err)
		}
	}
	_, err2 := stream.CloseAndRecv()
	if err2 != nil {
		return fmt.Errorf("%v.CloseAndRecv() got error %v, want %v", stream, err2, nil)
	}
	return nil
}

// Lookup a single key.
func (c *KVStore) Lookup(indexPath string, k interface{}, valueType reflect.Kind, pathIsKey bool) (interface{}, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	
	key := k
	if pathIsKey {
		key = indexPath
	}
	replicaClients, indices := c.selectNodes(key, false)
	if len(replicaClients) == 0 {
		return nil, fmt.Errorf("%v.Lookup(_) = _, %v: ", c.client, " no available nodes")
	}

	// Use the highest weight client
	lookup, err := replicaClients[0].Lookup(ctx, &pb.IndexKVPair{IndexPath: indexPath, Key: shared.ToBytes(k), Value: nil})
	if err != nil {
		return uint64(0), fmt.Errorf("%v.Lookup(_) = _, %v: [%s]", c.client, err,
			c.Conn.clientConn[indices[0]].Target())
	}
	if lookup.Value != nil && len(lookup.Value) != 0 && len(lookup.Value[0]) != 0 {
		return shared.UnmarshalValue(valueType, lookup.Value[0]), nil
	}
	return nil, nil
}

// BatchLookup of multiple keys.
func (c *KVStore) BatchLookup(indexPath string, batch map[interface{}]interface{}, pathIsKey bool) (map[interface{}]interface{}, error) {

	if pathIsKey {
		nodeKeys := c.Conn.hashTable.GetN(c.Conn.Replicas, indexPath)
		if len(nodeKeys) == 0 {
			return nil, fmt.Errorf("no nodes available")
		}
		if i, ok := c.Conn.nodeMap[nodeKeys[0]]; ok {
			return c.batchLookup(c.client[i], indexPath, batch)
		}
		return nil, fmt.Errorf("no nodes available")
	}

	// We dont want to iterate over replicas for lookups so count is 1, first replica is primary
	batches := c.splitBatch(batch, 1)

	results := make(map[interface{}]interface{}, 0)
	rchan := make(chan map[interface{}]interface{})
	defer close(rchan)
	done := make(chan error)
	defer close(done)
	count := len(batches)
	for i := range batches {
		go func(client pb.KVStoreClient, idx string, b map[interface{}]interface{}) {
			r, e := c.batchLookup(client, idx, b)
			rchan <- r
			done <- e
		}(c.client[i], indexPath, batches[i])

	}
	for {
		b := <-rchan
		if b != nil {
			for k, v := range b {
				results[k] = v
			}
		}
		err := <-done
		if err != nil {
			return nil, err
		}
		count--
		if count == 0 {
			break
		}
	}

	return results, nil
}

func (c *KVStore) batchLookup(client pb.KVStoreClient, index string,
		batch map[interface{}]interface{}) (map[interface{}]interface{}, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	stream, err := client.BatchLookup(ctx)
	if err != nil {
		return nil, fmt.Errorf("%v.BatchLookup(_) = _, %v", c.client, err)
	}

	var keyType, valueType reflect.Kind
	// Just grab first key/value of incoming data to determine types
	for k, v := range batch {
		keyType = reflect.ValueOf(k).Kind()
		valueType = reflect.ValueOf(v).Kind()
		break
	}

	waitc := make(chan struct{})
	results := make(map[interface{}]interface{}, len(batch))
	go func() {
		for {
			kv, err := stream.Recv()
			if err == io.EOF {
				// read done.
				close(waitc)
				return
			}
			if err != nil {
				c.err <- fmt.Errorf("Failed to receive a KV pair : %v", err)
				return
			}
			k := shared.UnmarshalValue(keyType, kv.Key)
			v := shared.UnmarshalValue(valueType, kv.Value[0])
			results[k] = v
		}
	}()
	for k, v := range batch {
		newKV := &pb.IndexKVPair{IndexPath: index, Key: shared.ToBytes(k), Value: [][]byte{shared.ToBytes(v)}}
		if err := stream.Send(newKV); err != nil {
			return nil, fmt.Errorf("Failed to send a KV pair: %v", err)
		}
	}
	stream.CloseSend()
	<-waitc
	return results, nil
}

// Items - Iterate over entire set of items
func (c *KVStore) Items(index string, keyType, valueType reflect.Kind) (map[interface{}]interface{}, error) {

	results := make(map[interface{}]interface{}, 0)
	rchan := make(chan map[interface{}]interface{}, len(c.client))

	var eg errgroup.Group

	for i := range c.client {
		x := c.client[i]
		eg.Go(func() error {
			r, err := c.items(x, index, keyType, valueType)
			if err != nil {
				return err
			}
			rchan <- r
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return results, err
	}
	close(rchan)

	for b := range rchan {
		if b != nil {
			for k, v := range b {
				results[k] = v
			}
		}
	}
	return results, nil
}

func (c *KVStore) items(client pb.KVStoreClient, index string, keyType,

	valueType reflect.Kind) (map[interface{}]interface{}, error) {
	batch := make(map[interface{}]interface{}, 0)
	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	stream, err := client.Items(ctx, &wrappers.StringValue{Value: index})
	if err != nil {
		return nil, fmt.Errorf("%v.Items(_) = _, %v", client, err)
	}
	for {
		item, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%v.Items(_) = _, %v", client, err)
		}
		batch[shared.UnmarshalValue(keyType, item.Key)] = shared.UnmarshalValue(valueType, item.Value[0])
	}

	return batch, nil
}

// PutStringEnum - Put a new enum value
func (c *KVStore) PutStringEnum(index, value string) (uint64, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	replicaClients, indices := c.selectNodes(index, true)
	if len(replicaClients) == 0 {
		return 0, fmt.Errorf("%v.PutStringEnum(_) = _, %v: ", c.client, " no available nodes!")
	}

	// Perform PutStringEnum server call on first node in list (primary)
	// If the value already exists it will silently return the existing rowID
	rowID, err := replicaClients[0].PutStringEnum(ctx, &pb.StringEnum{IndexPath: index, Value: value})
	if err != nil {
		return 0, fmt.Errorf("%v.PutStringEnum(_) = _, %v: [%s]", c.client, err,
			c.Conn.clientConn[indices[0]].Target())
	}

	// Parallel iterate over remaining client list and perform Put operation (replication)
	var eg errgroup.Group
	for _, client := range replicaClients[1:] {
		c := client
		eg.Go(func() error {
			_, err := c.Put(ctx, &pb.IndexKVPair{IndexPath: index, Key: shared.ToBytes(value),
				Value: [][]byte{shared.ToBytes(rowID.Value)}})
			if err != nil {
				return fmt.Errorf("%v.Put(_) = _, %v: ", c, err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return 0, err
	}
	return rowID.Value, nil
}

// Resolve the node location(s) of a single key. If 'all' == true than all nodes are selected.
func (c *KVStore) selectNodes(key interface{}, all bool) ([]pb.KVStoreClient, []int) {

	c.Conn.nodeMapLock.RLock()
	defer c.Conn.nodeMapLock.RUnlock()

	replicas := c.Conn.Replicas
	if all && len(c.Conn.nodes) > 0 {
		replicas = len(c.Conn.nodes)
	}

	nodeKeys := c.Conn.hashTable.GetN(replicas, shared.ToString(key))
	selected := make([]pb.KVStoreClient, len(nodeKeys))
	indices := make([]int, len(nodeKeys))

	for i, v := range nodeKeys {
		if j, ok := c.Conn.nodeMap[v]; ok {
			selected[i] = c.client[j]
			indices[i] = j
		}
	}
	return selected, indices
}

// DeleteIndicesWithPrefix - Delete indices with a table prefix, optionally retain StringEnum data
func (c *KVStore) DeleteIndicesWithPrefix(prefix string, retainEnums bool) error {

	var eg errgroup.Group
	replicaClients, _ := c.selectNodes(prefix, true)
	if len(replicaClients) == 0 {
		return fmt.Errorf("no available nodes")
	}
	for _, client := range replicaClients {
		x := client
		eg.Go(func() error {
			return c.deleteIndicesWithPrefix(x, prefix, retainEnums)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func (c *KVStore) deleteIndicesWithPrefix(client pb.KVStoreClient, prefix string, retainEnums bool) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	_, err := client.DeleteIndicesWithPrefix(ctx,
		&pb.DeleteIndicesWithPrefixRequest{Prefix: prefix, RetainEnums: retainEnums})
	if err != nil {
		return fmt.Errorf("%v.DeleteIndicesWithPrefix(_) = _, %v: ", c, err)
	}
	return nil
}
