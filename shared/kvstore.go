package shared

// Simple KV store wrapper around the pogreb library.   Network enablement via gRPC with
// consistent hashing for scale.  The primary purpose of this is to create a backing store
// for Pym strings.   Also, indexing of high cardinality attributes where fine grained
// access is required.

import (
	"context"
	"fmt"
	pb "github.com/disney/quanta/grpc"
	"github.com/golang/protobuf/ptypes/wrappers"
	"golang.org/x/sync/errgroup"
	"io"
	"reflect"
)

var (
	// Ensure KVStore implements shared.Service
	_ Service = (*KVStore)(nil)
)

// KVStore API wrapper
type KVStore struct {
	*Conn
	client []pb.KVStoreClient
}

// NewKVStore - Construct KVStore service endpoint.
func NewKVStore(conn *Conn) *KVStore {

	clients := make([]pb.KVStoreClient, len(conn.ClientConnections()))
	for i := 0; i < len(conn.ClientConnections()); i++ {
		clients[i] = pb.NewKVStoreClient(conn.ClientConnections()[i])
	}
	c := &KVStore{Conn: conn, client: clients}
	conn.RegisterService(c)
	return c
}

// MemberJoined - A new node joined the cluster.
func (c *KVStore) MemberJoined(nodeID, ipAddress string, index int) {

	c.client = append(c.client, nil)
	copy(c.client[index+1:], c.client[index:])
	c.client[index] = pb.NewKVStoreClient(c.Conn.clientConn[index])
}

// MemberLeft - A node left the cluster.
func (c *KVStore) MemberLeft(nodeID string, index int) {

	if len(c.client) <= 1 {
		c.client = make([]pb.KVStoreClient, 0)
		return
	}
	c.client = append(c.client[:index], c.client[index+1:]...)
}

// Client - Get a client by index.
func (c *KVStore) Client(index int) pb.KVStoreClient {

	return c.client[index]
}

// Put a new attribute
func (c *KVStore) Put(indexPath string, k interface{}, v interface{}, pathIsKey bool) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	key := k
	if pathIsKey {
		key = indexPath
	}
	indices := c.SelectNodes(key, false, false)
	if len(indices) == 0 {
		return fmt.Errorf("%v.Put(_) = _, %v: ", c.client, " no available nodes!")
	}
	// Iterate over replica client list and perform Put operation
	for _, i := range indices {
		_, err := c.client[i].Put(ctx, &pb.IndexKVPair{IndexPath: indexPath, Key: ToBytes(k),
			Value: [][]byte{ToBytes(v)}})
		if err != nil {
			return fmt.Errorf("%v.Put(_) = _, %v: [%s]", c.client[i], err, c.Conn.ClientConnections()[i].Target())
		}
	}
	return nil
}

func (c *KVStore) splitBatch(batch map[interface{}]interface{}) []map[interface{}]interface{} {

	//c.Conn.nodeMapLock.RLock()
	//defer c.Conn.nodeMapLock.RUnlock()

	batches := make([]map[interface{}]interface{}, len(c.client))
	for i := range batches {
		batches[i] = make(map[interface{}]interface{}, 0)
	}
	for k, v := range batch {
		indices := c.SelectNodes(ToString(k), false, false)
		for _, i := range indices {
			batches[i][k] = v
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
		indices := c.SelectNodes(indexPath, false, false)
		for _, i := range indices {
			batches[i] = batch
		}
	} else {
		batches = c.splitBatch(batch)
	}

	done := make(chan error)
	defer close(done)
	count := len(batches)
	for i, v := range batches {
		go func(client pb.KVStoreClient, idx string, m map[interface{}]interface{}) {
			done <- c.BatchPutNode(client, idx, m)
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

// BatchPutNode - Put a batch of keys on a single node.
func (c *KVStore) BatchPutNode(client pb.KVStoreClient, index string, batch map[interface{}]interface{}) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	b := make([]*pb.IndexKVPair, len(batch))
	i := 0
	for k, v := range batch {
		b[i] = &pb.IndexKVPair{IndexPath: index, Key: ToBytes(k), Value: [][]byte{ToBytes(v)}}
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
	indices := c.SelectNodes(key, true, false)
	if len(indices) == 0 {
		return nil, fmt.Errorf("%v.Lookup(_) = _, %v: ", c.client, " no available nodes")
	}

	// Use the highest weight client
	lookup, err := c.client[indices[0]].Lookup(ctx, &pb.IndexKVPair{IndexPath: indexPath, Key: ToBytes(k), Value: nil})
	if err != nil {
		return uint64(0), fmt.Errorf("%v.Lookup(_) = _, %v: [%s]", c.client, err,
			c.Conn.ClientConnections()[indices[0]].Target())
	}
	if lookup.Value != nil && len(lookup.Value) != 0 && len(lookup.Value[0]) != 0 {
		return UnmarshalValue(valueType, lookup.Value[0]), nil
	}
	return nil, nil
}

// BatchLookup of multiple keys.
func (c *KVStore) BatchLookup(indexPath string, batch map[interface{}]interface{}, pathIsKey bool) (map[interface{}]interface{}, error) {

	if pathIsKey {
		indices := c.SelectNodes(indexPath, true, false)
		if len(indices) == 0 {
			return nil, fmt.Errorf("no nodes available")
		}
		return c.BatchLookupNode(c.client[indices[0]], indexPath, batch)
	}

	// We dont want to iterate over replicas for lookups so count is 1, first replica is primary
	batches := c.splitBatch(batch)

	results := make(map[interface{}]interface{}, 0)
	rchan := make(chan map[interface{}]interface{})
	defer close(rchan)
	done := make(chan error)
	defer close(done)
	count := len(batches)
	for i := range batches {
		go func(client pb.KVStoreClient, idx string, b map[interface{}]interface{}) {
			r, e := c.BatchLookupNode(client, idx, b)
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

// BatchLookupNode - Batch lookup of keys on a single node.
func (c *KVStore) BatchLookupNode(client pb.KVStoreClient, index string,
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
				err = fmt.Errorf("Failed to receive a KV pair : %v", err)
				return
			}
			k := UnmarshalValue(keyType, kv.Key)
			v := UnmarshalValue(valueType, kv.Value[0])
			results[k] = v
		}
	}()
	for k, v := range batch {
		newKV := &pb.IndexKVPair{IndexPath: index, Key: ToBytes(k), Value: [][]byte{ToBytes(v)}}
		if err := stream.Send(newKV); err != nil {
			return nil, fmt.Errorf("Failed to send a KV pair: %v", err)
		}
	}
	stream.CloseSend()
	<-waitc
	return results, nil
}

// Items - Iterate over entire set of items across all nodes
func (c *KVStore) Items(index string, keyType, valueType reflect.Kind) (map[interface{}]interface{}, error) {

	results := make(map[interface{}]interface{}, 0)
	rchan := make(chan map[interface{}]interface{}, len(c.client))

	var eg errgroup.Group

	for i := range c.client {
		x := c.client[i]
		eg.Go(func() error {
			r, err := c.NodeItems(x, index, keyType, valueType)
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

// NodeItems - Iterate over all  items on a single node.
func (c *KVStore) NodeItems(client pb.KVStoreClient, index string, keyType,
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
		batch[UnmarshalValue(keyType, item.Key)] = UnmarshalValue(valueType, item.Value[0])
	}

	return batch, nil
}

// PutStringEnum - Put a new enum value
func (c *KVStore) PutStringEnum(index, value string) (uint64, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	indices := c.SelectNodes(index, false, true)
	if len(indices) == 0 {
		return 0, fmt.Errorf("PutStringEnum(_) = _, %v: ", " no available nodes!")
	}

	// Perform PutStringEnum server call on first node in list (primary)
	// If the value already exists it will silently return the existing rowID
	rowID, err := c.client[indices[0]].PutStringEnum(ctx, &pb.StringEnum{IndexPath: index, Value: value})
	if err != nil {
		return 0, fmt.Errorf("PutStringEnum(_) = _, %v: [%s]", err, c.Conn.ClientConnections()[indices[0]].Target())
	}

	// Parallel iterate over remaining client list and perform Put operation (replication)
	var eg errgroup.Group
	for _, i := range indices[1:] {
		c := c.client[indices[i]]
		eg.Go(func() error {
			_, err := c.Put(ctx, &pb.IndexKVPair{IndexPath: index, Key: ToBytes(value),
				Value: [][]byte{ToBytes(rowID.Value)}})
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

// DeleteIndicesWithPrefix - Delete indices with a table prefix, optionally retain StringEnum data
func (c *KVStore) DeleteIndicesWithPrefix(prefix string, retainEnums bool) error {

	var eg errgroup.Group
	indices := c.SelectNodes(prefix, true, false)
	if len(indices) == 0 {
		return fmt.Errorf("no available nodes")
	}
	for _, i := range indices {
		x := c.client[indices[i]]
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
