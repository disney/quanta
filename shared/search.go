package shared

//
// Distributed high cardinality string indexing and search API.  Used by Quanta 'LIKE'
// operator in SQL queries.
//

import (
	"context"
	"fmt"
	u "github.com/araddon/gou"
	pb "github.com/disney/quanta/grpc"
	"github.com/golang/protobuf/ptypes/wrappers"
	"io"
	"sync"
	"time"
)

var (
	// Ensure StringSearch implements shared.Service
	_ Service = (*StringSearch)(nil)
)

// StringSearch API state
type StringSearch struct {
	*Conn
	client     []pb.StringSearchClient
	indexBatch map[string]struct{}
	batchSize  int
	batchMutex sync.Mutex
}

// NewStringSearch - Construct and Initialize search API.
func NewStringSearch(conn *Conn, batchSize int) *StringSearch {

	// Search utilizes the admin connections for the service
	clients := make([]pb.StringSearchClient, len(conn.ClientConnections()))
	for i := 0; i < len(conn.ClientConnections()); i++ {
		clients[i] = pb.NewStringSearchClient(conn.ClientConnections()[i])
	}
	c := &StringSearch{Conn: conn, batchSize: batchSize, client: clients}
	conn.RegisterService(c)
	return c
}

// MemberJoined - A new node joined the cluster.
func (c *StringSearch) MemberJoined(nodeID, ipAddress string, index int) {

	c.client = append(c.client, nil)
	copy(c.client[index+1:], c.client[index:])
	c.client[index] = pb.NewStringSearchClient(c.Conn.clientConn[index])
}

// MemberLeft - A node left the cluster.
func (c *StringSearch) MemberLeft(nodeID string, index int) {

	if len(c.client) <= 1 {
		c.client = make([]pb.StringSearchClient, 0)
		return
	}
	c.client = append(c.client[:index], c.client[index+1:]...)
}

// Client - Get a client by index.
func (c *StringSearch) Client(index int) pb.StringSearchClient {

	return c.client[index]
}

// Flush - Commit remaining string batch
func (c *StringSearch) Flush() error {

	c.batchMutex.Lock()
	defer c.batchMutex.Unlock()

	if c.indexBatch != nil {
		if err := c.BatchIndex(c.indexBatch); err != nil {
			return err
		}
		c.indexBatch = nil
	}
	return nil
}

// Separate a batch of strings to be indexed by consistant hashing by node key.
func (c *StringSearch) splitStringBatch(batch map[string]struct{}, replicas int) []map[string]struct{} {

	batches := make([]map[string]struct{}, len(c.client))
	for i := range batches {
		batches[i] = make(map[string]struct{}, 0)
	}
	for k, v := range batch {
		indices, err := c.SelectNodes(ToString(k), WriteIntent)
		if err != nil {
			u.Errorf("splitBitmapBatch: %v", err)
			continue
		}
		for _, i := range indices {
			batches[i][k] = v
		}
	}
	return batches
}

//
// Index a string for full text search.
// Indexing algorithm:
// 1) Break a string into words and cast list to lower case.
// 2) Discard the stem words.
// 3) Create a bloom filter with the remaining key words.
// 4) Store the bloom filter in a distributed hash (pogreb is the backing store).
//    The key is a murmur32 hash of the original string.
//
func (c *StringSearch) Index(str string) error {

	c.batchMutex.Lock()
	defer c.batchMutex.Unlock()

	if c.indexBatch == nil {
		c.indexBatch = make(map[string]struct{}, 0)
	}

	c.indexBatch[str] = struct{}{}

	if len(c.indexBatch) >= c.batchSize {
		if err := c.BatchIndex(c.indexBatch); err != nil {
			return err
		}
		c.indexBatch = nil
	}
	return nil
}

// BatchIndex - Process a batch of string for indexing.  Parallelized across nodes.
func (c *StringSearch) BatchIndex(batch map[string]struct{}) error {

	batches := c.splitStringBatch(batch, c.Conn.Replicas)

	done := make(chan error)
	defer close(done)
	count := len(batches)
	for i, v := range batches {
		go func(client pb.StringSearchClient, m map[string]struct{}) {
			done <- c.batchIndex(client, m)
		}(c.client[i], v)
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

// Process a batch of strings that are hashed to a particular node.
func (c *StringSearch) batchIndex(client pb.StringSearchClient, batch map[string]struct{}) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline*time.Second)
	defer cancel()
	b := make([]*wrappers.StringValue, len(batch))
	i := 0
	for k := range batch {
		b[i] = &wrappers.StringValue{Value: k}
		i++
	}
	stream, err := client.BatchIndex(ctx)
	if err != nil {
		return fmt.Errorf("%v.BatchIndex(_) = _, %v: ", c.client, err)
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

//
// Search - Process a string containing search terms:
// 1) Construct a bloom filter for the search terms similar to the Index API (above).
// 2) Execute the remaining steps across all cluster nodes.
// 3) On each node, iterate over the local keys looking for bloom filter matches.
// 4) On each node, return the hash codes for the matching items.
// 5) Merge and return the results as a set of unique hash codes.
//
func (c *StringSearch) Search(searchTerms string) (map[uint64]struct{}, error) {

	results := make(map[uint64]struct{}, 0)
	rchan := make(chan map[uint64]struct{})
	defer close(rchan)
	done := make(chan error)
	defer close(done)
	count := len(c.client)

	indices, err := c.SelectNodes(searchTerms, ReadIntentAll)
	if err != nil {
		return results, fmt.Errorf("Search: %v", err)
	}
	for _, i := range indices {
		go func(client pb.StringSearchClient) {
			r, e := c.search(client, searchTerms)
			rchan <- r
			done <- e
		}(c.client[i])
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

// Perform a search on a single node.
func (c *StringSearch) search(client pb.StringSearchClient, searchTerms string) (map[uint64]struct{}, error) {

	batch := make(map[uint64]struct{}, 0)

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	stream, err := client.Search(ctx, &wrappers.StringValue{Value: searchTerms})
	if err != nil {
		return nil, fmt.Errorf("%v.Search(_) = _, %v", client, err)
	}
	for {
		item, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%v.Search(_) = _, %v", client, err)
		}
		batch[item.Value] = struct{}{}
	}

	return batch, nil
}
