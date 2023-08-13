package shared

//
// Client side bitmap functions and API wrappers for bulk loading functions such as SetBit and
// SetValue for bitmap and BSI fields respectively.
//

import (
	"context"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	u "github.com/araddon/gou"
	pb "github.com/disney/quanta/grpc"
	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/sync/errgroup"
)

var (
	// Ensure BitmapIndex implements shared.Service
	_ Service = (*BitmapIndex)(nil)
)

const (
	timeFmt = "2006-01-02T15"
	ifDelim = "/"
)

// BitmapIndex - Client side API for bitmap operations.
//
// Conn - "base" class wrapper for network connection to servers.
// client - Array of client API wrappers, one each for every server node.
type BitmapIndex struct {
	*Conn
	client []pb.BitmapIndexClient
}

// NewBitmapIndex - Initializer for client side API wrappers.
func NewBitmapIndex(conn *Conn) *BitmapIndex {

	clients := make([]pb.BitmapIndexClient, len(conn.ClientConnections()))
	for i := 0; i < len(conn.ClientConnections()); i++ {
		clients[i] = pb.NewBitmapIndexClient(conn.ClientConnections()[i])
	}
	c := &BitmapIndex{Conn: conn, client: clients}
	conn.RegisterService(c)
	return c
}

// MemberJoined - A new node joined the cluster.
func (c *BitmapIndex) MemberJoined(nodeID, ipAddress string, index int) {

	c.client = append(c.client, nil)
	copy(c.client[index+1:], c.client[index:])
	c.client[index] = pb.NewBitmapIndexClient(c.Conn.clientConn[index])
}

// MemberLeft - A node left the cluster.
func (c *BitmapIndex) MemberLeft(nodeID string, index int) {

	if len(c.client) <= 1 {
		c.client = make([]pb.BitmapIndexClient, 0)
		return
	}
	c.client = append(c.client[:index], c.client[index+1:]...)
}

// Client - Get a client by index.
func (c *BitmapIndex) Client(index int) pb.BitmapIndexClient {

	return c.client[index]
}

// Update - Handle Updates
func (c *BitmapIndex) Update(index, field string, columnID uint64, rowIDOrValue int64,
	ts time.Time, isBSI, isExclusive bool) error {

	req := &pb.UpdateRequest{Index: index, Field: field, ColumnId: columnID,
		RowIdOrValue: rowIDOrValue, Time: ts.UnixNano()}

	var eg errgroup.Group

	/*
	 * Send the same update request to each node.  This is so that exclusive fields can be
	 * handled (clear of previous rowIds).  Update requests for non-existent data is
	 * silently ignored.
	 */
	var indices []int
	var err error
	op := WriteIntent
	if isBSI {
		indices, err = c.SelectNodes(fmt.Sprintf("%s/%s/%s", index, field, ts.Format(timeFmt)), op)
	} else {
		if isExclusive {
			op = WriteIntentAll
		}
		indices, err = c.SelectNodes(fmt.Sprintf("%s/%s/%d/%s", index, field, rowIDOrValue, ts.Format(timeFmt)), op)
	}
	if err != nil {
		return fmt.Errorf("Update: %v", err)
	}
	for _, n := range indices {
		client := c.client[n]
		clientIndex := n
		eg.Go(func() error {
			if err := c.updateClient(client, req, clientIndex); err != nil {
				return err
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

// Send an update request to all nodes.
func (c *BitmapIndex) updateClient(client pb.BitmapIndexClient, req *pb.UpdateRequest,
	clientIndex int) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	if _, err := client.Update(ctx, req); err != nil {
		return fmt.Errorf("%v.Update(_) = _, %v, node = %s", client, err,
			c.Conn.ClientConnections()[clientIndex].Target())
	}
	return nil
}

// BatchMutate - Send a batch of standard bitmap mutations to the server cluster for processing.
// Does this by calling BatchMutateNode in parallel for optimal throughput.
func (c *BitmapIndex) BatchMutate(batch map[string]map[string]map[uint64]map[int64]*roaring64.Bitmap,
	clear bool) error {

	batches := c.splitBitmapBatch(batch)
	var eg errgroup.Group

	for i, v := range batches {
		cl := c.client[i]
		batch := v
		eg.Go(func() error {
			return c.BatchMutateNode(clear, cl, batch)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

// BatchMutateNode - Send batch to its respective node.
func (c *BitmapIndex) BatchMutateNode(clear bool, client pb.BitmapIndexClient,
	batch map[string]map[string]map[uint64]map[int64]*roaring64.Bitmap) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	b := make([]*pb.IndexKVPair, 0)
	i := 0
	for indexName, index := range batch {
		for fieldName, field := range index {
			for rowID, ts := range field {
				for t, bitmap := range ts {
					buf, err := bitmap.ToBytes()
					if err != nil {
						u.Errorf("bitmap.ToBytes: %v", err)
						return err
					}
					ba := make([][]byte, 1)
					ba[0] = buf
					b = append(b, &pb.IndexKVPair{IndexPath: indexName + "/" + fieldName,
						Key: ToBytes(int64(rowID)), Value: ba, Time: t, IsClear: clear})
					i++
					//u.Debug("Sent batch %d for path %s\n", i, b[i].IndexPath)
				}
			}
		}
	}
	stream, err := client.BatchMutate(ctx)
	if err != nil {
		u.Errorf("%v.BatchMutate(_) = _, %v: ", c.client, err)
		return fmt.Errorf("%v.BatchMutate(_) = _, %v: ", c.client, err)
	}

	for i := 0; i < len(b); i++ {
		if err := stream.Send(b[i]); err != nil {
			u.Errorf("%v.Send(%v) = %v", stream, b[i], err)
			return fmt.Errorf("%v.Send(%v) = %v", stream, b[i], err)
		}
	}
	_, err2 := stream.CloseAndRecv()
	if err2 != nil {
		u.Errorf("%v.CloseAndRecv() got error %v, want %v", stream, err2, nil)
		return fmt.Errorf("%v.CloseAndRecv() got error %v, want %v", stream, err2, nil)
	}
	return nil
}

// splitBitmapBatch - For a given batch of standard bitmap mutations, separate them into
// sub-batches based upon a consistently hashed shard key so that they can be send to their
// respective nodes.  For standard bitmaps, this shard key consists of [index/field/rowid/timestamp].
func (c *BitmapIndex) splitBitmapBatch(batch map[string]map[string]map[uint64]map[int64]*roaring64.Bitmap,
) []map[string]map[string]map[uint64]map[int64]*roaring64.Bitmap {

	batches := make([]map[string]map[string]map[uint64]map[int64]*roaring64.Bitmap, len(c.client))
	for i := range batches {
		batches[i] = make(map[string]map[string]map[uint64]map[int64]*roaring64.Bitmap)
	}

	for indexName, index := range batch {
		for fieldName, field := range index {
			for rowID, ts := range field {
				for t, bitmap := range ts {
					tm := time.Unix(0, t)
					indices, err := c.SelectNodes(fmt.Sprintf("%s/%s/%d/%s", indexName, fieldName, rowID, tm.Format(timeFmt)),
						WriteIntent)
					if err != nil {
						u.Errorf("splitBitmapBatch: %v", err)
						continue
					}
					for _, i := range indices {
						if batches[i] == nil {
							batches[i] = make(map[string]map[string]map[uint64]map[int64]*roaring64.Bitmap)
						}
						if _, ok := batches[i][indexName]; !ok {
							batches[i][indexName] = make(map[string]map[uint64]map[int64]*roaring64.Bitmap)
						}
						if _, ok := batches[i][indexName][fieldName]; !ok {
							batches[i][indexName][fieldName] = make(map[uint64]map[int64]*roaring64.Bitmap)
						}
						if _, ok := batches[i][indexName][fieldName][rowID]; !ok {
							batches[i][indexName][fieldName][rowID] = make(map[int64]*roaring64.Bitmap)
						}
						batches[i][indexName][fieldName][rowID][t] = bitmap
					}
				}
			}
		}
	}
	return batches
}

// BatchSetValue - Send a batch of BSI mutations to the server cluster for processing.  Does this by calling
// BatchSetValueNode in parallel for optimal throughput.
func (c *BitmapIndex) BatchSetValue(batch map[string]map[string]map[int64]*roaring64.BSI) error {

	batches := c.splitBSIBatch(batch)
	var eg errgroup.Group
	for i, v := range batches {
		cl := c.client[i]
		batch := v
		eg.Go(func() error {
			return c.BatchSetValueNode(cl, batch)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

// BatchSetValueNode - Send a batch of BSI values to a specific node.
func (c *BitmapIndex) BatchSetValueNode(client pb.BitmapIndexClient,
	batch map[string]map[string]map[int64]*roaring64.BSI) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	b := make([]*pb.IndexKVPair, 0)
	i := 0
	var err error
	for indexName, index := range batch {
		for fieldName, field := range index {
			for t, bsi := range field {
				if bsi.BitCount() == 0 {
					u.Debugf("BSI for %s - %s is empty.", indexName, fieldName)
					continue
				}
				ba, err := bsi.MarshalBinary()
				if err != nil {
					u.Errorf("BSI.MarshalBinary: %v", err)
					return err
				}
				b = append(b, &pb.IndexKVPair{IndexPath: indexName + "/" + fieldName,
					Key: ToBytes(int64(bsi.BitCount() * -1)), Value: ba, Time: t})
				i++
				//u.Debugf("Sent batch %d for path %s\n", i, b[i].IndexPath)
			}
		}
	}
	stream, err := client.BatchMutate(ctx)
	if err != nil {
		u.Errorf("%v.BatchMutate(_) = _, %v: ", c.client, err)
		return fmt.Errorf("%v.BatchMutate(_) = _, %v: ", c.client, err)
	}

	for i := 0; i < len(b); i++ {
		if err := stream.Send(b[i]); err != nil {
			u.Errorf("%v.Send(%v) = %v", stream, b[i], err)
			return fmt.Errorf("%v.Send(%v) = %v", stream, b[i], err)
		}
	}
	_, err2 := stream.CloseAndRecv()
	if err2 != nil {
		u.Errorf("%v.CloseAndRecv() got error %v, want %v", stream, err2, nil)
		return fmt.Errorf("%v.CloseAndRecv() got error %v, want %v", stream, err2, nil)
	}
	return nil
}

// For a given batch of BSI mutations, separate them into sub-batches based upon
// a consistently hashed shard key so that they can be send to their respective nodes.
// For BSI fields, this shard key consists of [index/field/timestamp].  All BSI slices
// for a given field are co-located.
func (c *BitmapIndex) splitBSIBatch(batch map[string]map[string]map[int64]*roaring64.BSI,
) []map[string]map[string]map[int64]*roaring64.BSI {

	batches := make([]map[string]map[string]map[int64]*roaring64.BSI, len(c.client))
	for i := range batches {
		batches[i] = make(map[string]map[string]map[int64]*roaring64.BSI)
	}

	for indexName, index := range batch {
		for fieldName, field := range index {
			for t, bsi := range field {
				tm := time.Unix(0, t)
				indices, err := c.SelectNodes(fmt.Sprintf("%s/%s/%s", indexName, fieldName, tm.Format(timeFmt)), WriteIntent)
				if err != nil {
					u.Errorf("splitBSIBatch: %v", err)
					continue
				}
				for _, i := range indices {
					if batches[i] == nil {
						batches[i] = make(map[string]map[string]map[int64]*roaring64.BSI)
					}
					if _, ok := batches[i][indexName]; !ok {
						batches[i][indexName] = make(map[string]map[int64]*roaring64.BSI)
					}
					if _, ok := batches[i][indexName][fieldName]; !ok {
						batches[i][indexName][fieldName] = make(map[int64]*roaring64.BSI)
					}
					batches[i][indexName][fieldName][t] = bsi
				}
			}
		}
	}
	return batches
}

// BulkClear - Send a resultset bitmap to all nodes and perform bulk clear operation.
func (c *BitmapIndex) BulkClear(index, fromTime, toTime string,
	foundSet *roaring64.Bitmap) error {

	data, err := foundSet.MarshalBinary()
	if err != nil {
		return err
	}

	req := &pb.BulkClearRequest{Index: index, FoundSet: data}

	if from, err := time.Parse(timeFmt, fromTime); err == nil {
		req.FromTime = from.UnixNano()
	} else {
		return err
	}
	if to, err := time.Parse(timeFmt, toTime); err == nil {
		req.ToTime = to.UnixNano()
	} else {
		return err
	}

	var eg errgroup.Group

	// Send the same clear request to each node
	indices, err := c.SelectNodes(index, WriteIntentAll)
	if err != nil {
		return fmt.Errorf("BulkClear: %v", err)
	}
	for _, n := range indices {
		client := c.client[n]
		clientIndex := n
		eg.Go(func() error {
			if err := c.clearClient(client, req, clientIndex); err != nil {
				return err
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

// Send bulk clear request to all nodes.
func (c *BitmapIndex) clearClient(client pb.BitmapIndexClient, req *pb.BulkClearRequest,
	clientIndex int) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	if _, err := client.BulkClear(ctx, req); err != nil {
		return fmt.Errorf("%v.BulkClear(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return nil
}

// CheckoutSequence - Request a sequence generator from owning server node.
func (c *BitmapIndex) CheckoutSequence(indexName, pkField string, ts time.Time,
	reservationSize int) (*Sequencer, error) {

	req := &pb.CheckoutSequenceRequest{Index: indexName, PkField: pkField, Time: ts.UnixNano(),
		ReservationSize: uint32(reservationSize)}

	// We are checking out a sequence with the intent to write, but we only want the Active primary hence ReadIntent
	indices, err1 := c.SelectNodes(fmt.Sprintf("%s/%s/%s", indexName, pkField, ts.Format(timeFmt)), ReadIntent)
	if err1 != nil {
		return nil, fmt.Errorf("CheckoutSequence: %v", err1)
	}

	/*
	 * Make sure to target the node with the true maximum column ID for the table.
	 * If time quantums are enabled, then the PK must be a timestamp field.
	 * (Note: For compound keys this must be the first (leftmost) key).
	 * In this case, the timestamp is truncated with timeFmt and it's nano value is
	 * added to the sequence start value on the server and returned to the client..
	 */
	res, err := c.sequencerClient(c.client[indices[0]], req, indices[0])
	if err != nil {
		return nil, err
	}
	return NewSequencer(res.Start, int(res.Count)), nil
}

// Send projection processing request to a specific node.
func (c *BitmapIndex) sequencerClient(client pb.BitmapIndexClient, req *pb.CheckoutSequenceRequest,
	clientIndex int) (result *pb.CheckoutSequenceResponse, err error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	if result, err = client.CheckoutSequence(ctx, req); err != nil {
		return nil, fmt.Errorf("%v.CheckoutSequence(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return result, nil
}

// Projection - Send fields and target set for a given index to cluster for projection processing.
func (c *BitmapIndex) Projection(index string, fields []string, fromTime, toTime int64,
	foundSet *roaring64.Bitmap, negate bool) (map[string]*roaring64.BSI, map[string]map[uint64]*roaring64.Bitmap, error) {

	bsiResults := make(map[string][]*roaring64.BSI, 0)
	bitmapResults := make(map[string]map[uint64][]*roaring64.Bitmap, 0)

	data, err := foundSet.MarshalBinary()
	if err != nil {
		return nil, nil, err
	}

	req := &pb.ProjectionRequest{Index: index, Fields: fields, FromTime: fromTime,
		ToTime: toTime, FoundSet: data, Negate: negate}

	resultChan := make(chan *pb.ProjectionResponse, 100)
	var eg errgroup.Group

	// Send the same projection request to each readable node.
	indices, err2 := c.SelectNodes(index, ReadIntentAll)
	if err2 != nil {
		return nil, nil, fmt.Errorf("Projection: %v", err2)
	}
	for _, n := range indices {
		client := c.client[n]
		clientIndex := n
		eg.Go(func() error {
			pr, err := c.projectionClient(client, req, clientIndex)
			if err != nil {
				return err
			}
			resultChan <- pr
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	close(resultChan)

	for rs := range resultChan {
		for _, v := range rs.GetBsiResults() {
			bsi, ok := bsiResults[v.Field]
			if !ok {
				bsi = make([]*roaring64.BSI, 0)
			}

			newBsi := roaring64.NewDefaultBSI()
			if err := newBsi.UnmarshalBinary(v.Bitmaps); err != nil {
				return nil, nil, fmt.Errorf("Error unmarshalling BSI projection results - %v", err)
			}
			bsiResults[v.Field] = append(bsi, newBsi)
		}
		for _, v := range rs.GetBitmapResults() {
			if _, ok := bitmapResults[v.Field]; !ok {
				bitmapResults[v.Field] = make(map[uint64][]*roaring64.Bitmap, 0)
			}
			field := bitmapResults[v.Field]
			bm, ok := field[v.RowId]
			if !ok {
				bm = make([]*roaring64.Bitmap, 0)
			}
			newBm := roaring64.NewBitmap()
			if err := newBm.UnmarshalBinary(v.Bitmap); err != nil {
				return nil, nil, fmt.Errorf("Error unmarshalling bitmap projection results - %v", err)
			}
			field[v.RowId] = append(bm, newBm)
		}
	}

	// Aggregate the per node results
	aggbsiResults := make(map[string]*roaring64.BSI)
	for k, v := range bsiResults {
		bsi := roaring64.NewDefaultBSI()
		//bsi.ParOr(0, v...)
		for _, z := range v {
			bsi.ParOr(0, z)
		}
		aggbsiResults[k] = bsi
	}
	aggbitmapResults := make(map[string]map[uint64]*roaring64.Bitmap)
	for k, v := range bitmapResults {
		aggbitmapResults[k] = make(map[uint64]*roaring64.Bitmap)
		for k2, v2 := range v {
			aggbitmapResults[k][k2] = roaring64.ParOr(0, v2...)
		}
	}
	return aggbsiResults, aggbitmapResults, nil
}

// Send projection processing request to a specific node.
func (c *BitmapIndex) projectionClient(client pb.BitmapIndexClient, req *pb.ProjectionRequest,
	clientIndex int) (*pb.ProjectionResponse, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	result, err := client.Projection(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("%v.Projection(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return result, nil
}

// TableOperation - Handle TableOperations
func (c *BitmapIndex) TableOperation(table, operation string) error {

	sop := AllActive
	var op pb.TableOperationRequest_OpType
	switch operation {
	case "deploy":
		sop = Admin
		op = pb.TableOperationRequest_DEPLOY
	case "drop":
		op = pb.TableOperationRequest_DROP
	case "truncate":
		op = pb.TableOperationRequest_TRUNCATE
	default:
		return fmt.Errorf("unknown operation %v", operation)
	}
	req := &pb.TableOperationRequest{Table: table, Operation: op}

	var eg errgroup.Group

	// Send the same tableOperation request to each node.  They must be all Active
	indices, err := c.SelectNodes(table, sop)
	if err != nil {
		return fmt.Errorf("table %s operation: %v", operation, err)
	}
	for _, n := range indices {
		client := c.client[n]
		clientIndex := n
		eg.Go(func() error {
			if err := c.tableOperationClient(client, req, clientIndex); err != nil {
				return err
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

// Send a tableOperation request to all nodes.
func (c *BitmapIndex) tableOperationClient(client pb.BitmapIndexClient, req *pb.TableOperationRequest,
	clientIndex int) error {

	ctx, cancel := context.WithTimeout(context.Background(), OpDeadline)
	defer cancel()

	if _, err := client.TableOperation(ctx, req); err != nil {
		return fmt.Errorf("%v.TableOperation(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return nil
}

// Commit - Block until persistence queues are synced (empty).
func (c *BitmapIndex) Commit() error {

	sop := AllActive
	var eg errgroup.Group

	// Send the same commit request to each node.  They must be all Active
	indices, err := c.SelectNodes(nil, sop)
	if err != nil {
		return fmt.Errorf("commit: %v", err)
	}
	for _, n := range indices {
		client := c.client[n]
		clientIndex := n
		eg.Go(func() error {
			if err := c.commitClient(client, clientIndex); err != nil {
				return err
			}
u.Errorf("SENDING TO %v", c.ClientConnections()[clientIndex].Target())
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}


// Send a Commit request to all nodes.
func (c *BitmapIndex) commitClient(client pb.BitmapIndexClient, clientIndex int) error {

	ctx, cancel := context.WithTimeout(context.Background(), OpDeadline)
	defer cancel()
	if _, err := client.Commit(ctx, &empty.Empty{}); err != nil {
		return fmt.Errorf("%v.Commit(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return nil
}
