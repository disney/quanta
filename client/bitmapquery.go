package quanta

//
// Query interface API.
//

import (
	"context"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	u "github.com/araddon/gou"
	pb "github.com/disney/quanta/grpc"
	"github.com/disney/quanta/shared"
	"golang.org/x/sync/errgroup"
	"time"
)

//
// Main processing flow for bitmap queries.  Returns a bitmap.  Processing is parallelized.
//
func (c *BitmapIndex) query(query *pb.BitmapQuery) (*roaring64.Bitmap, error) {

	c.Conn.nodeMapLock.RLock()
	defer c.Conn.nodeMapLock.RUnlock()

	if len(query.Query) == 0 {
		return nil, fmt.Errorf("query must have at least 1 predicate")
	}

	// Query should have at least 1 union predicate.  If not, make the first intersect a union.
	// An exception to the rule is null checks
	foundUnion := false
	for _, v := range query.Query {
		if v.Operation == pb.QueryFragment_UNION {
			foundUnion = true
			break
		}
		if v.NullCheck {
			foundUnion = true
			break
		}
	}
	if !foundUnion {
		for i, v := range query.Query {
			if v.Operation == pb.QueryFragment_INTERSECT {
				query.Query[i].Operation = pb.QueryFragment_UNION
				break
			}
		}
	}

	resultChan := make(chan *shared.IntermediateResult, 100)
	resultsMap := make(map[string]*shared.IntermediateResult)

	var eg errgroup.Group

	// Break up the query and group fragments by index
	indexGroups := c.groupQueryFragmentsByIndex(query)
	for k, v := range indexGroups {
		ir := shared.NewIntermediateResult(k)
		// Iterate query fragments looking for join criteria
		for _, f := range v.Query {
			if f.Operation == pb.QueryFragment_INNER_JOIN {
				ir.AddFK(f.Fk, f.Field)
			}
		}
		resultsMap[k] = ir
		i := k
		q := v
		eg.Go(func() error {
			ir, err := c.queryGroup(i, q)
			if err != nil {
				return err
			}
			resultChan <- ir
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	close(resultChan)

	// Merge all group results
	for rs := range resultChan {
		ir, ok := resultsMap[rs.Index]
		if !ok {
			ir = rs
			resultsMap[rs.Index] = ir
		} else {
			for _, v := range rs.GetIntersects() {
				ir.AddIntersect(v)
			}
			for _, v := range rs.GetSamples() {
				ir.AddSample(v)
			}
			for _, v := range rs.GetUnions() {
				ir.AddUnion(v)
			}
			for _, v := range rs.GetDifferences() {
				ir.AddDifference(v)
			}
			for _, v := range rs.GetExistences() {
				ir.AddExistence(v)
			}
		}
	}

	// Pass #1, collapse intermediate results, select driver index
	var driver string
	unions := make([]*roaring64.Bitmap, 0)
	intersects := make([]*roaring64.Bitmap, 0)
	differences := make([]*roaring64.Bitmap, 0)
	existences := make([]*roaring64.Bitmap, 0)

	for _, v := range resultsMap {
		v.Collapse()
		differences = append(differences, v.GetFinalDifference())
		if v.FKCount() == 0 {
			if driver != "" {
				return nil, fmt.Errorf("Must specify at least 1 join for index %s", v.Index)
			}
			driver = v.Index
		}
		unions = append(unions, v.GetFinalUnion())
		existences = append(existences, v.GetFinalExistence())
		for _, x := range v.GetIntersects() {
			intersects = append(intersects, x)
		}
	}

	// Pass #2, Perform joins, if applicable
	for _, v := range resultsMap {
		for _, fk := range v.GetFKList() {
			// Get the results of the FK table
			fki, ok := resultsMap[fk.JoinIndex]
			if !ok {
				return nil, fmt.Errorf("Can't find FK %s", fk.JoinIndex)
			}
			// Perform the join transpose using local results
			r := v.GetFinalUnion()
			for _, x := range v.GetIntersects() {
				r = roaring64.FastAnd(x, r)
			}
			start := time.Now()
			rs, err := c.Join(v.Index, []string{fk.Field}, query.FromTime, query.ToTime, r, nil)
			if err != nil {
				return nil, err
			}
			fki.AddIntersect(rs.GetExistenceBitmap())
			elapsed := time.Since(start)
			u.Infof("Transpose input (%d), elapsed time %v", r.GetCardinality(), elapsed)
		}
	}

	union := roaring64.NewBitmap()
	difference := roaring64.NewBitmap()
	existence := roaring64.NewBitmap()
	if len(unions) > 0 {
		union = roaring64.ParOr(0, unions...)
	}
	if len(differences) > 0 {
		difference = roaring64.ParOr(0, differences...)
	}
	if len(existences) > 0 {
		existence = roaring64.ParOr(0, existences...)
	}
	result := roaring64.ParOr(0, union, existence)

	if len(intersects) > 0 {
		intersects = append(intersects, result)
		result = roaring64.FastAnd(intersects...)
	}
	if len(differences) > 0 {
		result.AndNot(difference)
	}
	return result, nil
}

//
// Perform query processing for a group of query predicates (fragments) for a given index.
// Processing is parallelized across nodes.
//
func (c *BitmapIndex) queryGroup(index string, query *pb.BitmapQuery) (*shared.IntermediateResult, error) {

	resultChan := make(chan *pb.QueryResult, 100)
	var eg errgroup.Group

	gr := shared.NewIntermediateResult(index)

	// Send the same query to each node
	for i, n := range c.client {
		client := n
		clientIndex := i
		eg.Go(func() error {
			qr, err := c.queryClient(client, query, clientIndex)
			if err != nil {
				return err
			}
			resultChan <- qr
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	close(resultChan)

	ra := make([]*shared.IntermediateResult, 0)
	sa := make(map[string][]*roaring64.Bitmap)
	for rs := range resultChan {
		ir := shared.NewIntermediateResult(index)
		if err := ir.UnmarshalAndAdd(rs); err != nil {
			return nil, err
		}

		ra = append(ra, ir)
		for _, v := range ir.GetSamples() {
			key := fmt.Sprintf("%s/%d", v.Field, v.RowID)
			if e, ok := sa[key]; !ok {
				e = make([]*roaring64.Bitmap, 0)
				e = append(e, v.Bits)
				sa[key] = e
			} else {
				e = append(e, v.Bits)
				sa[key] = e
			}
		}
	}

	// Iterate over node results and summarize into group results
	aa := make([]*roaring64.Bitmap, len(ra))
	ad := make([]*roaring64.Bitmap, len(ra))
	ae := make([]*roaring64.Bitmap, 0)
	numAndPredicates := len(ra[0].GetIntersects())
	for i := 0; i < numAndPredicates; i++ {
		for j := 0; j < len(ra); j++ {
			aa[j] = ra[j].GetIntersects()[i]
		}
		gr.AddIntersect(roaring64.ParOr(len(ra), aa...))
	}

	samples := make([]*roaring64.Bitmap, 0)
	// Collapse sample results (if any)
	for _, v := range sa {
		samples = append(samples, roaring64.ParOr(len(sa), v...))
	}

	gr.SamplePct = ra[0].SamplePct
	gr.SampleIsUnion = ra[0].SampleIsUnion
	for i, v := range ra {
		// Perform stratified sampling (if applicable) and add to operational list.  Samples are collapsed client side.
		if v.SamplePct > 0 {
			samples = performStratifiedSampling(samples, v.SamplePct)
			for _, x := range samples {
				if v.SampleIsUnion {
					v.AddUnion(x)
				} else {
					v.AddIntersect(x)
				}
			}
		}
		v.Collapse()
		aa[i] = v.GetFinalUnion()
		ad[i] = v.GetFinalDifference()
		if v.GetFinalExistence() != nil {
			x := v.GetFinalExistence()
			if x.GetCardinality() > 0 {
				ae = append(ae, x)
			}
		}
	}
	gr.AddUnion(roaring64.ParOr(len(aa), aa...))
	gr.AddDifference(roaring64.ParOr(len(ad), ad...))
	gr.AddExistence(roaring64.ParOr(len(ae), ae...))

	return gr, nil
}

//
// Break up a query and group all of the predicate fragments by index.  Queries that span multiple
// indices must include the relevant join criteria.  This function returns a map of the predicates
// keyed by index name.
//
func (c *BitmapIndex) groupQueryFragmentsByIndex(query *pb.BitmapQuery) map[string]*pb.BitmapQuery {

	results := make(map[string]*pb.BitmapQuery)
	for _, f := range query.Query {
		rq, ok := results[f.Index]
		if !ok {
			rq = &pb.BitmapQuery{Query: []*pb.QueryFragment{f}}
			rq.FromTime = query.FromTime
			rq.ToTime = query.ToTime
			results[f.Index] = rq
		} else {
			rq.Query = append(rq.Query, f)
		}
	}
	return results
}

// Execute a query against a single node.
func (c *BitmapIndex) queryClient(client pb.BitmapIndexClient, q *pb.BitmapQuery,
	clientIndex int) (*pb.QueryResult, error) {

	/*
	   d, err := json.Marshal(&q)
	   if err != nil {
	       u.Errorf("error: %v", err)
	       return nil, err
	   }
	   u.Debugf("vvv query dump:\n%s\n\n", string(d))
	*/

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	result, err := client.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("%v.Query(_) = _, %v, node = %s", client, err,
			c.Conn.clientConn[clientIndex].Target())
	}
	return result, nil
}

// BitmapQueryResponse - Contains query results
type BitmapQueryResponse struct {
	Success      bool
	ErrorMessage string
	Value        int64
	Count        uint64
	Results      *roaring64.Bitmap
}

// Query - Entrypoint for count/result queries
func (c *BitmapIndex) Query(query *shared.BitmapQuery) (*shared.BitmapQueryResponse, error) {

	response := &shared.BitmapQueryResponse{}
	var err error
	if response.Results, err = c.query(query.ToProto()); err != nil {
		response.ErrorMessage = fmt.Sprintf("%v", err)
	} else {
		response.Count = response.Results.GetCardinality()
		response.Success = true
	}

	return response, err
}

// ResultsQuery - Entrypoint for queries where result is returned as a list of column IDs
func (c *BitmapIndex) ResultsQuery(query *pb.BitmapQuery, limit uint64) ([]uint64, error) {

	result, err := c.query(query)
	if err != nil {
		return []uint64{}, err
	}
	if result.GetCardinality() > limit {
		ra := make([]uint64, 0)
		iter := result.Iterator()
		for iter.HasNext() {
			ra = append(ra, iter.Next())
		}
		return ra, nil
	}
	return result.ToArray(), nil
}

//
// Join - Send summarized results for a given index to cluster for join processing.
// Resulting bitmap is then intersected with final query results.
//
func (c *BitmapIndex) Join(driverIndex string, fklist []string, fromTime, toTime int64,
	foundSet *roaring64.Bitmap, filterSets []*roaring64.Bitmap) (*roaring64.BSI, error) {

	foundData, err := foundSet.MarshalBinary()
	if err != nil {
		return nil, err
	}

	fs := make([][]byte, len(filterSets))
	for i, v := range filterSets {
		filterData, err := v.MarshalBinary()
		if err != nil {
			return nil, err
		}
		fs[i] = filterData
	}

	req := &pb.JoinRequest{DriverIndex: driverIndex, FkFields: fklist, FromTime: fromTime,
		ToTime: toTime, FoundSet: foundData, FilterSets: fs}

	resultChan := make(chan *pb.JoinResponse, 100)
	var eg errgroup.Group

	// Send the same join request to each node
	for i, n := range c.client {
		client := n
		clientIndex := i
		eg.Go(func() error {
			jr, err := c.joinClient(client, req, clientIndex)
			if err != nil {
				return err
			}
			resultChan <- jr
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	close(resultChan)

	results := make([]*roaring64.BSI, 0)
	for rs := range resultChan {
		bsi := roaring64.NewDefaultBSI()
		if rs.Results != nil {
			if err := bsi.UnmarshalBinary(rs.Results); err != nil {
				return nil, fmt.Errorf("Error unmarshalling join results - %v", err)
			}
			if bsi.GetCardinality() > 0 {
				results = append(results, bsi)
			}
		}
	}

	ret := roaring64.NewDefaultBSI()
	for _, v := range results {
		ret.Add(v)
	}

	return ret, nil
}

// Send join processing request to a specific node.
func (c *BitmapIndex) joinClient(client pb.BitmapIndexClient, req *pb.JoinRequest,
	clientIndex int) (*pb.JoinResponse, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	result, err := client.Join(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("%v.Join(_) = _, %v, node = %s", client, err,
			c.Conn.clientConn[clientIndex].Target())
	}
	return result, nil
}

//
// Projection - Send fields and target set for a given index to cluster for projection processing.
//
func (c *BitmapIndex) Projection(index string, fields []string, fromTime, toTime int64,
	foundSet *roaring64.Bitmap) (map[string]*roaring64.BSI, map[string]map[uint64]*roaring64.Bitmap, error) {

	bsiResults := make(map[string][]*roaring64.BSI, 0)
	bitmapResults := make(map[string]map[uint64][]*roaring64.Bitmap, 0)

	data, err := foundSet.MarshalBinary()
	if err != nil {
		return nil, nil, err
	}

	req := &pb.ProjectionRequest{Index: index, Fields: fields, FromTime: fromTime,
		ToTime: toTime, FoundSet: data}

	resultChan := make(chan *pb.ProjectionResponse, 100)
	var eg errgroup.Group

	// Send the same projection request to each node
	for i, n := range c.client {
		client := n
		clientIndex := i
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
		bsi.ParOr(0, v...)
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
			c.Conn.clientConn[clientIndex].Target())
	}
	return result, nil
}
