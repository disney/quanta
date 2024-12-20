package shared

//
// Query interface API contains methods called "client side".
//

import (
	"context"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	u "github.com/araddon/gou"
	pb "github.com/disney/quanta/grpc"
	"golang.org/x/sync/errgroup"
)

// Main processing flow for bitmap queries.  Returns a bitmap.  Processing is parallelized.
func (c *BitmapIndex) query(query *pb.BitmapQuery) (*roaring64.Bitmap, error) {

	//c.Conn.nodeMapLock.RLock()
	//defer c.Conn.nodeMapLock.RUnlock()

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

	resultChan := make(chan *IntermediateResult, 100)
	resultsMap := make(map[string]*IntermediateResult)

	var eg errgroup.Group

	// Break up the query and group fragments by index
	indexGroups := c.groupQueryFragmentsByIndex(query)
	for k, v := range indexGroups {
		ir := NewIntermediateResult(k)
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
			for _, v := range rs.GetAndDifferences() {
				ir.AddAndDifference(v)
			}
			for _, v := range rs.GetOrDifferences() {
				ir.AddOrDifference(v)
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
	andDifferences := make([]*roaring64.Bitmap, 0)
	orDifferences := make([]*roaring64.Bitmap, 0)
	existences := make([]*roaring64.Bitmap, 0)

	for _, v := range resultsMap {
		v.Collapse()
		//differences = append(differences, v.GetFinalDifference())
		for _, x := range v.GetAndDifferences() {
			andDifferences = append(andDifferences, x)
		}
		for _, x := range v.GetOrDifferences() {
			orDifferences = append(orDifferences, x)
		}
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
			rs, err := c.Join(v.Index, []string{fk.Field}, query.FromTime, query.ToTime, r, nil, false)
			if err != nil {
				return nil, err
			}
			fki.AddIntersect(rs.GetExistenceBitmap())
			elapsed := time.Since(start)
			u.Infof("Transpose input (%d), elapsed time %v", r.GetCardinality(), elapsed)
		}
	}

	union := roaring64.NewBitmap()
	existence := roaring64.NewBitmap()
	if len(unions) > 0 {
		union = roaring64.ParOr(0, unions...)
	}

	if len(existences) > 0 {
		existence = roaring64.ParOr(0, existences...)
	}
	result := union
	if result.GetCardinality() == 0 {
		result = existence
	}

	if len(orDifferences) > 0 {
		diff := make([]*roaring64.Bitmap, 0)
		for _, x := range orDifferences {
			d := roaring64.AndNot(result, x)
			diff = append(diff, d)
		}
		result = roaring64.ParOr(0, diff...)
	}

	if len(andDifferences) > 0 {
		diff := roaring64.ParOr(0, andDifferences...)
		result.AndNot(diff)
	}

	if len(intersects) > 0 {
		intersects = append(intersects, result)
		result = roaring64.FastAnd(intersects...)
	}
	return result, nil
}

// Perform query processing for a group of query predicates (fragments) for a given index.
// Processing is parallelized across nodes.
func (c *BitmapIndex) queryGroup(index string, query *pb.BitmapQuery) (*IntermediateResult, error) {

	resultChan := make(chan *pb.QueryResult, 100)
	var eg errgroup.Group

	gr := NewIntermediateResult(index)

	// Send the same query to each node
	for i, n := range c.client {
		status, err := c.GetCachedNodeStatusForIndex(i)
		if err != nil {
			u.Errorf("Skipping node to error - %v", err)
			continue
		}
		if status.NodeState != "Active" {
			continue
		}
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

	ra := make([]*IntermediateResult, 0)
	sa := make(map[string][]*roaring64.Bitmap)
	for rs := range resultChan {
		ir := NewIntermediateResult(index)
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
	adOr := make([]*roaring64.Bitmap, len(ra))
	adAnd := make([]*roaring64.Bitmap, len(ra))
	ae := make([]*roaring64.Bitmap, 0)
	numAndPredicates := len(ra[0].GetIntersects())
	for i := 0; i < numAndPredicates; i++ {
		for j := 0; j < len(ra); j++ {
			aa[j] = ra[j].GetIntersects()[i]
		}
		gr.AddIntersect(roaring64.ParOr(len(ra), aa...))
	}

	numDiffAndPredicates := len(ra[0].GetAndDifferences())
	for i := 0; i < numDiffAndPredicates; i++ {
		for j := 0; j < len(ra); j++ {
			adAnd[j] = ra[j].GetAndDifferences()[i]
		}
		gr.AddAndDifference(roaring64.ParOr(len(ra), adAnd...))
	}

	numDiffOrPredicates := len(ra[0].GetOrDifferences())
	for i := 0; i < numDiffOrPredicates; i++ {
		for j := 0; j < len(ra); j++ {
			adOr[j] = ra[j].GetOrDifferences()[i]
		}
		gr.AddOrDifference(roaring64.ParOr(len(ra), adOr...))
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
			samples = PerformStratifiedSampling(samples, v.SamplePct)
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
		if v.GetFinalExistence() != nil {
			x := v.GetFinalExistence()
			if x.GetCardinality() > 0 {
				ae = append(ae, x)
			}
		}
	}
	gr.AddUnion(roaring64.ParOr(len(aa), aa...))
	gr.AddExistence(roaring64.ParOr(len(ae), ae...))

	return gr, nil
}

// Break up a query and group all of the predicate fragments by index.  Queries that span multiple
// indices must include the relevant join criteria.  This function returns a map of the predicates
// keyed by index name.
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
		t := ""
		return nil, fmt.Errorf("%v.Query(_) = _, %v, node = %s", client, err, t)
		// c.ClientConnections()[clientIndex].Target())
	}
	return result, nil
}

// Query - Entrypoint for count/result queries
func (c *BitmapIndex) Query(query *BitmapQuery) (*BitmapQueryResponse, error) {

	response := &BitmapQueryResponse{}
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

// Join - Send summarized results for a given index to cluster for join processing.
// Resulting bitmap is then intersected with final query results.
func (c *BitmapIndex) Join(driverIndex string, fklist []string, fromTime, toTime int64,
	foundSet *roaring64.Bitmap, filterSets []*roaring64.Bitmap, negate bool) (*roaring64.BSI, error) {

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
		ToTime: toTime, FoundSet: foundData, FilterSets: fs, Negate: negate}

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
			c.ClientConnections()[clientIndex].Target())
	}
	return result, nil
}
