package server

//
// This file contains the Query API and all server side query related functions.
// It is important to note that while most of the bulk processing of a query happens
// server side, all of the map reduce functions and final query result compilation
// happen client side given a masterless architecture.  All BSI related functions are
// processed on the server and reduced to roaring bitmaps.  Only roaring bitmaps are
// returned to the client.
//

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	u "github.com/araddon/gou"
	// "github.com/golang/protobuf/ptypes/wrappers"
	pb "github.com/disney/quanta/grpc"
	"github.com/disney/quanta/shared"
	"time"
)

// Query API endpoint for client wrapper functions.
func (m *BitmapIndex) Query(ctx context.Context, query *pb.BitmapQuery) (*pb.QueryResult, error) {

	if query == nil {
		return nil, fmt.Errorf("query must not be nil")
	}

	d, errx := json.Marshal(&query)
	if errx != nil {
		u.Errorf("error: %v", errx)
		return nil, errx
	}
	u.Debugf("vvv query dump:\n%s\n\n", string(d))

	if query.Query == nil {
		return nil, fmt.Errorf("query fragment array must not be nil")
	}
	if len(query.Query) == 0 {
		return nil, fmt.Errorf("query fragment array must not be empty")
	}
	fromTime := time.Unix(0, query.FromTime)
	toTime := time.Unix(0, query.ToTime)

	dataMap := make(map[string]*roaring64.Bitmap)
	samples := make([]*shared.RowBitmap, 0)

	/*
	 *  Iterate over query predicates to see if there are any null checks or situations where there is no
	 *  union.  This can happen if there are only negated conditions in the query.
	 *  If so, gather existence for referenced table.
	 */
	foundUnion := false
	for _, v := range query.Query {
		if v.Operation == pb.QueryFragment_UNION {
			foundUnion = true
			break
		}

	}
	globalExistence := make(map[string]*roaring64.Bitmap)
	for _, v := range query.Query {
		if v.Index == "" {
			return nil, fmt.Errorf("Index not specified for query fragment %#v", v)
		}
		if v.Field == "" {
			return nil, fmt.Errorf("Field not specified for query fragment %#v", v)
		}
		if v.NullCheck || !foundUnion {
			ei, found := globalExistence[v.Index]
			if found {
				continue
			}
			table, ok := m.tableCache[v.Index]
			if !ok {
				return nil, fmt.Errorf("Cannot locate configuration for %s", v.Index)
			}
			pka, err := table.GetPrimaryKeyInfo()
			if err != nil {
				return nil, fmt.Errorf("timeRangeExistence GetPK info failed for %s - %v", v.Index, err)
			}
			var errx error
			ei, errx = m.timeRangeExistence(v.Index, pka[0].FieldName, fromTime, toTime)
			if errx != nil {
				return nil, fmt.Errorf("timeRangeExistence failed for %s - %v", v.Index, errx)
			}
			globalExistence[v.Index] = ei
		}
	}

	// Main query flow loop
	for _, v := range query.Query {
		var bm *roaring64.Bitmap
		var err error
		if v.NullCheck {
			if v.Negate {
				v.Operation = pb.QueryFragment_INTERSECT
			} else {
				v.Operation = pb.QueryFragment_DIFFERENCE
			}
		}
		if v.NullCheck && m.isBSI(v.Index, v.Field) {
			bm, err = m.timeRangeExistence(v.Index, v.Field, fromTime, toTime)
			if err != nil {
				return nil, fmt.Errorf("timeRangeExistence failed for %s - %v", v.Index, err)
			}
		} else if v.BsiOp > 0 {
			start := time.Now()
			bsi, err := m.timeRangeBSI(v.Index, v.Field, fromTime, toTime, nil)
			if err != nil {
				return nil, err
			}
			elapsed := time.Since(start)
			u.Debugf("timeRange BSI elapsed time %v", elapsed)
			// Evaluate BSI operation resulting in roaring bitmap
			start = time.Now()
			switch v.BsiOp {
			case pb.QueryFragment_LT:
				bm = bsi.CompareValue(0, roaring64.LT, v.Value, 0, nil)
			case pb.QueryFragment_LE:
				bm = bsi.CompareValue(0, roaring64.LE, v.Value, 0, nil)
			case pb.QueryFragment_EQ:
				bm = bsi.CompareValue(0, roaring64.EQ, v.Value, 0, nil)
			case pb.QueryFragment_GE:
				bm = bsi.CompareValue(0, roaring64.GE, v.Value, 0, nil)
			case pb.QueryFragment_GT:
				bm = bsi.CompareValue(0, roaring64.GT, v.Value, 0, nil)
			case pb.QueryFragment_RANGE:
				bm = bsi.CompareValue(0, roaring64.RANGE, v.Begin, v.End, nil)
			case pb.QueryFragment_BATCH_EQ:
				bm = bsi.BatchEqual(0, v.Values)
			}
			elapsed = time.Since(start)
			u.Debugf("BSI Compare (%d) elapsed time %v", v.BsiOp, elapsed)
		} else {
			start := time.Now()
			if v.SamplePct > 0 || v.NullCheck {
				var x *roaring64.Bitmap
				exist := make([]*roaring64.Bitmap, 0)
				for _, row := range m.listAllRowIDs(v.Index, v.Field) {
					if x, err = m.timeRange(v.Index, v.Field, row, fromTime, toTime, nil); err != nil {
						return nil, err
					}
					if x.GetCardinality() == 0 {
						continue
					}
					if v.NullCheck {
						exist = append(exist, x)
					} else {
						samples = append(samples, shared.NewRowBitmap(v.Field, row, x))
					}
				}
				if len(exist) > 0 {
					bm = roaring64.ParOr(0, exist...)
				}
			} else {
				if bm, err = m.timeRange(v.Index, v.Field, v.RowID, fromTime, toTime, nil); err != nil {
					return nil, err
				}
			}
			elapsed := time.Since(start)
			u.Debugf("timeRange elapsed time %v", elapsed)
		}

		if bm != nil {
			dataMap[v.Id] = bm
		} else {
			dataMap[v.Id] = roaring64.NewBitmap()
		}
	}

	start := time.Now()
	ir := shared.FromProto(query, dataMap).Reduce()
	if len(samples) > 0 {
		ir.AddSamples(samples)
	}
	if ge, ok := globalExistence[ir.Index]; ok {
		ir.AddExistence(ge)
	}
	elapsed := time.Since(start)
	u.Debugf("Reduce and finalize response elapsed time %v", elapsed)

	return ir.MarshalQueryResult()
}

func truncateTime(tr time.Time, tq string) time.Time {
	var rts int64
	if tq == "YMD" {
		rts = time.Date(tr.Year(), tr.Month(), tr.Day(), 0, 0, 0, 0, tr.Location()).UnixNano()
	} else { // YMDH
		rts = time.Date(tr.Year(), tr.Month(), tr.Day(), tr.Hour(), 0, 0, 0,
			tr.Location()).UnixNano()
	}
	return time.Unix(0, rts)
}

// Walk the time range and assemble a union of all bitmap fields.
func (m *BitmapIndex) timeRange(index, field string, rowID uint64, fromTime,
	toTime time.Time, foundSet *roaring64.Bitmap) (*roaring64.Bitmap, error) {

	m.bitmapCacheLock.RLock()
	defer m.bitmapCacheLock.RUnlock()

	attr, err := m.getFieldConfig(index, field)
	if err != nil {
		return nil, err
	}
	tq := attr.TimeQuantumType
	fromTime = truncateTime(fromTime, tq)
	toTime = truncateTime(toTime, tq)
	result := roaring64.NewBitmap()
	yr, mn, da := fromTime.Date()
	lookupTime := time.Date(yr, mn, da, 0, 0, 0, 0, time.UTC)
	a := make([]*roaring64.Bitmap, 0)

	if tq == "" { // No time quantum
		hashKey := fmt.Sprintf("%s/%s/%d/%s", index, field, rowID, lookupTime.Format(timeFmt))
		/*
		   if !m.Member(hashKey) {
		       return result, nil
		   }
		*/
		if bm, ok := m.bitmapCache[index][field][rowID][0]; ok {
			if foundSet != nil {
				b := bm.Bits.Clone()
				b.And(foundSet)
				a = append(a, b)
			} else {
				a = append(a, bm.Bits)
			}
			u.Debugf("timeRange No Quantum selecting %s", hashKey)
			result = roaring64.ParOr(0, a...)
		}
	} else {
		if rm, ok := m.bitmapCache[index][field][rowID]; ok {
			for ts, bitmap := range rm {
				rts := truncateTime(time.Unix(0, ts).UTC(), tq).UnixNano()
				if rts < fromTime.UnixNano() || rts > toTime.UnixNano() {
					continue
				}
				hashKey := fmt.Sprintf("%s/%s/%d/%s", index, field, rowID, time.Unix(0, ts).Format(timeFmt))
				if !m.Member(hashKey) {
					continue
				}
				if foundSet != nil {
					b := bitmap.Bits.Clone()
					b.And(foundSet)
					if b.GetCardinality() == 0 {
						continue
					}
					a = append(a, b)
					u.Debugf("timeRange %s selecting %s", tq, hashKey)
				} else {
					a = append(a, bitmap.Bits)
					u.Debugf("timeRange %s selecting %s", tq, hashKey)
				}
			}
		}
		result = roaring64.ParOr(0, a...)
	}
	return result, nil
}

func (m *BitmapIndex) listAllRowIDs(index, field string) []uint64 {

	m.bitmapCacheLock.RLock()
	defer m.bitmapCacheLock.RUnlock()
	rowIDs := make([]uint64, 0)
	for k := range m.bitmapCache[index][field] {
		rowIDs = append(rowIDs, k)
	}
	return rowIDs
}

// Walk the time range and assemble a union of all BSI fields.
func (m *BitmapIndex) timeRangeBSI(index, field string, fromTime, toTime time.Time,
	foundSet *roaring64.Bitmap) (*BSIBitmap, error) {

	m.bsiCacheLock.RLock()
	defer m.bsiCacheLock.RUnlock()

	attr, err := m.getFieldConfig(index, field)
	if err != nil {
		return nil, err
	}
	tq := attr.TimeQuantumType
	fromTime = truncateTime(fromTime, tq)
	toTime = truncateTime(toTime, tq)
	result := m.newBSIBitmap(index, field)
	yr, mn, da := fromTime.Date()
	lookupTime := time.Date(yr, mn, da, 0, 0, 0, 0, time.UTC)
	a := make([]*roaring64.BSI, 0)

	if tq == "" { // No time quantum
		// Verify that the data shard is primary here, skip if not.
		hashKey := fmt.Sprintf("%s/%s/%s", index, field, lookupTime.Format(timeFmt))
		/*
		   if !m.Member(hashKey) {
		       return result, nil
		   }
		*/
		if bm, ok := m.bsiCache[index][field][0]; ok {
			if foundSet != nil {
				a = append(a, bm.BSI.NewBSIRetainSet(foundSet))
			} else {
				a = append(a, bm.BSI)
			}
			u.Debugf("timeRangeBSI No Quantum selecting %s", hashKey)
			result.BSI.ParOr(0, a...)
		}
	} else {
		if tm, ok := m.bsiCache[index][field]; ok {
			for ts, bsi := range tm {
				rts := truncateTime(time.Unix(0, ts).UTC(), tq).UnixNano()
				if rts < fromTime.UnixNano() || rts > toTime.UnixNano() {
					continue
				}
				hashKey := fmt.Sprintf("%s/%s/%s", index, field, time.Unix(0, ts).Format(timeFmt))
				/*
					if !m.Member(hashKey) {
						continue
					}
				*/
				if foundSet != nil {
					x := bsi.BSI.NewBSIRetainSet(foundSet)
					if x.GetCardinality() == 0 {
						continue
					}
					a = append(a, x)
					u.Debugf("timeRangeBSI %s selecting %s", tq, hashKey)
				} else {
					a = append(a, bsi.BSI)
					u.Debugf("timeRangeBSI %s selecting %s", tq, hashKey)
				}
			}
		}
		result.BSI.ParOr(0, a...)
	}
	return result, nil
}

// Walk the time range and assemble a union of all BSI esistence
func (m *BitmapIndex) timeRangeExistence(index, field string, fromTime, toTime time.Time) (*roaring64.Bitmap, error) {

	m.bsiCacheLock.RLock()
	defer m.bsiCacheLock.RUnlock()

	attr, err := m.getFieldConfig(index, field)
	if err != nil {
		return nil, err
	}
	tq := attr.TimeQuantumType
	fromTime = truncateTime(fromTime, tq)
	toTime = truncateTime(toTime, tq)
	results := make([]*roaring64.Bitmap, 0)
	yr, mn, da := fromTime.Date()
	lookupTime := time.Date(yr, mn, da, 0, 0, 0, 0, time.UTC)
	if tq == "" { // No time quantum
		// Verify that the data shard is primary here, skip if not.
		hashKey := fmt.Sprintf("%s/%s/%s", index, field, lookupTime.Format(timeFmt))
		/*
		   if !m.Member(hashKey) {
		       return result, nil
		   }
		*/
		if bm, ok := m.bsiCache[index][field][0]; ok {
			results = append(results, bm.BSI.GetExistenceBitmap())
		}
		u.Debugf("timeRangeExistence No Quantum selecting %s", hashKey)
	} else {
		if tm, ok := m.bsiCache[index][field]; ok {
			for ts, bm := range tm {
				rts := truncateTime(time.Unix(0, ts).UTC(), tq).UnixNano()
				if rts < fromTime.UnixNano() || rts > toTime.UnixNano() {
					continue
				}
				hashKey := fmt.Sprintf("%s/%s/%s", index, field, time.Unix(0, ts).Format(timeFmt))
				/*
					if !m.Member(hashKey) {
						continue
					}
				*/
				u.Debugf("timeRangeExistence %s selecting %s", tq, hashKey)
				results = append(results, bm.BSI.GetExistenceBitmap())
			}
		}
	}
	return roaring64.ParOr(0, results...), nil
}

//
// Join - Once the client has mapreduced the initial query fragment results, A followup call is made to
// the Join API.   This API is responsible for mapping the column ID spaces for the child index
// to the column ID space of the parent (driver) index.  It does this by using the values contained
// in a foreign key BSI as a vector to the parent column ID values.
//
// Once these values are transposed they are returned as a roaring bitmap and intersected with
// the parent index results to formulate the final results.
//
func (m *BitmapIndex) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {

	fromTime := time.Unix(0, req.FromTime)
	toTime := time.Unix(0, req.ToTime)

	if req.DriverIndex == "" {
		return nil, fmt.Errorf("Index not specified for join criteria")
	}
	if req.FkFields == nil || len(req.FkFields) == 0 {
		return nil, fmt.Errorf("FK Field(s) not specified for join criteria")
	}

	foundSet := roaring64.NewBitmap()
	if err := foundSet.UnmarshalBinary(req.FoundSet); err != nil {
		return nil, err
	}

	filterSets := make([]*roaring64.Bitmap, len(req.FilterSets))
	for i, fsData := range req.FilterSets {
		filterSet := roaring64.NewBitmap()
		if err := filterSet.UnmarshalBinary(fsData); err != nil {
			return nil, err
		}
		filterSets[i] = filterSet
	}

	bsiArray := make([]*BSIBitmap, len(req.FkFields))
	minCardValue := uint64(1<<64 - 1)
	minCardIndex := 0
	for i, v := range req.FkFields {
		start := time.Now()
		bsi, err := m.timeRangeBSI(req.DriverIndex, v, fromTime, toTime, foundSet)
		if err != nil {
			err2 := fmt.Errorf("Cannot find FK BSI for %s %s - %v", req.DriverIndex, v, err)
			return nil, err2
		}
		c := bsi.GetCardinality()
		if c < minCardValue {
			minCardValue = c
			minCardIndex = i
		}
		bsiArray[i] = bsi
		elapsed := time.Since(start)
		u.Debugf("inner join timeRange BSI elapsed time %v for %s %s", elapsed, req.DriverIndex, v)
	}

	// Process the final FK relation with TransposeWithCounts
	start := time.Now()
	transposeBsi := bsiArray[minCardIndex]
	jr := transposeBsi.TransposeWithCounts(0, transposeBsi.GetExistenceBitmap(), filterSets[minCardIndex])
	elapsed := time.Since(start)
	u.Debugf("inner join transpose elapsed time %v", elapsed)

	data, err := jr.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return &pb.JoinResponse{Results: data}, nil
}

//
// Projection - Retrieve bitmaps to be included in a result set projection.
//
func (m *BitmapIndex) Projection(ctx context.Context, req *pb.ProjectionRequest) (*pb.ProjectionResponse, error) {

	u.Debugf("Projection retrieval started for %v - %v", req.Index, req.Fields)

	fromTime := time.Unix(0, req.FromTime)
	toTime := time.Unix(0, req.ToTime)

	if req.Index == "" {
		return nil, fmt.Errorf("Index not specified for projection criteria")
	}
	if req.Fields == nil || len(req.Fields) == 0 {
		return nil, fmt.Errorf("One or more fields not specified for projection criteria")
	}

	foundSet := roaring64.NewBitmap()
	if err := foundSet.UnmarshalBinary(req.FoundSet); err != nil {
		return nil, err
	}

	bitmapResults := make([]*pb.BitmapResult, 0)
	bsiResults := make([]*pb.BSIResult, 0)

	start := time.Now()
	var err2 error
	for _, v := range req.Fields {
		if _, ok := m.bitmapCache[req.Index][v]; ok {
			var x *roaring64.Bitmap
			for _, row := range m.listAllRowIDs(req.Index, v) {
				if x, err2 = m.timeRange(req.Index, v, row, fromTime, toTime, foundSet); err2 != nil {
					return nil, err2
				}
				if x.GetCardinality() == 0 {
					continue
				}
				bmr := &pb.BitmapResult{Field: v, RowId: row}
				if bmr.Bitmap, err2 = x.MarshalBinary(); err2 != nil {
					return nil, fmt.Errorf("Error marshalling bitmap for field %s, rowId %d, [%v]", v, row, err2)
				}
				bitmapResults = append(bitmapResults, bmr)
			}
		}
		if _, ok := m.bsiCache[req.Index][v]; ok {
			var bsi *BSIBitmap
			if bsi, err2 = m.timeRangeBSI(req.Index, v, fromTime, toTime, foundSet); err2 != nil {
				return nil, fmt.Errorf("Error ranging projection BSI for %s %s - %v", req.Index, v, err2)
			}
			if bsi.GetCardinality() == 0 {
				continue
			}
			bsir := &pb.BSIResult{Field: v}
			if bsir.Bitmaps, err2 = bsi.BSI.MarshalBinary(); err2 != nil {
				return nil, fmt.Errorf("Error marshalling BSI for field %s, [%v]", v, err2)
			}
			bsiResults = append(bsiResults, bsir)
		}
	}
	elapsed := time.Since(start)
	u.Debugf("Projection retrieval elapsed time %v", elapsed)
	return &pb.ProjectionResponse{BitmapResults: bitmapResults, BsiResults: bsiResults}, nil
}
