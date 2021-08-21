package shared

//
// BitmapQuery is a container and API for the construction of
// bitmap server queries.  This code contains common functions
// shared by both client/server side.
//

import (
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	pb "github.com/disney/quanta/grpc"
	"github.com/pborman/uuid"
	"time"
)

const (
	// YMDHTimeFmt - Time format for YMDH
	YMDHTimeFmt = "2006-01-02T15"
	// YMDTimeFmt - Time format for YMD
	YMDTimeFmt = "2006-01-02"
)

// QueryFragment - Atomic query predicate elements
type QueryFragment struct {
	Index     string  `yaml:"index"`
	Field     string  `yaml:"field"`
	RowID     uint64  `yaml:"rowID,omitempty"`
	Value     int64   `yaml:"value,omitempty"`
	Values    []int64 `yaml:"values,omitempty"`
	Operation string  `yaml:"op,omitempty"`
	BSIOp     string  `yaml:"bsiOp,omitempty"`
	Begin     int64   `yaml:"begin,omitempty"`
	End       int64   `yaml:"end,omitempty"`
	Fk        string  `yaml:"fk,omitempty"`
	Search    string  `yaml:"search,omitempty"`
	SamplePct float32 `yaml:"samplePct,omitempty"`
	NullCheck bool    `yaml:"nullCheck,omitempty"`
	Negate    bool    `yaml:"negate,omitempty"`
	children  []*QueryFragment
	parent    *QueryFragment
	Query     *BitmapQuery
	Data      *roaring64.Bitmap
	Sample    []*RowBitmap
	Added     bool
}

// BitmapQuery - Top level query state container
type BitmapQuery struct {
	FromTime string `yaml:"fromTime"`
	ToTime   string `yaml:"toTime"`
	root     *QueryFragment
	curLevel int
}

// NewBitmapQuery - Construct a new query.
func NewBitmapQuery() *BitmapQuery {
	return &BitmapQuery{}
}

// BitmapQueryResponse - Bitmap query API response container
type BitmapQueryResponse struct {
	Success      bool
	ErrorMessage string
	Value        int64
	Count        uint64
	Results      *roaring64.Bitmap
}

//
// IntermediateResult - Container for query results returned from individual server nodes.
// Aggregated results are stored in this structure for further processing on the client.
//
type IntermediateResult struct {
	Index         string
	SamplePct     float32
	SampleIsUnion bool
	unions        []*roaring64.Bitmap
	intersects    []*roaring64.Bitmap
	differences   []*roaring64.Bitmap
	existences    []*roaring64.Bitmap
	samples       []*RowBitmap
	joinFkList    []FK
	union         *roaring64.Bitmap
	difference    *roaring64.Bitmap
	existence     *roaring64.Bitmap
}

// FK - Foreign key container.
type FK struct {
	JoinIndex string
	Field     string
}

// RowBitmap - Container that includes RowID information for standard bitmap types.
type RowBitmap struct {
	Field string
	RowID uint64
	Bits  *roaring64.Bitmap
}

// NewRowBitmap - Construct a new RowBitmap container.
func NewRowBitmap(field string, rowID uint64, bm *roaring64.Bitmap) *RowBitmap {
	return &RowBitmap{Field: field, RowID: rowID, Bits: bm}
}

// NewIntermediateResult - Construct a new intermediate result.
func NewIntermediateResult(index string) *IntermediateResult {

	r := &IntermediateResult{Index: index}
	r.unions = make([]*roaring64.Bitmap, 0)
	r.intersects = make([]*roaring64.Bitmap, 0)
	r.differences = make([]*roaring64.Bitmap, 0)
	r.samples = make([]*RowBitmap, 0)
	r.joinFkList = make([]FK, 0)
	return r
}

// AddIntersect - Add a new "AND" predicate to the query.
func (r *IntermediateResult) AddIntersect(b *roaring64.Bitmap) {
	if b == nil {
		panic("Attempt to add nil Intersect.")
	}
	r.intersects = append(r.intersects, b)
}

// GetIntersects - Get all filtering "AND" predicates.
func (r *IntermediateResult) GetIntersects() []*roaring64.Bitmap {
	return r.intersects
}

// AddUnion - Add a new "OR" predicate to the query.
func (r *IntermediateResult) AddUnion(b *roaring64.Bitmap) {
	r.unions = append(r.unions, b)
}

// GetUnions - Get all "OR" predicates.
func (r *IntermediateResult) GetUnions() []*roaring64.Bitmap {
	return r.unions
}

// GetFinalUnion - "OR" operations are distributive so they can be aggregated on the nodes.
func (r *IntermediateResult) GetFinalUnion() *roaring64.Bitmap {
	return r.union
}

// AddDifference - Add a new "ANDNOT" predicate to the query.
func (r *IntermediateResult) AddDifference(b *roaring64.Bitmap) {
	if b == nil {
		panic("Attempt to add nil Difference.")
	}
	r.differences = append(r.differences, b)
}

// GetDifferences - Get all "ANDNOT" predicates.
func (r *IntermediateResult) GetDifferences() []*roaring64.Bitmap {
	return r.differences
}

// GetFinalDifference - "ANDNOT" operations are distributive so they can be aggregated on the nodes.
func (r *IntermediateResult) GetFinalDifference() *roaring64.Bitmap {
	return r.difference
}

// AddSamples - Add a stratified sample predicate batch.
func (r *IntermediateResult) AddSamples(b []*RowBitmap) {
	for _, x := range b {
		r.samples = append(r.samples, x)
	}
}

// AddSample - Add a stratified sample row.
func (r *IntermediateResult) AddSample(b *RowBitmap) {
	if b == nil {
		panic("Attempt to add nil Sample.")
	}
	r.samples = append(r.samples, b)
}

// GetSamples - Get all rows comprising a stratified sample.
func (r *IntermediateResult) GetSamples() []*RowBitmap {
	return r.samples
}

// AddExistence - Add an existence bitmap to the query predicates.
func (r *IntermediateResult) AddExistence(b *roaring64.Bitmap) {
	if b == nil {
		panic("Attempt to add nil Existence.")
	}
	r.existences = append(r.existences, b)
}

// GetExistences - Get all existence bitmaps.
func (r *IntermediateResult) GetExistences() []*roaring64.Bitmap {
	return r.existences
}

// GetFinalExistence - Existence bitmaps are distributive and can be "OR"ed together.
func (r *IntermediateResult) GetFinalExistence() *roaring64.Bitmap {
	return r.existence
}

// AddFK - Add foreign key for join.
func (r *IntermediateResult) AddFK(index, fk string) {
	r.joinFkList = append(r.joinFkList, FK{JoinIndex: index, Field: fk})
}

// FKCount - Return a count of all foreign keys.
func (r *IntermediateResult) FKCount() int {
	return len(r.joinFkList)
}

// GetFKList - Return a list of all foreign keys.
func (r *IntermediateResult) GetFKList() []FK {
	return r.joinFkList
}

// Collapse and finalize all distributive values.
func (r *IntermediateResult) Collapse() {

	r.union = roaring64.ParOr(0, r.unions...)
	r.difference = roaring64.ParOr(0, r.differences...)
	r.existence = roaring64.ParOr(0, r.existences...)
}

// MarshalQueryResult into protobuf response for gRPC (server side).
func (r *IntermediateResult) MarshalQueryResult() (qr *pb.QueryResult, err error) {

	var unionBuf, differenceBuf, existenceBuf []byte
	var intersectBuf [][]byte
	var sampleRows []*pb.BitmapResult
	if r.union != nil {
		unionBuf, err = r.union.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("Cannot marshal intermediate union result bitmap - %v", err)
		}
	}

	intersectBuf = make([][]byte, len(r.intersects))
	for i, bm := range r.intersects {
		intersectBuf[i], err = bm.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("Cannot marshal intermediate intersect result bitmap - %v", err)
		}
	}

	sampleRows = make([]*pb.BitmapResult, len(r.samples))
	for i, rb := range r.samples {
		sampleBuf, err := rb.Bits.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("Cannot marshal intermediate sample result bitmap - %v", err)
		}
		sampleRows[i] = &pb.BitmapResult{Field: rb.Field, RowId: rb.RowID, Bitmap: sampleBuf}
	}

	if r.difference != nil {
		differenceBuf, err = r.difference.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("Cannot marshal intermediate difference result bitmap - %v", err)
		}
	}

	/*
	 * This is a hack (next 3 lines)  due to the fact that existence is added server side after the
	 * intermediate results are collapsed just before the query response is returned.
	 */
	if len(r.existences) > 0 && (r.existence == nil || r.existence.GetCardinality() == 0) {
		r.existence = roaring64.ParOr(0, r.existences...)
	}

	if r.existence != nil {
		existenceBuf, err = r.existence.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("Cannot marshal intermediate existence result bitmap - %v", err)
		}
	}

	return &pb.QueryResult{Unions: unionBuf, Intersects: intersectBuf, Differences: differenceBuf,
		Samples: sampleRows, SamplePct: r.SamplePct, SampleIsUnion: r.SampleIsUnion,
		Existences: existenceBuf}, nil
}

// UnmarshalAndAdd protobuf into Quanta query response (client side).
func (r *IntermediateResult) UnmarshalAndAdd(rs *pb.QueryResult) error {

	bm := roaring64.NewBitmap()
	if err := bm.UnmarshalBinary(rs.GetUnions()); err != nil {
		return fmt.Errorf("Error unmarshalling query result unions - %v", err)
	}
	if bm.GetCardinality() > 0 {
		r.AddUnion(bm)
	}

	for _, b := range rs.GetIntersects() {
		bm = roaring64.NewBitmap()
		if err := bm.UnmarshalBinary(b); err != nil {
			return fmt.Errorf("Error unmarshalling query result intersects - %v", err)
		}
		r.AddIntersect(bm)
	}

	for _, b := range rs.GetSamples() {
		bm = roaring64.NewBitmap()
		if err := bm.UnmarshalBinary(b.Bitmap); err != nil {
			return fmt.Errorf("Error unmarshalling query result samples - %v", err)
		}
		rb := NewRowBitmap(b.Field, b.RowId, bm)
		r.AddSample(rb)
	}
	r.SamplePct = rs.GetSamplePct()
	r.SampleIsUnion = rs.SampleIsUnion

	bm = roaring64.NewBitmap()
	if err := bm.UnmarshalBinary(rs.GetDifferences()); err != nil {
		return fmt.Errorf("Error unmarshalling query result differences - %v", err)
	}
	if bm.GetCardinality() > 0 {
		r.AddDifference(bm)
	}

	bm = roaring64.NewBitmap()
	if err := bm.UnmarshalBinary(rs.GetExistences()); err != nil {
		return fmt.Errorf("Error unmarshalling query result existence - %v", err)
	}
	if bm.GetCardinality() > 0 {
		r.AddExistence(bm)
	}
	return nil
}

// IsEmpty - Query has no predicates.
func (q *BitmapQuery) IsEmpty() bool {

	if q.root == nil {
		return true
	}
	return false
}

// GetRootIndex - Get root index for query.
func (q *BitmapQuery) GetRootIndex() string {

	if q.root != nil {
		return q.root.Index
	}
	return ""
}

// PushLevel - Push nested predicate.
func (q *BitmapQuery) PushLevel(f *QueryFragment) {

	if f == nil {
		panic("PushLevel: fragment should never be nil!")
	}

	q.curLevel++
	q.root = f
}

// PopLevel - Pop nested predicate level.
func (q *BitmapQuery) PopLevel() *QueryFragment {

	if q.curLevel == 0 {
		panic("Cannot PopLevel at root! Pop without Push?")
	}
	if q.root.parent != nil {
		q.root = q.root.parent
	}
	q.curLevel--
	return q.root
}

func (q *BitmapQuery) addFragment(f *QueryFragment) *QueryFragment {

	if f == nil {
		panic("QueryFragment should never be nil!")
	}

	if q.root == nil {
		q.root = f
		f.Added = true
		return f
	}

	if q.root.Index == "" {
		q.root.Index = f.Index
		q.root.Field = f.Field
		q.root.RowID = f.RowID
		q.root.Value = f.Value
		q.root.Values = f.Values
		q.root.Operation = f.Operation
		q.root.BSIOp = f.BSIOp
		q.root.Begin = f.Begin
		q.root.End = f.End
		q.root.Fk = f.Fk
		q.root.Search = f.Search
		f.Added = true
		return f
	}

	if f.Operation == "INNER_JOIN" {
		if q.root.children == nil {
			q.root.children = make([]*QueryFragment, 0)
		}
		q.root.children = append(q.root.children, f)
		f.Added = true
		return f
	}

	if q.root.parent != nil {
		if q.root.parent.children == nil {
			q.root.parent.children = make([]*QueryFragment, 0)
		}
		q.root.parent.children = append(q.root.parent.children, f)
		f.Added = true
		return f
	}

	if q.root.children == nil {
		q.root.children = make([]*QueryFragment, 0)
	}
	q.root.children = append(q.root.children, f)
	f.Added = true
	return f
}

// NewQueryFragment - Construct a new predicate item.
func (q *BitmapQuery) NewQueryFragment() *QueryFragment {
	return &QueryFragment{Query: q}
}

// AddFragment to the overall query.
func (q *BitmapQuery) AddFragment(f *QueryFragment) {
	q.addFragment(f)
}

// SetBitmapPredicate attribute on the fragment (standard bitmap).
func (f *QueryFragment) SetBitmapPredicate(index, field string, rowID uint64) {
	f.Index = index
	f.Field = field
	f.RowID = rowID
}

// SetBSIPredicate attribute on the fragment (bsi).
func (f *QueryFragment) SetBSIPredicate(index, field, bsiOp string, value int64) {
	f.Index = index
	f.Field = field
	f.BSIOp = bsiOp
	f.Value = value
}

// SetParent fragment.
func (f *QueryFragment) SetParent(p *QueryFragment) {
	f.parent = p
}

// SetBSIRangePredicate - Set parameters for BSI range queries.
func (f *QueryFragment) SetBSIRangePredicate(index, field string, begin, end int64) {
	f.Index = index
	f.Field = field
	f.BSIOp = "RANGE"
	f.Begin = begin
	f.End = end
}

// SetBSIBatchEQPredicate - Set value for BSI equals batch.
func (f *QueryFragment) SetBSIBatchEQPredicate(index, field string, values []int64) {
	f.Index = index
	f.Field = field
	f.BSIOp = "BATCH_EQ"
	f.Values = values
}

// SetNullPredicate - Set predicate item == null
func (f *QueryFragment) SetNullPredicate(index, field string) {
	f.Index = index
	f.Field = field
	f.NullCheck = true
}

/*
func (q *BitmapQuery) AddJoinCriteria(index, field, fk string) *QueryFragment {
    return q.addFragment(&QueryFragment{Query: q, Index: index, Field: field, Fk: fk,
        Operation: "INNER_JOIN"}, false)
}
*/

// Dump query to log
func (q *BitmapQuery) Dump() {

	dq := q.ToProto()
	from := time.Unix(0, dq.FromTime).Format(YMDHTimeFmt)
	to := time.Unix(0, dq.ToTime).Format(YMDHTimeFmt)
	fmt.Printf(" FROM TIME: %s\n", from)
	fmt.Printf("   TO TIME: %s\n", to)
	fmt.Println("FRAGMENTS: VVV")
	for _, f := range dq.Query {
		fmt.Printf("%s->:%#v\n", f.Operation, f)
	}
}

// ToProto - Convert from API representation to protobuf for submission to GRPC wire protocols.
func (q *BitmapQuery) ToProto() *pb.BitmapQuery {

	frags := make([]*pb.QueryFragment, 0)
	q.toProtoFrag(q.root, &frags)

	bq := &pb.BitmapQuery{Query: frags}
	if fromTime, err := time.Parse(YMDHTimeFmt, q.FromTime); err == nil {
		bq.FromTime = fromTime.UnixNano()
	}
	if toTime, err := time.Parse(YMDHTimeFmt, q.ToTime); err == nil {
		bq.ToTime = toTime.UnixNano()
	}
	return bq

}

func (q *BitmapQuery) toProtoFrag(n *QueryFragment, fa *[]*pb.QueryFragment) string {

	if n == nil {
		return ""
	}
	childIds := make([]string, len(n.children))
	for i, f := range n.children {
		childIds[i] = q.toProtoFrag(f, fa)
	}

	var op pb.QueryFragment_OpType
	var bsiOp pb.QueryFragment_BSIOp
	if v, ok := pb.QueryFragment_OpType_value[n.Operation]; ok {
		switch v {
		case 0:
			op = pb.QueryFragment_INTERSECT
		case 1:
			op = pb.QueryFragment_UNION
		case 2:
			op = pb.QueryFragment_DIFFERENCE
		case 3:
			op = pb.QueryFragment_INNER_JOIN
		case 4:
			op = pb.QueryFragment_OUTER_JOIN
		}
	}
	if v, ok := pb.QueryFragment_BSIOp_value[n.BSIOp]; ok {
		switch v {
		case 0:
			bsiOp = pb.QueryFragment_NA
		case 1:
			bsiOp = pb.QueryFragment_LT
		case 2:
			bsiOp = pb.QueryFragment_LE
		case 3:
			bsiOp = pb.QueryFragment_EQ
		case 4:
			bsiOp = pb.QueryFragment_GE
		case 5:
			bsiOp = pb.QueryFragment_GT
		case 6:
			bsiOp = pb.QueryFragment_RANGE
		case 7:
			bsiOp = pb.QueryFragment_BATCH_EQ
		}
	}

	id := uuid.New()

	f := &pb.QueryFragment{Index: n.Index, Field: n.Field, RowID: n.RowID, Id: id, ChildrenIds: childIds,
		Operation: op, BsiOp: bsiOp, Value: n.Value, Begin: n.Begin, End: n.End, Fk: n.Fk, Values: n.Values,
		SamplePct: n.SamplePct, NullCheck: n.NullCheck, Negate: n.Negate}

	*fa = append(*fa, f)

	return id

}

// Reduce - Collect and aggregate intermediate results.
func (q *BitmapQuery) Reduce() *IntermediateResult {

	return q.walkReduce(q.root)
}

func (q *BitmapQuery) walkReduce(n *QueryFragment) *IntermediateResult {

	result := NewIntermediateResult(n.Index)
	result.SamplePct = n.SamplePct
	for _, f := range n.children {
		ir := q.walkReduce(f)
		for _, v := range ir.GetIntersects() {
			result.AddIntersect(v)
		}
		for _, v := range ir.GetSamples() {
			result.AddSample(v)
		}
		result.AddUnion(ir.GetFinalUnion())
		result.AddDifference(ir.GetFinalDifference())
	}

	if v, ok := pb.QueryFragment_OpType_value[n.Operation]; ok {
		switch v {
		case 0:
			//case QueryFragment_INTERSECT:
			if n.SamplePct > 0 {
				result.AddSamples(n.Sample)
				result.SampleIsUnion = false
			} else {
				result.AddIntersect(n.Data)
			}
		case 1:
			//case QueryFragment_UNION:
			if n.SamplePct > 0 {
				result.AddSamples(n.Sample)
				result.SampleIsUnion = true
			} else {
				result.AddUnion(n.Data)
			}
		case 2:
			result.AddDifference(n.Data)
			//case QueryFragment_DIFFERENCE:
			//result.AddDifference(n.Data)
			//case pb.QueryFragment_INNER_JOIN:
		}
	}

	result.Collapse()
	return result
}

// Visitor defines a function that can be used to perform transformations on query fragments in the DAG
type Visitor func(fragment *QueryFragment) error

// Visit all nodes in the query DAG and execute Visitor function
func (q *BitmapQuery) Visit(v Visitor) error {
	return q.walkDAG(q.root, v)
}

func (q *BitmapQuery) walkDAG(n *QueryFragment, v Visitor) error {

	for _, f := range n.children {
		if err := q.walkDAG(f, v); err != nil {
			return err
		}
	}
	return v(n)
}

// FromProto - Deserialize from GRPC into API structure (client side).
func FromProto(q *pb.BitmapQuery, dataMap map[string]*roaring64.Bitmap) *BitmapQuery {

	nmap := make(map[string]*QueryFragment)

	// Pass #1 populate map
	for _, v := range q.Query {
		op := ""
		switch v.Operation {
		case pb.QueryFragment_INTERSECT:
			op = "INTERSECT"
		case pb.QueryFragment_UNION:
			op = "UNION"
		case pb.QueryFragment_DIFFERENCE:
			op = "DIFFERENCE"
		case pb.QueryFragment_INNER_JOIN:
			op = "INNER_JOIN"
		case pb.QueryFragment_OUTER_JOIN:
			op = "OUTER_JOIN"
		}
		f := &QueryFragment{Index: v.Index, Field: v.Field, RowID: v.RowID, Value: v.Value, End: v.End,
			Begin: v.Begin, Fk: v.Fk, Values: v.Values, Operation: op, SamplePct: v.SamplePct}
		if dataMap != nil {
			if bm, ok := dataMap[v.Id]; ok {
				f.Data = bm
			}
		}
		if _, ok := nmap[v.Id]; !ok {
			nmap[v.Id] = f
		}

	}

	// Pass #2 create linkages
	for _, v := range q.Query {
		for _, c := range v.ChildrenIds {
			if parent, pok := nmap[v.Id]; pok {
				if child, cok := nmap[c]; cok {
					parent.children = append(parent.children, child)
					child.parent = parent
				}
			}
		}
	}

	// find root
	var root *QueryFragment
	for _, v := range nmap {
		if v.parent == nil {
			root = v
			break
		}
	}

	from := time.Unix(0, q.FromTime).Format(YMDHTimeFmt)
	to := time.Unix(0, q.ToTime).Format(YMDHTimeFmt)

	return &BitmapQuery{root: root, FromTime: from, ToTime: to}

}

//
// GroupQueryFragmentsByIndex - Break up a query and group all of the predicate fragments by index.
// This function returns a map of the predicates keyed by index name.
//
func (q *BitmapQuery) GroupQueryFragmentsByIndex() map[string]*pb.BitmapQuery {

	query := q.ToProto()

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
