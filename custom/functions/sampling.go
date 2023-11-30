package functions

import (
	"fmt"

	u "github.com/araddon/gou"
	"github.com/disney/quanta/qlbridge/expr"
	"github.com/disney/quanta/qlbridge/value"
	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/source"
)

// StratifiedSample - Perform stratified sampling.  Use in query predicate attribute filter.
//
// stratifed_sample("fieldName", 1.5)
type StratifiedSample struct{}

// Type - return type.
func (m *StratifiedSample) Type() value.ValueType { return value.NilType }

// Validate stratified_sample parameters.
func (m *StratifiedSample) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for stratified_sample(fieldName, percentage) but got %d", len(n.Args))
	}
	return stratifiedSampleEval, nil
}

func stratifiedSampleEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	var q *shared.QueryFragment
	x, _ := ctx.Get("q")
	q = x.Value().(*shared.QueryFragment)
	var m *source.SQLToQuanta
	y, _ := ctx.Get("m")
	m = y.Value().(*source.SQLToQuanta)

	q.Field = args[0].Value().(string)

	if fr, ft, err := m.ResolveField(m.ResolveTable(nil), q.Field); ft || err != nil {
		if ft {
			u.Errorf("Sampling on BSI field %s not supported.", q.Field)
			return nil, false
		}
		u.Errorf("stratified_sample error %s.", err)
	} else {
		q.Index = fr.Parent.Name
	}
	switch args[1].Type() {
	case value.NumberType:
		q.SamplePct = float32(args[1].Value().(float64))
	case value.IntType:
		q.SamplePct = float32(args[1].Value().(int64))
	}

	return value.NewNilValue(), true
}
