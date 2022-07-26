package functions

import (
	"fmt"
	"math"
	"time"

    //u "github.com/araddon/gou"
    "github.com/araddon/dateparse"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

// Avg average of values.  Note, this function DOES NOT persist state doesn't aggregate
// across multiple calls.  That would be responsibility of write context.
//
//    avg(1,2,3) => 2.0, true
//    avg("hello") => math.NaN, false
//
type Avg struct{}

// Type is NumberType
func (m *Avg) Type() value.ValueType { return value.NumberType }
func (m *Avg) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected 1 or more args for Avg(arg, arg, ...) but got %s", n)
	}
	return avgEval, nil
}
//func (m *Avg) IsAgg() bool { return true }

func avgEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	avg := float64(0)
	ct := 0
	for _, val := range vals {
		switch v := val.(type) {
		case value.StringsValue:
			for _, sv := range v.Val() {
				if fv, ok := value.StringToFloat64(sv); ok && !math.IsNaN(fv) {
					avg += fv
					ct++
				} else {
					return value.NumberNaNValue, false
				}
			}
		case value.SliceValue:
			for _, sv := range v.Val() {
				if fv, ok := value.ValueToFloat64(sv); ok && !math.IsNaN(fv) {
					avg += fv
					ct++
				} else {
					return value.NumberNaNValue, false
				}
			}
		case value.StringValue:
			if fv, ok := value.StringToFloat64(v.Val()); ok {
				avg += fv
				ct++
			}
		case value.NumericValue:
			avg += v.Float()
			ct++
		}
	}
	if ct > 0 {
		return value.NewNumberValue(avg / float64(ct)), true
	}
	return value.NumberNaNValue, false
}

// Sum function to add values. Note, this function DOES NOT persist state doesn't aggregate
// across multiple calls.  That would be responsibility of write context.
//
//   sum(1, 2, 3) => 6
//   sum(1, "horse", 3) => nan, false
//
type Sum struct{}

// Type is number
func (m *Sum) Type() value.ValueType { return value.NumberType }

// IsAgg yes sum is an agg.
//func (m *Sum) IsAgg() bool { return true }

func (m *Sum) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected 1 or more args for Sum(arg, arg, ...) but got %s", n)
	}
	return sumEval, nil
}

func sumEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	sumval := float64(0)
	for _, val := range vals {
		if val == nil || val.Nil() || val.Err() {
			// we don't need to evaluate if nil or error
		} else {
			switch v := val.(type) {
			case value.StringValue:
				if fv, ok := value.StringToFloat64(v.Val()); ok && !math.IsNaN(fv) {
					sumval += fv
				}
			case value.StringsValue:
				for _, sv := range v.Val() {
					if fv, ok := value.StringToFloat64(sv); ok && !math.IsNaN(fv) {
						sumval += fv
					}
				}
			case value.SliceValue:
				for _, sv := range v.Val() {
					if fv, ok := value.ValueToFloat64(sv); ok && !math.IsNaN(fv) {
						sumval += fv
					} else {
						return value.NumberNaNValue, false
					}
				}
			case value.NumericValue:
				fv := v.Float()
				if !math.IsNaN(fv) {
					sumval += fv
				}
			default:
				// Do we silently drop, or fail?
				return value.NumberNaNValue, false
			}
		}
	}
	if sumval == float64(0) {
		return value.NumberNaNValue, false
	}
	return value.NewNumberValue(sumval), true
}


// TimeDiff function to add values. Note, this function DOES NOT persist state doesn't aggregate
// across multiple calls.  That would be responsibility of write context.
//
//   timediff("now", "Apr 7, 2014 4:58:55 PM") => 72765h8m31.126651125s 
//   timediff("now", "Apr 7, 2014 4:58:55 PM", "milliseconds") => 261954462212
//
type TimeDiff struct{}

// Type is number
func (m *TimeDiff) Type() value.ValueType { return value.StringType }

func (m *TimeDiff) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {

	if len(n.Args) < 2 || len(n.Args) > 3 {
		return nil, fmt.Errorf("Expected 2 or 3 args for TimeDiff(arg, arg, ...) but got %s", n)
	}
	return timeDiffEval, nil
}

func timeDiffEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	outFormat := "duration"
	if len(vals) == 3 {
		outFormat = vals[2].ToString()
	}
	var value1, value2 time.Time
	switch vals[0].(type) {
	case value.StringValue:
		if vals[0].ToString() == "now" {
			value1 = time.Now()
			break
		}
		if t, err := dateparse.ParseAny(vals[0].ToString()); err != nil {
			return value.NewStringValue(""), false
		} else {
			value1 = t
		}
	}

	switch vals[1].(type) {
	case value.StringValue:
		if vals[1].ToString() == "now" {
			value2 = time.Now()
			break
		}
		if t, err := dateparse.ParseAny(vals[1].ToString()); err != nil {
			return value.NewStringValue(""), false
		} else {
			value2 = t
		}
	}

	diff :=  value1.Sub(value2)
	switch outFormat {
	case "nanoseconds":
		return value.NewStringValue(fmt.Sprintf("%v", diff.Nanoseconds())), true
	case "microseconds":
		return value.NewStringValue(fmt.Sprintf("%v", diff.Microseconds())), true
	case "milliseconds":
		return value.NewStringValue(fmt.Sprintf("%v", diff.Milliseconds())), true
	case "seconds":
		return value.NewStringValue(fmt.Sprintf("%v", diff.Seconds())), true
	case "minutes":
		return value.NewStringValue(fmt.Sprintf("%v", diff.Minutes())), true
	case "hours":
		return value.NewStringValue(fmt.Sprintf("%v", diff.Hours())), true
	}
	return value.NewStringValue(diff.String()), true
}
