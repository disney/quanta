package functions

import (
	"fmt"

	"github.com/disney/quanta/qlbridge/expr"
	"github.com/disney/quanta/qlbridge/value"
)

// VersionFunc return version number.
type VersionFunc struct{}

// Type - return type.
func (m *VersionFunc) Type() value.ValueType { return value.StringType }

// Validate stratified_sample parameters.
func (m *VersionFunc) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 0 {
		return nil, fmt.Errorf("Expected 0 args for version() but got %d", len(n.Args))
	}
	return versionEval, nil
}

func versionEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	//return value.NewStringsValue([]string{"8.0.12", "Quanta version " + Version + " - Build: " + Build}), true
	return value.NewStringValue("8.0.12"), true
}

// DatabaseFunc - return database name
type DatabaseFunc struct{}

// Type - return type.
func (m *DatabaseFunc) Type() value.ValueType { return value.StringType }

// Validate stratified_sample parameters.
func (m *DatabaseFunc) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 0 {
		return nil, fmt.Errorf("Expected 0 args for version() but got %d", len(n.Args))
	}
	return versionEval, nil
}

func databaseEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	return value.NewStringValue("quanta"), true
}
