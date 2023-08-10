package sink

// TableSink - Support for SELECT * INTO "<table>"

import (
	//u "github.com/araddon/gou"
	"database/sql/driver"

	"github.com/disney/quanta/qlbridge/exec"
	"github.com/disney/quanta/qlbridge/plan"
)

type (
	// TableSink - State for table implemention of Sink interface.
	TableSink struct{}
)

var (
	// Ensure that we implement the Sink interface
	// to ensure this can be called form the Into task
	_ exec.Sink = (*TableSink)(nil)
)

// NewTableSink - Construct TableSink
func NewTableSink(ctx *plan.Context, outTable string, params map[string]interface{}) (exec.Sink, error) {
	s := &TableSink{}
	err := s.Open(ctx, outTable, params)
	return s, err
}

// Open output session to table
func (s *TableSink) Open(ctx *plan.Context, destination string, params map[string]interface{}) error {
	return nil
}

// Next - Write next batch of data to session.
func (s *TableSink) Next(dest []driver.Value, colIndex map[string]int) error {
	return nil
}

// Close output session.
func (s *TableSink) Close() error {
	return nil
}

// Cleanup output session.
func (s *TableSink) Cleanup() error {
	return nil
}
