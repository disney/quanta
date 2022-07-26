package source

// Projection handling task for query processor.

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Task Runner interface
	_ exec.TaskRunner = (*QuantaProjection)(nil)
)

// QuantaProjection does nothing but pass incoming messages to the output channel.
type QuantaProjection struct {
	*exec.TaskBase
}

// NewQuantaProjection - Construct a QuantaProjection task.
func NewQuantaProjection(ctx *plan.Context) exec.TaskRunner {

	m := &QuantaProjection{
		TaskBase: exec.NewTaskBase(ctx),
	}

	var err error
	m.Ctx.Projection.Proj, _, _, _, _, err = createFinalProjection(m.Ctx.Stmt.(*rel.SqlSelect), m.Ctx.Schema, "")
	if err != nil {
		u.Errorf("QuantaProjection error %v\n", err)
		m.Ctx.Errors = append(m.Ctx.Errors, err)
	}
	return m
}

// Run the task.
func (m *QuantaProjection) Run() error {

	defer m.Ctx.Recover()
	defer close(m.MessageOut())

	outCh := m.MessageOut()

	inCh := m.MessageIn()

msgReadLoop:
	for {

		select {
		case <-m.SigChan():
			return nil
		case msg, ok := <-inCh:
			if !ok {
				break msgReadLoop
			} else {
				outCh <- msg
			}
		}
	}
	return nil
}
