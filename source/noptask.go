package source
// NopTask - Empty task to satisfy query processor.

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Task Runner interface
	_ exec.TaskRunner = (*NopTask)(nil)
)

// NopTask does nothing but pass incoming messages to the output channel.
type NopTask struct {
	*exec.TaskBase
}

// NewNopTask - Construct a NopTask
func NewNopTask(ctx *plan.Context) exec.TaskRunner {

	m := &NopTask{
		TaskBase: exec.NewTaskBase(ctx),
	}

	return m
}

// Run a NopTask
func (m *NopTask) Run() error {

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
