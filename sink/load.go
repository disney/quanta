package sink

import (
	"sync"

	"github.com/disney/quanta/qlbridge/exec"
)

var loadOnce sync.Once

// LoadAll custom functions.
func LoadAll() {
	loadOnce.Do(func() {
		exec.Register("s3", NewS3Sink)
		exec.Register("table", NewTableSink)
	})
}
