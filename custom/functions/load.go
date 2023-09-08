package functions

import (
	"sync"

	"github.com/disney/quanta/qlbridge/expr"
)

// Variables to identify the build
var (
	Version string
	Build   string
)

var loadOnce sync.Once

// LoadAll - Load and register all custom functions.
func LoadAll() {
	loadOnce.Do(func() {
		expr.FuncAdd("is_bucket_public", &IsBucketPublic{})
		expr.FuncAdd("is_bucket_encrypted", &IsBucketEncrypted{})
		expr.FuncAdd("is_bucket_readable", &IsBucketReadable{})
		expr.FuncAdd("is_bucket_writable", &IsBucketWritable{})
		expr.FuncAdd("sample_stratified", &StratifiedSample{})
		expr.FuncAdd("version", &VersionFunc{})
		expr.FuncAdd("database", &DatabaseFunc{})
		expr.FuncAdd("add", &Sum{})
		expr.FuncAdd("timediff", &TimeDiff{})
	})
}
