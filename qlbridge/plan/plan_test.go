package plan_test

import (
	"testing"

	td "github.com/disney/quanta/qlbridge/datasource/mockcsvtestdata"
	"github.com/disney/quanta/qlbridge/testutil"
)

func init() {
	testutil.Setup()
	// load our mock data sources "users", "articles"
	td.LoadTestDataOnce()
}

// atw FIXME:
func XXTestRunTestSuite(t *testing.T) {
	testutil.RunDDLTests(t)
	testutil.RunTestSuite(t)
}
