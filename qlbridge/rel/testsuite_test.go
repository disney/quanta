package rel_test

import (
	"testing"

	"github.com/disney/quanta/qlbridge/testutil"
)

func init() {
	testutil.Setup()
}
func TestSuite(t *testing.T) {
	testutil.RunTestSuite(t)
}
