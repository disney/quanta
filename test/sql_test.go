package test

import (
	"fmt"
	"os"
	"testing"

	admin "github.com/disney/quanta/quanta-admin-lib"
	"github.com/disney/quanta/shared"

	"github.com/stretchr/testify/assert"
)

// This requires that consul is running on localhost:8500
// If a cluster is running it will use that cluster.
// If not it will start a cluster.

func TestShowTables(t *testing.T) {

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	// erase the storage
	if !IsLocalRunning() { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster(3)

	ctx := admin.Context{ConsulAddr: "localhost:8500", Port: 4000}

	cmd := admin.TablesCmd{}
	out := captureStdout(func() {
		cmd.Run(&ctx) // sent to console
	})
	fmt.Println("initial tables", out)

	// load something

	// AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin drop orders_qa"}, true)
	// AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin drop customers_qa"}, true)
	// default dir is ./sqlrunner/config/
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin create ../../test/testdata/config/cities"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin create ../../test/testdata/config/cityzip"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin create customers_qa"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin create orders_qa"}, true)

	cmd = admin.TablesCmd{}
	out = captureStdout(func() {
		cmd.Run(&ctx) // sent to console
	})
	// fmt.Println(out)
	assert.Contains(t, out, "cities")
	assert.Contains(t, out, "cityzip")
	assert.Contains(t, out, "customers_qa")
	assert.Contains(t, out, "orders_qa")

	// now, the sql version:

	showTablesGot := AnalyzeRow(*state.ProxyConnect, []string{"show tables;@2"}, true)
	fmt.Println(showTablesGot.RowDataArray) //  is [map[Table:customers_qa] map[Table:orders_qa]]
	assert.EqualValues(t, showTablesGot.RowDataArray[0]["Table"], "cities")
	assert.EqualValues(t, showTablesGot.RowDataArray[1]["Table"], "cityzip")
	assert.EqualValues(t, showTablesGot.RowDataArray[2]["Table"], "customers_qa")
	assert.EqualValues(t, showTablesGot.RowDataArray[3]["Table"], "orders_qa")

	// should we check mysql through the console?

	// time.Sleep(999999999 * time.Second) // stay alive forever -
	// -p "" // omit for no password

	showcmd := `mysql -h127.0.0.1 -P4000 -Dquanta -u"DEEMC004"  -A -e 'show tables'`
	shellResult, err := Shell(showcmd, "") // \n is the password
	check(err)
	// fmt.Println(result)
	assert.Contains(t, shellResult, "cities")
	assert.Contains(t, shellResult, "cityzip")
	assert.Contains(t, shellResult, "customers_qa")
	assert.Contains(t, shellResult, "orders_qa")

	// release as necessary
	state.Release()
}
