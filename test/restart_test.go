package test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/disney/quanta/shared"

	"github.com/stretchr/testify/assert"
)

// This requires that consul is running on localhost:8500
// and that a cluster is NOT running.

func TestRetainData(t *testing.T) {

	var err error

	shared.SetUTCdefault()

	// erase the storage
	if !IsLocalRunning() { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	} else {
		fmt.Println("FAIL cluster already running")
		t.FailNow()
	}
	// ensure_custer
	state := Ensure_cluster()
	state.db, err = state.proxyConnect.ProxyConnectConnect()
	_ = err

	// load something

	AnalyzeRow(*state.proxyConnect, []string{"quanta-admin drop orders_qa"}, true)
	AnalyzeRow(*state.proxyConnect, []string{"quanta-admin drop customers_qa"}, true)
	AnalyzeRow(*state.proxyConnect, []string{"quanta-admin create customers_qa"}, true)

	AnalyzeRow(*state.proxyConnect, []string{"insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('101','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');"}, true)

	// query

	AnalyzeRow(*state.proxyConnect, []string{"select * from customers_qa;@1"}, true)
	assert.EqualValues(t, 0, FailCount) // FailCount in sql-types.go

	// release as necessary
	fmt.Println("releasing local in memory cluster")
	state.Release()
	// restart the cluster

	fmt.Println("starting local in memory cluster")
	time.Sleep(10 * time.Second)
	state = Ensure_cluster()

	// query

	AnalyzeRow(*state.proxyConnect, []string{"select * from customers_qa;@1"}, true)

	assert.EqualValues(t, 0, FailCount) // FailCount in sql-types.go

	state.Release()
}
