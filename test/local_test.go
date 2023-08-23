package test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/disney/quanta/shared"

	"github.com/stretchr/testify/assert"
)

func TestLocalQuery(t *testing.T) {

	var err error

	shared.SetUTCdefault()

	// erase the storage
	if !IsLocalRunning() { // if no cluster is up
		err = os.RemoveAll("./test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster()

	// load something

	AnalyzeRow(*state.proxyConnect, []string{"quanta-admin drop orders_qa"}, true)
	AnalyzeRow(*state.proxyConnect, []string{"quanta-admin drop customers_qa"}, true)
	AnalyzeRow(*state.proxyConnect, []string{"quanta-admin create customers_qa"}, true)

	AnalyzeRow(*state.proxyConnect, []string{"insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('101','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');"}, true)

	// query

	AnalyzeRow(*state.proxyConnect, []string{"select * from customers_qa;@1"}, true)

	assert.EqualValues(t, FailCount, 0) // FailCount in sql-types.go

	// release as necessary
	// FIXME: state.Release()
}

func TestRetainData(t *testing.T) {

	var err error

	shared.SetUTCdefault()

	// erase the storage
	if !IsLocalRunning() { // if no cluster is up
		err = os.RemoveAll("./test/localClusterData/") // start fresh
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

	assert.EqualValues(t, FailCount, 0) // FailCount in sql-types.go

	// release as necessary
	state.Release()
	// wait for consul to notice?
	time.Sleep(10 * time.Second)

	// restart the cluster
	state = Ensure_cluster()

	// query

	AnalyzeRow(*state.proxyConnect, []string{"select * from customers_qa;@1"}, true)

	assert.EqualValues(t, FailCount, 0) // FailCount in sql-types.go

	state.Release()
}
