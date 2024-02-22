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

// TestRetainData fails when we can't get port 4000 closed and reopened  FIXME:
func TestRetainData(t *testing.T) {

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
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
	state := Ensure_cluster(3)
	state.Db, err = state.ProxyConnect.ProxyConnectConnect()
	_ = err

	// load something

	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin drop orders_qa"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin drop customers_qa"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin create customers_qa"}, true)

	AnalyzeRow(*state.ProxyConnect, []string{"insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('101','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');"}, true)

	// query

	got := AnalyzeRow(*state.ProxyConnect, []string{"select cust_id from customers_qa where cust_id != NULL ;@1"}, true)
	assert.EqualValues(t, got.ExpectedRowcount, got.ActualRowCount)

	got = AnalyzeRow(*state.ProxyConnect, []string{"select first_name from customers_qa;@1"}, true)
	assert.EqualValues(t, got.ExpectedRowcount, got.ActualRowCount)

	// release as necessary
	fmt.Println("releasing local in memory cluster")
	state.Release()

	// restart the cluster
	fmt.Println("starting local in memory cluster in 120")
	time.Sleep(120 * time.Second) // wait for port 4000 to be released
	state = Ensure_cluster(3)

	vectors := []string{"customers_qa/cust_id/1970-01-01T00"}

	dumpField(t, state, vectors)

	// query

	got = AnalyzeRow(*state.ProxyConnect, []string{"select first_name from customers_qa;@1"}, true)
	assert.EqualValues(t, got.ExpectedRowcount, got.ActualRowCount)

	state.Release()
}
