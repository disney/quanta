package test

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
)

// TestLocalBasic3then4_continuous will get a cluster started, and start loading data into it.
// While that is happening we add a node.
// See if things got lost.
// FIXME: this test shows that replicas are lost when a node is added. This is a bug.
func TestLocalBasic3then4_continuous(t *testing.T) {

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	isLocalRunning := IsLocalRunning()

	// erase the storage
	if !isLocalRunning { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	} else {
		t.Error("we don't expect a cluster to be running")
		return
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster(3)

	TestStatesAllMatch(t, state, "initial")

	// clear the table
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin drop customers_qa"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin create customers_qa"}, true)
	time.Sleep(10 * time.Second)

	// add 10 records
	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf("insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('%d','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');", i)
		AnalyzeRow(*state.ProxyConnect, []string{sql}, true)
	}
	AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)
	time.Sleep(1 * time.Second)

	vectors := []string{"customers_qa/cust_id/1970-01-01T00"}

	time.Sleep(10 * time.Second)
	DumpField(t, state, vectors) // see the mapping

	addedCount := 0
	addingFinished := false
	var addedCountLock sync.Mutex
	go func() {
		// for 30 seconds add data, then quit. 60 records total.
		//start := time.Now()
		for addedCount < 60 { //time.Since(start) < 30*time.Second {

			addedCountLock.Lock()

			sql := fmt.Sprintf("insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('%d','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');", addedCount+600)
			AnalyzeRow(*state.ProxyConnect, []string{sql}, true)

			addedCount++
			addedCountLock.Unlock()

			time.Sleep(500 * time.Millisecond)
		}
		addingFinished = true
	}()

	// query, wait for first 5 records to be added
	for {
		addedCountLock.Lock()
		if addedCount >= 5 {
			AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)

			// got := AnalyzeRow(*state.ProxyConnect, []string{"select cust_id,zip from customers_qa"}, true)
			got := AnalyzeRow(*state.ProxyConnect, []string{"select cust_id from customers_qa"}, true)
			for i := 0; i < len(got.RowDataArray); i++ {
				json, err := json.Marshal(got.RowDataArray[i])

				check(err)
				fmt.Printf("%v\n", string(json))
			}
			assert.EqualValues(t, 15, got.ActualRowCount)
			addedCountLock.Unlock()
			break
		}
		addedCountLock.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

	// check the data distribution.
	time.Sleep(1 * time.Second)
	DumpField(t, state, vectors) // check the mapping

	fmt.Println("---- before adding node 3 ----")

	// now add a node
	node, err := StartNode(3) // this will be node 4
	check(err)
	state.nodes = append(state.nodes, node)

	// wait for sync
	// WaitForStatusGreenLocal()
	TestStatesAllMatch(t, state, "added node")

	time.Sleep(1 * time.Second)
	fmt.Println("---- after adding node 3 ----")
	// check the data
	DumpField(t, state, vectors)
	fmt.Println("----")

	// wait for the data adding thread to finish.
	for {
		if addingFinished {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)
	// check the data.

	// got := AnalyzeRow(*state.ProxyConnect, []string{"select cust_id,zip from customers_qa"}, true)
	got := AnalyzeRow(*state.ProxyConnect, []string{"select cust_id from customers_qa"}, true)
	for i := 0; i < len(got.RowDataArray); i++ {
		json, err := json.Marshal(got.RowDataArray[i])
		check(err)
		fmt.Printf("%v\n", string(json))
	}
	assert.EqualValues(t, 70, got.ActualRowCount)

	fmt.Println("---- much later ----")
	// check the data
	DumpField(t, state, vectors)
	fmt.Println("----")

	time.Sleep(10 * time.Second)

	// release as necessary
	// or, just abandon it
	state.Release()
}

func TestLocalBasic3then4Zip(t *testing.T) {

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	isLocalRunning := IsLocalRunning()

	// erase the storage
	if !isLocalRunning { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster(3)

	TestStatesAllMatch(t, state, "initial")

	// load something
	if !isLocalRunning {
		ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_load.sql")
	}

	vectors := []string{"customers_qa/zip/1970-01-01T00"}

	time.Sleep(5 * time.Second)
	DumpField(t, state, vectors) // see the mapping

	// query
	{
		// select zip from customers_qa where zip is not null
		got := ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_body_bug_zip.sql")

		for _, child := range got.FailedChildren {
			fmt.Println("child failed", child.Statement)
		}

		assert.EqualValues(t, got.ExpectedRowcount, got.ActualRowCount)
		assert.EqualValues(t, 0, len(got.FailedChildren))

		fmt.Println("TestLocalBasic3then4 3 nodes ", got.ExpectedRowcount, got.ActualRowCount)
	}

	// check the data
	time.Sleep(5 * time.Second)
	DumpField(t, state, vectors) // see the mapping

	fmt.Println("---- before adding node 3 ----")

	localConsulAddress := "127.0.0.1" // we have to use the mapping when we're outside the container
	consulClient, err := api.NewClient(&api.Config{Address: localConsulAddress + ":8500"})
	check(err)
	err = shared.SetClusterSizeTarget(consulClient, 4)
	check(err)

	// now add a node
	node, err := StartNode(3)
	check(err)
	state.nodes = append(state.nodes, node)

	err = shared.SetClusterSizeTarget(consulClient, 4)
	check(err)

	// wait for sync
	// WaitForStatusGreenLocal()
	TestStatesAllMatch(t, state, "added node")

	time.Sleep(5 * time.Second) // let batchProcessLoop shard count print out
	fmt.Println("---- after adding node 3 ----")
	// check the data
	DumpField(t, state, vectors)
	fmt.Println("----")

	{
		got := ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_body_bug_zip.sql")
		assert.EqualValues(t, got.ExpectedRowcount, got.ActualRowCount)
		assert.EqualValues(t, 0, len(got.FailedChildren))

		fmt.Println("TestLocalBasic3then4 4 nodes ", got.ExpectedRowcount, got.ActualRowCount)

		for _, child := range got.FailedChildren {
			fmt.Println("child failed", child.Statement)
		}
	}

	// time.Sleep(30 * time.Second) // let batchProcessLoop shard count print out
	fmt.Println("---- much later ----")
	// check the data
	DumpField(t, state, vectors)
	fmt.Println("----")

	// release as necessary
	state.Release()
}
