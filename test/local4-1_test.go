package test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
)

func FixmeTestLocalBasic4minus1(t *testing.T) {

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
	state := Ensure_cluster(4)

	vectors := []string{"customers_qa/isActive/0/1970-01-01T00", "customers_qa/isActive/1/1970-01-01T00"}

	testStatesAllMatch(t, state, "initial")
	dumpField(t, state, vectors) // see the mapping

	// load something
	if !isLocalRunning {
		ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_load.sql")
	}

	testStatesAllMatch(t, state, "after basic_queries_load")
	dumpField(t, state, vectors) // see the mapping

	// wait at least 10 sec for cleanupStrandedShards to happen
	time.Sleep(15 * time.Second)

	testStatesAllMatch(t, state, "after wait for cleanupStrandedShards")
	dumpField(t, state, vectors) // see the mapping

	// query
	{
		// select * from customers_qa where isActive = false;@6
		got := ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_body_bug.sql")

		for _, child := range got.FailedChildren {
			fmt.Println("child failed", child.Statement)
		}

		assert.EqualValues(t, got.ExpectedRowcount, got.ActualRowCount)
		assert.EqualValues(t, 0, len(got.FailedChildren))

		fmt.Println("TestLocalBasic4minus1 4 nodes wanted, got", got.ExpectedRowcount, got.ActualRowCount)
	}

	// check the data
	fmt.Println("check map after query")
	testStatesAllMatch(t, state, "after query")
	dumpField(t, state, vectors) // see the mapping

	fmt.Println("---- before removing node 4 ----")

	localConsulAddress := "127.0.0.1" // we have to use the mapping when we're outside the container
	consulClient, err := api.NewClient(&api.Config{Address: localConsulAddress + ":8500"})
	check(err)
	err = shared.SetClusterSizeTarget(consulClient, 3)
	check(err)

	// now delete a node
	err = shared.SetClusterSizeTarget(consulClient, 3) // FIXME: (atw) we should only need to do this AFTER the node is stopped
	check(err)

	state.nodes[3].Stop <- true
	state.nodes = state.nodes[:3]

	err = shared.SetClusterSizeTarget(consulClient, 3) // this official note that the new size is 3
	check(err)

	// wait for sync
	// WaitForStatusGreenLocal()
	testStatesAllMatch(t, state, "deleted node 4")

	time.Sleep(15 * time.Second) // let batchProcessLoop shard count print out

	// check the data
	fmt.Println("---- after removing 4 ----")
	dumpField(t, state, vectors)

	{
		got := ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_body_bug.sql")
		assert.EqualValues(t, got.ExpectedRowcount, got.ActualRowCount)
		assert.EqualValues(t, 0, len(got.FailedChildren))

		fmt.Println("TestLocalBasic4minus1 3 nodes ", got.ExpectedRowcount, got.ActualRowCount)

		for _, child := range got.FailedChildren {
			fmt.Println("child failed", child.Statement)
		}
	}

	// release as necessary
	state.Release()
}
