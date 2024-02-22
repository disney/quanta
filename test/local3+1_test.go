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

func TestLocalBasic3then4(t *testing.T) {

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

	vectors := []string{"customers_qa/isActive/0/1970-01-01T00", "customers_qa/isActive/1/1970-01-01T00"}

	testStatesAllMatch(t, state, "initial")

	// WaitForStatusGreenLocal()
	// do we have to? time.Sleep(5 * time.Second)

	// load something
	if !isLocalRunning {
		ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_load.sql")
	}

	time.Sleep(5 * time.Second)
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

		fmt.Println("TestLocalBasic3then4 3 nodes ", got.ExpectedRowcount, got.ActualRowCount)
	}

	// check the data
	time.Sleep(5 * time.Second)
	dumpField(t, state, vectors) // see the mapping

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
	testStatesAllMatch(t, state, "added node")

	time.Sleep(5 * time.Second) // let batchProcessLoop shard count print out
	fmt.Println("---- after adding node 3 ----")
	// check the data
	dumpField(t, state, vectors)
	fmt.Println("----")

	{
		got := ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_body_bug.sql")
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
	dumpField(t, state, vectors)
	fmt.Println("----")

	// release as necessary
	state.Release()
}

func TestStartLocal(t *testing.T) { // this is NOT a test. Too Slow. I use this manually
	// TestBasic3 and then run this and see if it fails

	state := &ClusterLocalState{}

	go func(state *ClusterLocalState) {
		for {
			vectors := []string{"customers_qa/numFamilyMembers/1969-12-31T16"}
			dumpField(t, state, vectors)
			time.Sleep(1 * time.Second)
		}
	}(state)

	Ensure_this_cluster(3, state)

	time.Sleep(99999999 * time.Second)

}
func TestBasic3(t *testing.T) {

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

	if !isLocalRunning {
		ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_load.sql")
	}

	vectors := []string{"customers_qa/isActive/0/1970-01-01T00", "customers_qa/isActive/1/1970-01-01T00"}

	testStatesAllMatch(t, state, "initial")
	dumpField(t, state, vectors)

	// release as necessary
	state.Release()

}
