package test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	admin "github.com/disney/quanta/quanta-admin-lib"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
)

func testStatesAllMatch(t *testing.T, state *ClusterLocalState) {
	fmt.Println("testStatesAllMatch ")

	consulAddress := "127.0.0.1:8500"

	ctx := admin.Context{ConsulAddr: consulAddress,
		Port:  4000,
		Debug: true}
	_ = ctx

	start := time.Now()
	// first we will scan the state and wait for it to be green
	for {
		time.Sleep(100 * time.Millisecond)
		states := make([]string, 0)
		clusterState, active, target := state.proxyControl.Src.GetConnection().GetClusterState()
		states = append(states, clusterState.String())
		fmt.Println("dump clusterState", clusterState, "active", active, "target", target)
		_ = active
		_ = target

		adminstate := GetStatusViaAdminLocal() // should return the same thing
		fmt.Println("testStatesAllMatch state", adminstate)
		states = append(states, adminstate)

		for _, node := range state.nodes {
			nodeState, active, target := node.Conn.GetClusterState()
			states = append(states, nodeState.String())
			_ = active
			_ = target
		}

		matched := true

		// they must match
		for i := 1; i < len(states); i++ {
			if states[i] != states[0] {
				fmt.Println("dumpIsActive states MISMATCH", states)
				time.Sleep(500 * time.Millisecond)
				matched = false
			}
		}
		if time.Since(start) > 30*time.Second {
			fmt.Println("testStatesAllMatch timeout")
			t.Error("dumpIsActivetestStatesAllMatch timeout", states)
			break
		}
		if clusterState == shared.Green && matched {
			break
		}
	}
}

type Mappings []string

// dumpIsActive return shard info for "customers_qa/isActive/1/1970-01-01T00"
// or "customers_qa/zip/1970-01-01T00"
// or whatever's in vectors
func dumpField(t *testing.T, state *ClusterLocalState, vectors []string) {

	fieldMatch := strings.Split(vectors[0], "/")[1]

	fmt.Println("bitmap dump of ", fieldMatch, vectors[0])

	testStatesAllMatch(t, state)

	//mappings0 := make([]string, 0)
	//mappings1 := make([]string, 0)

	mappings := make([]Mappings, len(vectors))
	for i := range vectors {
		mappings[i] = make([]string, 0)
	}

	conn := state.proxyControl.Src.GetConnection()

	for i, v := range vectors {
		nodes, err := conn.SelectNodes(v, shared.AllActive)
		check(err)
		m := mappings[i]
		mappings[i] = append(m, fmt.Sprintf("%v", nodes))
		fmt.Println("proxy hash nodes", i, nodes)
	}

	// nodes, err := conn.SelectNodes("customers_qa/isActive/0/1970-01-01T00", shared.AllActive)
	// check(err)
	// mappings0 = append(mappings0, fmt.Sprintf("%v", nodes))
	// fmt.Println("proxy hash nodes 0 ", nodes)

	// nodes, err = conn.SelectNodes("customers_qa/isActive/1/1970-01-01T00", shared.AllActive)
	// check(err)
	// mappings1 = append(mappings1, fmt.Sprintf("%v", nodes))
	// fmt.Println("proxy hash nodes 1 ", nodes)

	for n, node := range state.nodes {
		for i, v := range vectors {
			nodes, err := node.Conn.SelectNodes(v, shared.AllActive)
			check(err)
			fmt.Println("node hash nodes", i, n, nodes)
			m := mappings[i]
			mappings[i] = append(m, fmt.Sprintf("%v", nodes))

		}
		// nodes, err = node.Conn.SelectNodes("customers_qa/isActive/0/1970-01-01T00", shared.AllActive)
		// check(err)
		// fmt.Println("node hash nodes 0", i, nodes)
		// mappings0 = append(mappings0, fmt.Sprintf("%v", nodes))
		// nodes, err = node.Conn.SelectNodes("customers_qa/isActive/1/1970-01-01T00", shared.AllActive)
		// check(err)
		// fmt.Println("node hash nodes 1", i, nodes)
		// mappings1 = append(mappings1, fmt.Sprintf("%v", nodes))
	}
	for i := range vectors {

		m := mappings[i]
		fmt.Println("mappings ", i, " collected", m)
		// they should all match
		for i := 1; i < len(m); i++ {
			if m[i] != m[0] {
				fmt.Println("dumpIsActive mappings0 MISMATCH", m)
				t.Error("dumpIsActive mappings0 MISMATCH", m)
			}
		}
		// for i := 1; i < len(mappings1); i++ {
		// 	if mappings1[i] != mappings1[0] {
		// 		fmt.Println("dumpIsActive mappings0 MISMATCH", mappings1)
		// 		t.Error("dumpIsActive mappings0 MISMATCH", mappings1)
		// 	}
		// }
	}

	allIntegers := make(map[uint64]uint64, 0)

	for i, node := range state.nodes {
		tmp := state.nodes[i].GetNodeService("BitmapIndex")
		bitmap := tmp.(*server.BitmapIndex)
		_ = bitmap
		c := bitmap.GetBitmapCache()
		fmt.Print("bitmap indexName ")
		for indexName, fm := range c {
			fmt.Println(" ", indexName, node.GetNodeID())
			for fieldName, rm := range fm {
				if fieldName != fieldMatch {
					continue
				}
				fmt.Println("bitmap fieldName ", fieldName)
				for rowID, tm := range rm {
					fmt.Println("bitmap rowID ", rowID)
					for ts, bitmap := range tm {
						bitmap.Lock.Lock()
						arr := bitmap.Bits.ToArray()
						for _, v := range arr {
							got, ok := allIntegers[v]
							if !ok {
								got = 0
							}
							allIntegers[v] = got + 1
						}
						//partition := &server.Partition{Index: indexName, Field: fieldName, Time: time.Unix(0, ts),
						//	TQType: bitmap.TQType, RowIDOrBits: int64(rowID), Shard: bitmap}

						fmt.Println("bitmap time ", time.Unix(0, ts), "ints", arr)
						bitmap.Lock.Unlock()
					}
				}
			}
		}

		bsi := bitmap.GetBsiCache()
		{
			for indexName, fm := range bsi {
				fmt.Println("bsi indexName ", indexName, node.GetNodeID())
				for fieldName, tm := range fm {
					if fieldName != fieldMatch {
						continue
					}
					fmt.Println("bsi fieldName ", fieldName)
					for ts, bsi := range tm {
						bsi.Lock.Lock()
						partition := &server.Partition{Index: indexName, Field: fieldName, Time: time.Unix(0, ts),
							TQType: bsi.TQType, RowIDOrBits: -1, Shard: bsi}
						_ = partition
						// fmt.Println("bsi partition ", partition)

						fmt.Println("bsi minmax ", fieldName, bsi.MinValue, bsi.MaxValue)
						//val, _ := bsi.GetValue(0)
						//fmt.Println("bsi val 0 ", fieldName, val)

						bm := bsi.GetExistenceBitmap()
						arr := bm.ToArray()
						for _, v := range arr {
							got, ok := allIntegers[v]
							if !ok {
								got = 0
							}
							allIntegers[v] = got + 1
						}
						fmt.Println("bitmap time ", time.Unix(0, ts), "ints", arr)

						bsi.Lock.Unlock()
					}
				}
			}
		}
		fmt.Println()
	}
	// fmt.Println()
	fmt.Println("all integers", allIntegers)
	// they should all be 2
	for k, v := range allIntegers {
		if v != 2 {
			fmt.Println("dumpIsActive allIntegers not all 2", k, v)
			// atw put this back
			t.Error("dumpIsActive allIntegers not all 2", k, v)
		}
	}
	fmt.Println()
}

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

	testStatesAllMatch(t, state)

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
	testStatesAllMatch(t, state)

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

func TestLocalBasic4minus1(t *testing.T) {

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

	testStatesAllMatch(t, state)

	// load something
	if !isLocalRunning {
		ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_load.sql")
	}

	time.Sleep(5 * time.Second)
	// query
	{
		// select * from customers_qa where isActive = false;@6
		got := ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_body_bug.sql")

		for _, child := range got.FailedChildren {
			fmt.Println("child failed", child.Statement)
		}

		assert.EqualValues(t, got.ExpectedRowcount, got.ActualRowCount)
		assert.EqualValues(t, 0, len(got.FailedChildren))

		fmt.Println("TestLocalBasic4minus1 4 nodes ", got.ExpectedRowcount, got.ActualRowCount)
	}

	// check the data
	time.Sleep(15 * time.Second)
	dumpField(t, state, vectors) // see the mapping

	fmt.Println("---- before removing node 4 ----")

	localConsulAddress := "127.0.0.1" // we have to use the mapping when we're outside the container
	consulClient, err := api.NewClient(&api.Config{Address: localConsulAddress + ":8500"})
	check(err)
	err = shared.SetClusterSizeTarget(consulClient, 3)
	check(err)

	// now delete a node
	state.nodes[3].Stop <- true
	state.nodes = state.nodes[:3]

	err = shared.SetClusterSizeTarget(consulClient, 3)
	check(err)

	// wait for sync
	// WaitForStatusGreenLocal()
	testStatesAllMatch(t, state)

	time.Sleep(15 * time.Second) // let batchProcessLoop shard count print out

	fmt.Println("---- after removing 4 ----")
	// check the data
	dumpField(t, state, vectors)
	fmt.Println("----")

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
