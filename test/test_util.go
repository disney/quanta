package test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	admin "github.com/disney/quanta/quanta-admin-lib"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
)

// testStatesAllMatch - Wait for the proxy, the admin, and all the nodes report the same cluster state.
func testStatesAllMatch(t *testing.T, state *ClusterLocalState, comment string) {
	fmt.Println("tsam testStatesAllMatch " + comment)

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
		fmt.Println("tsam proxy state", clusterState, "active", active, "target", target)
		_ = active
		_ = target

		adminstate := GetStatusViaAdminLocal() // should return the same thing
		fmt.Println("tsam GetStatusViaAdminLocal", adminstate)
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
				fmt.Println("tsam states MISMATCH", states)
				time.Sleep(500 * time.Millisecond)
				matched = false
			}
		}
		if time.Since(start) > 60*time.Second {
			fmt.Println("tsam timeout")
			if states[1] == "bad" {
				GetStatusViaAdminLocal() // do it again for debug
			}
			t.Error("tsam timeout", states)
			break
		}
		if clusterState == shared.Green && matched {
			break
		}
	}
}

type Mappings []string

func dumpField(t *testing.T, state *ClusterLocalState, vectors []string) {
	ConsulAddr := "127.0.0.1:8500"
	consulClient, err := api.NewClient(&api.Config{Address: ConsulAddr})
	check(err)
	clientConn := shared.NewDefaultConnection("dumpField")
	clientConn.ServicePort = 4000
	clientConn.Quorum = 3
	err = clientConn.Connect(consulClient)
	check(err)
	defer func() {
		clientConn.Disconnect()
	}()
	dumpField_(t, state, vectors, clientConn)
}

// dumpIsActive return shard info for "customers_qa/isActive/1/1970-01-01T00"
// or "customers_qa/zip/1970-01-01T00"
// or whatever's in vectors
func dumpField_(t *testing.T, state *ClusterLocalState, vectors []string, clientConn *shared.Conn) {

	if clientConn.HashTable == nil {
		return
	}

	nodes, err := clientConn.SelectNodes("dummy", shared.Admin) // shared.AllActive)
	if err != nil {
		t.Error("dumpIsActive SelectNodes not ready", nodes, err)
		return
	}

	fieldMatch := strings.Split(vectors[0], "/")[1]

	fmt.Println("bitmap dump of ", fieldMatch, vectors[0])

	// testStatesAllMatch(t, state, "bitmap dump of "+fieldMatch)

	//mappings0 := make([]string, 0)
	//mappings1 := make([]string, 0)

	mappings := make([]Mappings, len(vectors))
	for i := range vectors {
		mappings[i] = make([]string, 0)
	}

	conn := clientConn // Src.GetConnection() // .proxyControl.Src.GetConnection()

	for i, v := range vectors {
		nodes, err := conn.SelectNodes(v, shared.Admin) // shared.AllActive)
		check(err)
		m := mappings[i]
		mappings[i] = append(m, fmt.Sprintf("%v", nodes))
		fmt.Println("proxy hash nodes", i, nodes)
	}

	for n, node := range state.nodes {
		for i, v := range vectors {
			if node.Conn.HashTable == nil {
				continue
			}
			nodes, err := node.Conn.SelectNodes(v, shared.Admin)
			if err != nil {
				continue
			}
			fmt.Println("node hash nodes", i, n, nodes)
			m := mappings[i]
			mappings[i] = append(m, fmt.Sprintf("%v", nodes))

		}
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
	}

	allIntegers := make(map[uint64]uint64, 0)

	for i, node := range state.nodes {
		fmt.Println("##### node ", i, node.GetNodeID())
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
	dirty := false
	for k, v := range allIntegers {
		if v != 2 {
			fmt.Println("dumpIsActive allIntegers not all 2", k, v)
			dirty = true
			t.Error("dumpIsActive allIntegers not all 2", k, v)
		}
	}
	if dirty {
		fmt.Println("bad mapping") // add breakpoint here
	}
	fmt.Println()
}
