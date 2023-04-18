package main

import (
	_ "net/http/pprof"
	"time"

	"github.com/disney/quanta/test"

	"github.com/disney/quanta/server"
)

func main() {

	// test.StartNodes(0, 1)
	// test.StartNodes(1, 1)
	// test.StartNodes(2, 1)

	m0, _ := test.StartNodes(0, 1) // from localCluster/local-cluster-main.go
	m1, _ := test.StartNodes(1, 1)
	m2, _ := test.StartNodes(2, 1)
	defer func() {
		m0.Stop <- true
		m1.Stop <- true
		m2.Stop <- true
	}()
	// this is too slow
	//fmt.Println("Waiting for nodes to start...", m2.State)
	for m0.State != server.Active || m1.State != server.Active || m2.State != server.Active {
		time.Sleep(100 * time.Millisecond)
	}
	test.StartProxy(1)

	for {
		time.Sleep(10 * time.Second)
	}

}
