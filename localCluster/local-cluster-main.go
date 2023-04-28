package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/disney/quanta/test"

	"github.com/disney/quanta/server"
)

func main() {

	// test.StartNodes(0, 1)
	// test.StartNodes(1, 1)
	// test.StartNodes(2, 1)

	m0, _ := test.StartNodes(0) // from localCluster/local-cluster-main.go
	m1, _ := test.StartNodes(1)
	m2, _ := test.StartNodes(2)
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

	fmt.Println("All nodes Active")

	test.StartProxy(1, "") // "./testdata/config")

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Ctrl+C pressed in Terminal")
		os.Exit(0)
	}()

	for {
		time.Sleep(10 * time.Second)
	}

}
