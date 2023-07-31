package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/disney/quanta/rbac"
	"github.com/disney/quanta/test"

	"github.com/disney/quanta/server"
	"github.com/disney/quanta/shared"
)

// Make sure consul is running in localhost
// run:  consul agent -dev

// Run this and a cluster should come up and be available on port 4000

func main() {

	m0, _ := test.StartNodes(0)
	m1, _ := test.StartNodes(1)
	m2, _ := test.StartNodes(2)
	defer func() {
		m0.Stop <- true
		m1.Stop <- true
		m2.Stop <- true
	}()

	shared.SetClusterSizeTarget(m0.Consul, 3) // why?

	// FIXME: this is too slow. Maybe don't poll for Node state in node? Use the node.Status api?
	// fmt.Println("Waiting for nodes to start...", m2.State)
	for m0.State != server.Active || m1.State != server.Active || m2.State != server.Active {
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("All nodes Active")

	conn := shared.NewDefaultConnection()
	err := conn.Connect(nil)
	check(err)
	defer conn.Disconnect()

	sharedKV := shared.NewKVStore(conn)

	ctx, err := rbac.NewAuthContext(sharedKV, "USER001", true)
	check(err)
	err = ctx.GrantRole(rbac.DomainUser, "USER001", "quanta", true)
	check(err)

	ctx, err = rbac.NewAuthContext(sharedKV, "MOLIG004", true)
	check(err)
	err = ctx.GrantRole(rbac.DomainUser, "MOLIG004", "quanta", true)
	check(err)

	localProxyControl := test.StartProxy(1, "../test/testdata/config") // "./testdata/config")
	_ = localProxyControl

	c := make(chan os.Signal, 1)
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

func check(err error) {
	if err != nil {
		fmt.Println("check err", err)
		panic(err.Error())
	}
}
