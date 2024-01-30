package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/disney/quanta/test"

	"github.com/disney/quanta/shared"
)

// Make sure consul is running in localhost
// run:  consul agent -dev

// Run this and a cluster should come up and be available on port 4000

func main() {
	shared.SetUTCdefault()

	state := test.Ensure_cluster(3)

	// Wait for Ctrl+C to exit
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Ctrl+C pressed in Terminal")
		state.Release()
		os.Exit(0)
	}()

	for {
		time.Sleep(10 * time.Second)
	}

}
