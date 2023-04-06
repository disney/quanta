package main

import (
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"strconv"
	"time"

	u "github.com/araddon/gou"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
)

func main() {

	StartNodes()

}

func StartNodes() {

	index := 0
	for index = 0; index < 3; index++ {

		Version := "v0.0.1"
		Build := "2006-01-01"

		hashKey := "node-" + strconv.Itoa(index)
		dataDir := "localClusterData/" + hashKey + "/data"
		bindAddr := "127.0.0.1"
		port := 4000 + index

		consul := "127.0.0.1:8500"
		environment := "DEV"
		logLevel := "DEBUG"

		memLimit := 0

		shared.InitLogging(logLevel, environment, "Data-Node", Version, "Quanta")

		// go func() { // if we run this it has to be just once
		// 	// Initialize Prometheus metrics endpoint.
		// 	http.Handle("/metrics", promhttp.Handler())
		// 	http.ListenAndServe(":2112", nil)
		// }()

		u.Infof("Node identifier '%s'", hashKey)
		u.Infof("Connecting to Consul at: [%s] ...\n", consul)
		consulClient, err := api.NewClient(&api.Config{Address: consul})
		if err != nil {
			u.Errorf("Is the consul agent running?")
			log.Fatalf("[node: Cannot initialize endpoint config: error: %s", err)
		}

		// Create /bitmap data directory
		fmt.Printf("Creating bitmap data directory: %s", dataDir+"/bitmap")
		if _, err := os.Stat(dataDir + "/bitmap"); err != nil {
			err = os.Mkdir(dataDir+"/bitmap", 0777)
			if err != nil {
				u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
			}
		}

		m, err := server.NewNode(fmt.Sprintf("%v:%v", Version, Build), int(port), bindAddr, dataDir, hashKey, consulClient)
		if err != nil {
			u.Errorf("[node: Cannot initialize node config: error: %s", err)
		}

		kvStore := server.NewKVStore(m)
		m.AddNodeService(kvStore)

		search := server.NewStringSearch(m)
		m.AddNodeService(search)

		bitmapIndex := server.NewBitmapIndex(m, int(memLimit))
		m.AddNodeService(bitmapIndex)

		// Start listening endpoint
		m.Start()

		// Start metrics publisher thread
		// todo: ticker := metricsTicker(m)

		// elsewhere
		// c := make(chan os.Signal, 1)
		// signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
		// go func() {
		// 	for range c {
		// 		u.Errorf("Interrupt signal received.  Starting Shutdown...")
		// 		ticker.Stop()
		// 		m.Leave()
		// 		time.Sleep(5)
		// 		os.Exit(0)
		// 	}
		// }()

		start := time.Now()
		err = m.InitServices()
		elapsed := time.Since(start)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Node initialized in %v.", elapsed)

		go func() {
			err = m.Join("quanta-" + hashKey) // this does not return
			if err != nil {
				u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
			}

			<-m.Stop
			select {
			case err = <-m.Err:
			default:
			}
			if err != nil {
				u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
			}
		}()
	}
	for {
		time.Sleep(1 * time.Second)
	}
}
