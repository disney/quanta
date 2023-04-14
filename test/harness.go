// Package test - This code creates an in-memory "stack" and loads test data.
package test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/disney/quanta/server"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"

	u "github.com/araddon/gou"
)

// Setup - Initialize test harness
func Setup() (*server.Node, error) {

	os.Mkdir("./testdata/bitmap", 0755)
	// Enable in memory instance
	node, err := server.NewNode("TEST", 0, "", "./testdata", "test", nil)
	if err != nil {
		return nil, err
	}
	kvStore := server.NewKVStore(node)
	node.AddNodeService(kvStore)
	search := server.NewStringSearch(node)
	node.AddNodeService(search)
	bitmapIndex := server.NewBitmapIndex(node, 0)
	node.AddNodeService(bitmapIndex)
	go func() {
		node.Start()
	}()
	err = node.InitServices()
	if err != nil {
		return nil, err
	}
	return node, nil
}

// RemoveContents - Remove local data files.
func RemoveContents(path string) error {
	files, err := filepath.Glob(path)
	if err != nil {
		return err
	}
	for _, file := range files {
		err = os.RemoveAll(file)
		if err != nil {
			return err
		}
	}
	return nil
}

func StartNodes(nodeStart int, nodeCount int) (*server.Node, error) {

	Version := "v0.0.1"
	Build := "2006-01-01"

	environment := "DEV"
	logLevel := "DEBUG"

	shared.InitLogging(logLevel, environment, "Data-Node", Version, "Quanta")

	index := nodeStart
	// for index = nodeStart; index < nodeStart+nodeCount; index++
	{
		hashKey := "quanta-node-" + strconv.Itoa(index)
		dataDir := "localClusterData/" + hashKey + "/data"
		bindAddr := "127.0.0.1"
		port := 4000 + index

		consul := "127.0.0.1:8500"

		memLimit := 0

		// Create /bitmap data directory
		fmt.Printf("Creating bitmap data directory: %s", dataDir+"/bitmap")
		if _, err := os.Stat(dataDir + "/bitmap"); err != nil {
			err = os.MkdirAll(dataDir+"/bitmap", 0777)
			if err != nil {
				u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
			}
		}

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

		newNodeName := hashKey
		// newNodeName = "quanta"
		m, err := server.NewNode(fmt.Sprintf("%v:%v", Version, Build), int(port), bindAddr, dataDir, newNodeName, consulClient)
		if err != nil {
			u.Errorf("[node: Cannot initialize node config: error: %s", err)
		}
		m.ServiceName = "quanta-node" // not hashKey this doesn't work

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

		// not connected yet: shared.SetClusterSizeTarget(m.Consul, 3)

		go func() {
			// joinName := hashKey // "quanta-" + hashKey
			joinName := "quanta-node" // this is the name for a cluster of nodes
			err = m.Join(joinName)    // this does not return
			if err != nil {
				u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
			}
			fmt.Println("StartNodes returned from join")

			<-m.Stop
			select {
			case err = <-m.Err:
			default:
			}
			if err != nil {
				u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
			}
		}()
		return m, nil
	}
	// for {
	// 	time.Sleep(1 * time.Second)
	// }
}
