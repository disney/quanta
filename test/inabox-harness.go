package test

import (
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/schema"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/custom/functions"
	proxy "github.com/disney/quanta/quanta-proxy-lib"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/sink"
	"github.com/disney/quanta/source"
	"github.com/hashicorp/consul/api"
)

func StartNodes(nodeStart int) (*server.Node, error) {

	Version := "v0.0.1"
	Build := "2006-01-01"

	environment := "DEV"
	logLevel := "DEBUG"

	shared.InitLogging(logLevel, environment, "Data-Node", Version, "Quanta")

	index := nodeStart
	{
		hashKey := "quanta-node-" + strconv.Itoa(index)
		dataDir := "../test/localClusterData/" + hashKey + "/data"
		bindAddr := "127.0.0.1"
		port := 4010 + index

		consul := bindAddr + ":8500"

		memLimit := 0

		// Create /bitmap data directory
		fmt.Printf("Creating bitmap data directory: %s", dataDir+"/bitmap")
		if _, err := os.Stat(dataDir + "/bitmap"); err != nil {
			err = os.MkdirAll(dataDir+"/bitmap", 0777)
			if err != nil {
				u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
			}
		}

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
		m.ServiceName = "quanta" // not hashKey. Do we need this? (atw)
		m.IsLocalCluster = true

		kvStore := server.NewKVStore(m)
		m.AddNodeService(kvStore)

		search := server.NewStringSearch(m)
		m.AddNodeService(search)

		bitmapIndex := server.NewBitmapIndex(m, int(memLimit))
		m.AddNodeService(bitmapIndex)

		// load the table schema from the file system manually here

		table := "cities"
		shared.LoadSchema("../test/testdata/config", table, consulClient)
		// ? m.TableCache.TableCache[table] = t

		table = "cityzip"
		shared.LoadSchema("../test/testdata/config", table, consulClient)

		table = "dmltest"
		shared.LoadSchema("../test/testdata/config", table, consulClient)

		// Start listening endpoint
		m.Start()

		start := time.Now()
		err = m.InitServices()
		elapsed := time.Since(start)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Node initialized in %v.", elapsed)

		go func() {
			joinName := "quanta"   // this is the name for a cluster of nodes was "quanta-node"
			err = m.Join(joinName) // this does not return
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
}

type LocalProxyControl struct {
	Stop chan bool
}

func StartProxy(count int, testConfigPath string) *LocalProxyControl {

	localProxy := &LocalProxyControl{}

	fmt.Println("Starting proxy")

	// for index := 0; index < count; index++ { // TODO: more than one proxy
	index := 0

	proxy.SetupCounters()
	proxy.Init()

	logging := "DEBUG"
	environment := "DEV"
	Version := "1.0.0"
	proxy.ConsulAddr = "127.0.0.1:8500"
	// cognito url for token service publicKeyURL := "" // unused
	proxy.QuantaPort = 4010
	proxyHostPort := 4000 + index

	// region := "us-east-1"

	if strings.ToUpper(logging) == "DEBUG" || strings.ToUpper(logging) == "TRACE" {
		if strings.ToUpper(logging) == "TRACE" {
			expr.Trace = true
		}
		u.SetupLogging("debug")
	} else {
		shared.InitLogging(logging, environment, "Proxy", Version, "Quanta")
	}

	// go func() { // FIXME: do this later
	// 	// Initialize Prometheus metrics endpoint.
	// 	http.Handle("/metrics", promhttp.Handler())
	// 	http.ListenAndServe(":2112", nil)
	// }()

	log.Printf("Connecting to Consul at: [%s] ...\n", proxy.ConsulAddr)
	consulConfig := &api.Config{Address: proxy.ConsulAddr}
	errx := shared.RegisterSchemaChangeListener(consulConfig, proxy.SchemaChangeListener)
	if errx != nil {
		u.Error(errx)
		os.Exit(1)
	}

	fmt.Println("Proxy RegisterSchemaChangeListener done")

	poolSize := 3

	// If the pool size is not configured then set it to the number of available CPUs
	// this is weird atw
	sessionPoolSize := poolSize
	if sessionPoolSize == 0 {
		sessionPoolSize = runtime.NumCPU()
		log.Printf("Session Pool Size not set, defaulting to number of available CPUs = %d", sessionPoolSize)
	} else {
		log.Printf("Session Pool Size = %d", sessionPoolSize)
	}

	// Match 2 or more whitespace chars inside string
	reWhitespace := regexp.MustCompile(`[\s\p{Zs}]{2,}`)
	_ = reWhitespace

	// load all of our built-in functions
	builtins.LoadAllBuiltins()
	sink.LoadAll()      // Register output sinks
	functions.LoadAll() // Custom functions

	// start cloud watch or prometheus metrics?

	fmt.Println("Proxy before NewQuantaSource")

	// Construct Quanta source
	tableCache := core.NewTableCacheStruct() // is this right? delete this?

	// configDir := "../test/testdata" // gets: ../test/testdata/config/schema.yaml
	// FIXME: empty configDir panics
	configDir := testConfigPath
	var err error                                                                                                       // this fails when run from test?
	proxy.Src, err = source.NewQuantaSource(tableCache, configDir, proxy.ConsulAddr, proxy.QuantaPort, sessionPoolSize) // do we really want this here?
	if err != nil {
		u.Error(err)
	}
	fmt.Println("Proxy after NewQuantaSource")

	schema.RegisterSourceAsSchema("quanta", proxy.Src)

	fmt.Println("Proxy starting to listen. ")

	// Start server endpoint
	portStr := strconv.FormatInt(int64(proxyHostPort), 10)
	l, err := net.Listen("tcp", "0.0.0.0:"+portStr)
	if err != nil {
		panic(err.Error())
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				u.Errorf(err.Error())
				return
			}
			go proxy.OnConn(conn)
		}
	}()

	go func(localProxy *LocalProxyControl) {

		<-localProxy.Stop
		fmt.Println("Stopping proxy")
		// and ??

	}(localProxy)

	return localProxy
}
