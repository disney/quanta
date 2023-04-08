package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/disney/quanta/custom/functions"
	proxy "github.com/disney/quanta/quanta-proxy-lib"

	"github.com/disney/quanta/server"
	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/sink"
	"github.com/disney/quanta/source"

	"github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	StartNodes(3)

	StartProxy(1)

}

func StartProxy(index int) {
	// app := kingpin.New("quanta-proxy", "MySQL Proxy adapter to Quanta").DefaultEnvars()
	// app.Version("Version: " + Version + "\nBuild: " + Build)

	// logging = app.Flag("log-level", "Logging level [ERROR, WARN, INFO, DEBUG]").Default("WARN").String()
	// environment = app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	// proxyHostPort = app.Flag("proxy-host-port", "Host:port mapping of MySQL Proxy server").Default("0.0.0.0:4000").String()
	// quantaPort = app.Flag("quanta-port", "Port number for Quanta service").Default("4000").Int()
	// publicKeyURL := app.Arg("public-key-url", "URL for JWT public key.").String()
	// region := app.Arg("region", "AWS region for cloudwatch metrics").Default("us-east-1").String()
	// tokenservicePort := app.Arg("tokenservice-port", "Token exchance service port").Default("4001").Int()
	// userKey := app.Flag("user-key", "Key used to get user id from JWT claims").Default("username").String()
	// // unused username = app.Flag("username", "User account name for MySQL DB").Default("root").String()
	// // unused password = app.Flag("password", "Password for account for MySQL DB (just press enter for now when logging in on mysql console)").Default("").String()
	// consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()
	// poolSize := app.Flag("session-pool-size", "Session pool size").Int()

	// kingpin.MustParse(app.Parse(os.Args[1:]))

	proxy.SetupCounters()
	proxy.Init()

	logging := "DEBUG"
	environment := "DEV"
	Version := "1.0.0"
	proxy.ConsulAddr = "127.0.0.1:8500"
	// cognito url for token service publicKeyURL := "" // unused
	proxy.QuantaPort = 4000
	proxyHostPort := 4040 + index

	region := "us-east-1"

	if strings.ToUpper(logging) == "DEBUG" || strings.ToUpper(logging) == "TRACE" {
		if strings.ToUpper(logging) == "TRACE" {
			expr.Trace = true
		}
		u.SetupLogging("debug")
	} else {
		shared.InitLogging(logging, environment, "Proxy", Version, "Quanta")
	}

	go func() {
		// Initialize Prometheus metrics endpoint.
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()

	log.Printf("Connecting to Consul at: [%s] ...\n", proxy.ConsulAddr)
	consulConfig := &api.Config{Address: proxy.ConsulAddr}
	errx := shared.RegisterSchemaChangeListener(consulConfig, proxy.SchemaChangeListener)
	if errx != nil {
		u.Error(errx)
		os.Exit(1)
	}

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

	sess, errx := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if errx != nil {
		u.Error(errx)
		os.Exit(1)
	}
	_ = sess
	// metrics = cloudwatch.New(sess)

	// Construct Quanta source
	for i := 0; i < sessionPoolSize; i++ {
		src, err := source.NewQuantaSource("", proxy.ConsulAddr, proxy.QuantaPort+1, sessionPoolSize)
		if err != nil {
			u.Error(err)
		}
		schema.RegisterSourceAsSchema("quanta-"+"node-"+strconv.Itoa(i), src)
	}

	// Start metrics publisher
	// var ticker *time.Ticker
	// ticker := metricsTicker(src)
	// c := make(chan os.Signal, 1)
	// signal.Notify(c, os.Interrupt)
	// go func() {
	// 	for range c {
	// 		u.Warn("Interrupted,  shutting down ...")
	// 		ticker.Stop()
	// 		src.Close()
	// 		os.Exit(0)
	// 	}
	// }()

	// queryCount = &Counter{}
	// updateCount = &Counter{}
	// insertCount = &Counter{}
	// deleteCount = &Counter{}
	// connectCount = &Counter{}
	// queryCountL = &Counter{}
	// updateCountL = &Counter{}
	// insertCountL = &Counter{}
	// deleteCountL = &Counter{}
	// connectCountL = &Counter{}
	// queryTime = &Counter{}
	// updateTime = &Counter{}
	// insertTime = &Counter{}
	// deleteTime = &Counter{}

	// Start server endpoint
	portStr := strconv.FormatInt(int64(proxyHostPort), 32)
	l, err := net.Listen("tcp", portStr)
	if err != nil {
		panic(err.Error())
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			u.Errorf(err.Error())
			return
		}
		go proxy.OnConn(conn)

	}
}

func StartNodes(nodeCount int) {

	index := 0
	for index = 0; index < nodeCount; index++ {

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
