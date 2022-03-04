//
// Data Node launcher.
//
package main

import (
	"fmt"
	u "github.com/araddon/gou"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	// Version number (i.e. 0.8.0)
	Version string
	// Build date
	Build string
)

func main() {
	app := kingpin.New(os.Args[0], "Quanta server node.").DefaultEnvars()
	app.Version("Version: " + Version + "\nBuild: " + Build)
	dataDir := app.Arg("data-dir", "Root directory for data files").Default("/home/ec2-user/data").String()
	bindAddr := app.Arg("bind", "Bind address for this endpoint.").Default("0.0.0.0").String()
	port := app.Arg("port", "Port for this endpoint.").Default("4000").Int32()
	expireDays := app.Flag("expire-days", "Data will expire after n days (disabled if not specified).").Default("0").Int32()
	tls := app.Flag("tls", "Connection uses TLS if true.").Bool()
	certFile := app.Flag("cert-file", "TLS cert file path.").String()
	keyFile := app.Flag("key-file", "TLS key file path.").String()
	consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()
	environment := app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	logLevel := app.Flag("log-level", "Log Level [ERROR, WARN, INFO, DEBUG]").Default("WARN").String()

	kingpin.MustParse(app.Parse(os.Args[1:]))

	shared.InitLogging(*logLevel, *environment, "Data-Node", Version, "Quanta")

	go func() {
		// Initialize Prometheus metrics endpoint.
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()

	u.Infof("Connecting to Consul at: [%s] ...\n", *consul)
	consulClient, err := api.NewClient(&api.Config{Address: *consul})
	if err != nil {
		u.Errorf("Is the consul agent running?")
		log.Fatalf("[node: Cannot initialize endpoint config: error: %s", err)
	}

	_ = *tls
	_ = *certFile
	_ = *keyFile

	m, err := server.NewNode(fmt.Sprintf("%v:%v", Version, Build), int(*port), *bindAddr, *dataDir, consulClient)
	if err != nil {
		u.Errorf("[node: Cannot initialize node config: error: %s", err)
	}

	kvStore := server.NewKVStore(m)
	m.AddNodeService(kvStore)

	search := server.NewStringSearch(m)
	m.AddNodeService(search)

	bitmapIndex := server.NewBitmapIndex(m, int(*expireDays))
	m.AddNodeService(bitmapIndex)

	// Start listening endpoint
	m.Start()

	// Start metrics publisher thread
	ticker := metricsTicker(m)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for range c {
			u.Errorf("Interrupt signal received.  Starting Shutdown...")
			ticker.Stop()
			m.Leave()
			time.Sleep(5)
			os.Exit(0)
		}
	}()

	start := time.Now()
	err = m.InitServices()
	elapsed := time.Since(start)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Data node initialized in %v.", elapsed)

	err = m.Join("quanta")
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
}

func metricsTicker(node *server.Node) *time.Ticker {

	t := time.NewTicker(time.Second * 10)
	start := time.Now()
	lastTime := time.Now()
	go func() {
		for range t.C {
			duration := time.Since(start)
			lastTime = node.PublishMetrics(duration, lastTime)
		}
	}()
	return t
}
