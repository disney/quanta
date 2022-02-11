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
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
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

	err = m.Connect(consulClient)
	if err != nil {
		u.Errorf("[node: Cannot establish peer connections: error: %s", err)
	}

	kvStore, err2 := server.NewKVStore(m)
	if err2 != nil {
		u.Errorf("[node: Cannot create kv store config: error: %s", err2)
	}

	err3 := kvStore.Init()
	if err3 != nil {
		u.Errorf("[node: Cannot initialized kv store error: %s", err3)
	}
	m.AddNodeService(kvStore)

	search, err4 := server.NewStringSearch(m)
	if err4 != nil {
		u.Errorf("[node: Cannot initialize search config: error: %s", err4)
	}
	m.AddNodeService(search)

	start := time.Now()
	bitmapIndex := server.NewBitmapIndex(m, int(*expireDays))
	bitmapIndex.Init()
	elapsed := time.Since(start)
	log.Printf("Bitmap data server initialized in %v.", elapsed)
	m.AddNodeService(bitmapIndex)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for range c {
			u.Errorf("Interrupt signal received.  Starting Shutdown...")
			m.Leave()
			time.Sleep(5)
			os.Exit(0)
		}
	}()

	err = m.Join("quanta")
	if err != nil {
		u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
	}

	<-m.Stop
	err = <-m.Err
	if err != nil {
		u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
	}
}
