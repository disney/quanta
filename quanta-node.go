// Data Node launchor.
package main

import (
	"fmt"

	"net"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	u "github.com/araddon/gou"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	// Version number (i.e. 0.8.0)
	Version string
	// Build date
	Build string
)

func main() {

	fmt.Println("hello from Node")
	fmt.Println("IP address", GetOutboundIP())

	app := kingpin.New(os.Args[0], "Quanta server node.").DefaultEnvars()
	app.Version("Version: " + Version + "\nBuild: " + Build)
	hashKey := app.Arg("hash-key", "Consistent hash key for node.").String()
	dataDir := app.Arg("data-dir", "Root directory for data files.").Default("/home/ec2-user/data").String()
	bindAddr := app.Arg("bind", "Bind address for this endpoint.").Default("0.0.0.0").String()
	port := app.Arg("port", "Port for this endpoint.").Default("4000").Int32()
	memLimit := app.Flag("mem-limit-mb", "Data partitions will expire after MB limit is exceeded (disabled if not specified).").Default("0").Int32()
	tls := app.Flag("tls", "Connection uses TLS if true.").Bool()
	certFile := app.Flag("cert-file", "TLS cert file path.").String()
	keyFile := app.Flag("key-file", "TLS key file path.").String()
	consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()
	environment := app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	logLevel := app.Flag("log-level", "Log Level [ERROR, WARN, INFO, DEBUG]").Default("WARN").String()
	pprof := app.Flag("pprof", "Start the pprof server").Default("false").String()

	kingpin.MustParse(app.Parse(os.Args[1:]))

	shared.InitLogging(*logLevel, *environment, "Data-Node", Version, "Quanta")

	if *bindAddr == "0.0.0.0" { // if there's no bind address given then find our ip address
		myaddr := GetOutboundIP().String()
		*bindAddr = myaddr
		fmt.Println("bindAddr", *bindAddr)
	}

	fmt.Println("pprof is ", *pprof)
	shared.StartPprofAndPromListener(*pprof)

	u.Warnf("Node identifier '%s'", *hashKey)
	u.Infof("Connecting to Consul at: [%s] ...\n", *consul)
	consulClient, err := api.NewClient(&api.Config{Address: *consul})
	if err != nil {
		// Is the consul agent running?
		u.Errorf("node: Cannot initialize endpoint config: error: %s", err)
	}

	_ = *tls
	_ = *certFile
	_ = *keyFile

	fmt.Println("before server.NewNode")
	m, err := server.NewNode(fmt.Sprintf("%v:%v", Version, Build), int(*port), *bindAddr, *dataDir, *hashKey, consulClient)
	if err != nil {
		u.Errorf("[node: Cannot initialize node config: error: %s", err)
	}
	fmt.Println("after server.NewNode")

	kvStore := server.NewKVStore(m)
	m.AddNodeService(kvStore)

	search := server.NewStringSearch(m)
	m.AddNodeService(search)

	bitmapIndex := server.NewBitmapIndex(m, int(*memLimit))
	m.AddNodeService(bitmapIndex)

	fmt.Println("after AddNodeService ...")

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
			time.Sleep(5 * time.Second)
			os.Exit(0)
		}
	}()

	fmt.Println("after m.InitServices")

	start := time.Now()
	err = m.InitServices()
	elapsed := time.Since(start)
	if err != nil {
		u.Error(err)
	}
	u.Debugf("Data node initialized in %v.", elapsed)

	fmt.Println("before m.Join")

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

// Get preferred outbound ip of this machine
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		u.Log(u.FATAL, err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP
}
