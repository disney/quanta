package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/disney/quanta/qlbridge/expr"
	"github.com/disney/quanta/qlbridge/expr/builtins"
	_ "github.com/disney/quanta/qlbridge/qlbdriver"
	"github.com/disney/quanta/qlbridge/schema"
	"github.com/hashicorp/consul/api"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/disney/quanta/core"
	"github.com/disney/quanta/custom/functions"
	proxy "github.com/disney/quanta/quanta-proxy-lib"
	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/sink"
	"github.com/disney/quanta/source"
)

// Exit Codes
const (
	Success         = 0
	InvalidHostPort = 100
)

var (
	logging       *string
	environment   *string
	proxyHostPort *string
)

func main() {

	app := kingpin.New("quanta-proxy", "MySQL Proxy adapter to Quanta").DefaultEnvars()
	app.Version("Version: " + proxy.Version + "\nBuild: " + proxy.Build)

	logging = app.Flag("log-level", "Logging level [ERROR, WARN, INFO, DEBUG]").Default("WARN").String()
	environment = app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	proxyHostPort = app.Flag("proxy-host-port", "Host:port mapping of MySQL Proxy server").Default("0.0.0.0:4000").String()
	quantaPortP := app.Flag("quanta-port", "Port number for Quanta service").Default("4000").Int() // port of a node, not of us. This is weird.
	publicKeyURL := app.Arg("public-key-url", "URL for JWT public key.").String()
	region := app.Arg("region", "AWS region for cloudwatch metrics").Default("us-east-1").String()
	// tokenservicePort := app.Arg("tokenservice-port", "Token exchance service port").Default("4001").Int()
	userKey := app.Flag("user-key", "Key used to get user id from JWT claims").Default("username").String()
	// unused username = app.Flag("username", "User account name for MySQL DB").Default("root").String()
	// unused password = app.Flag("password", "Password for account for MySQL DB (just press enter for now when logging in on mysql console)").Default("").String()
	consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()
	poolSize := app.Flag("session-pool-size", "Session pool size").Int()

	kingpin.MustParse(app.Parse(os.Args[1:]))

	proxy.QuantaPort = *quantaPortP
	proxy.Region = *region

	proxy.SetupCounters()
	proxy.Init()

	if strings.ToUpper(*logging) == "DEBUG" || strings.ToUpper(*logging) == "TRACE" {
		if strings.ToUpper(*logging) == "TRACE" {
			expr.Trace = true
		}
		u.SetupLogging("debug")
	} else {
		shared.InitLogging(*logging, *environment, "Proxy", proxy.Version, "Quanta")
	}

	go func() {
		// Initialize Prometheus metrics endpoint.
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()

	proxy.ConsulAddr = *consul
	log.Printf("Connecting to Consul at: [%s] ...\n", proxy.ConsulAddr)
	consulConfig := &api.Config{Address: proxy.ConsulAddr}
	errx := shared.RegisterSchemaChangeListener(consulConfig, proxy.SchemaChangeListener)
	if errx != nil {
		u.Error(errx)
		os.Exit(1)
	}

	if publicKeyURL != nil && len(*publicKeyURL) != 0 {
		proxy.PublicKeySet = make([]*jwk.Set, 0)
		urls := strings.Split(*publicKeyURL, ",")
		for _, url := range urls {
			log.Printf("Retrieving JWT public key from [%s]", url)
			keySet, err := jwk.Fetch(context.Background(), url)
			if err != nil {
				u.Error(err)
				os.Exit(1)
			}
			proxy.PublicKeySet = append(proxy.PublicKeySet, &keySet)
		}
	}
	proxy.UserClaimsKey = *userKey
	// Start the token exchange service TODO: repair or delete
	// log.Printf("Starting the token exchange service on port %d", *tokenservicePort)
	// authProvider = NewAuthProvider() // this instance is global used by tokenservice
	// StartTokenService(*tokenservicePort, authProvider)

	// If the pool size is not configured then set it to the number of available CPUs
	proxy.SessionPoolSize = *poolSize
	if proxy.SessionPoolSize == 0 {
		proxy.SessionPoolSize = runtime.NumCPU()
		log.Printf("Session Pool Size not set, defaulting to number of available CPUs = %d", proxy.SessionPoolSize)
	} else {
		log.Printf("Session Pool Size = %d", proxy.SessionPoolSize)
	}

	// load all of our built-in functions
	builtins.LoadAllBuiltins()
	sink.LoadAll()      // Register output sinks
	functions.LoadAll() // Custom functions

	tableCache := core.NewTableCacheStruct() // is this right?

	var err error
	proxy.Src, err = source.NewQuantaSource(tableCache, "", proxy.ConsulAddr, proxy.QuantaPort, proxy.SessionPoolSize)
	if err != nil {
		u.Error(err)
	}
	schema.RegisterSourceAsSchema("quanta", proxy.Src)

	// TODO:  we should ask consul if the nodes are up and then wait for a short while.

	// Start metrics publisher
	var ticker *time.Ticker
	ticker = proxy.MetricsTicker(proxy.Src)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			u.Warn("Interrupted,  shutting down ...")
			ticker.Stop()
			proxy.Src.Close()
			os.Exit(0)
		}
	}()

	// //conn := getClientConnection(consulConfig, ctx.Port)
	// 	consulClient, err := api.NewClient(&api.Config{Address: consulAddr})

	// sharedKV := shared.NewKVStore(conn)

	// ctx, err := rbac.NewAuthContext(sharedKV, "USER001", true)
	// check(err)
	// err = ctx.GrantRole(rbac.DomainUser, "USER001", "quanta", true)
	// check(err)

	// ctx, err = rbac.NewAuthContext(sharedKV, "MOLIG004", true)
	// check(err)
	// err = ctx.GrantRole(rbac.DomainUser, "MOLIG004", "quanta", true)
	// check(err)

	// Start server endpoint
	l, err := net.Listen("tcp", *proxyHostPort)
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

func check(err error) {
	if err != nil {
		fmt.Println("check err", err)
		panic(err.Error())
	}
}
