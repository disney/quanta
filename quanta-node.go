//
// Data Node launcher.
//
package main

import (
	"github.com/disney/quanta/server"
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

	kingpin.MustParse(app.Parse(os.Args[1:]))

	m, err := server.NewEndPoint(*dataDir)
	if err != nil {
		log.Printf("[node: Cannot initialize endpoint config: error: %s", err)
	}
	m.BindAddr = *bindAddr
	m.Port = uint(*port)
	_ = *tls
	_ = *certFile
	_ = *keyFile

	kvStore, err2 := server.NewKVStore(m)
	if err2 != nil {
		log.Printf("[node: Cannot create kv store config: error: %s", err2)
	}

	err3 := kvStore.Init()
	if err3 != nil {
		log.Printf("[node: Cannot initialized kv store error: %s", err3)
	}

	search, err4 := server.NewStringSearch(m)
	if err4 != nil {
		log.Printf("[node: Cannot initialize search config: error: %s", err4)
	}

	start := time.Now()
	bitmapIndex := server.NewBitmapIndex(m, uint(*expireDays))
	bitmapIndex.Init()
	elapsed := time.Since(start)
	log.Printf("Bitmap data server initialized in %v.", elapsed)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for range c {
			log.Printf("Interrupt signal received.  Starting Shutdown...")
			kvStore.Shutdown()
			search.Shutdown()
			bitmapIndex.Shutdown()
			time.Sleep(5)
			os.Exit(0)
		}
	}()

	node, err := server.Join("quanta", m)
	if err != nil {
		log.Printf("[node: Cannot initialize endpoint config: error: %s", err)
	}

	<-node.Stop
	err = <-node.Err
	if err != nil {
		log.Printf("[node: Cannot initialize endpoint config: error: %s", err)
	}
}
