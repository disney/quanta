package main

import (
	"context"
	//"expvar"
	"fmt"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/aws/aws-sdk-go/aws"
	_ "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/shared"
	"github.com/hamba/avro"
	"github.com/harlow/kinesis-consumer"
	"github.com/hashicorp/consul/api"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"
)

// Variables to identify the build
var (
	Version  string
	Build    string
	EPOCH, _ = time.ParseInLocation(time.RFC3339, "2000-01-01T00:00:00+00:00", time.UTC)
)

// Exit Codes
const (
	Success = 0
)

// Main strct defines command line arguments variables and various global meta-data associated with record loads.
type Main struct {
	Stream       string
	Region       string
	Index        string
	BufferSize   uint
	totalBytes   int64
	bytesLock    sync.RWMutex
	totalRecs    *Counter
	Port         int
	ConsulAddr   string
	ConsulClient *api.Client
	ShardCount   int
	lock         *api.Lock
	conns        []*core.Connection
	consumer     *consumer.Consumer
}

// NewMain allocates a new pointer to Main struct with empty record counter
func NewMain() *Main {
	m := &Main{
		totalRecs: &Counter{},
	}
	return m
}

func main() {

	app := kingpin.New(os.Args[0], "Quanta Kinesis data consumer").DefaultEnvars()
	app.Version("Version: " + Version + "\nBuild: " + Build)

	stream := app.Arg("stream", "Kinesis stream name.").Required().String()
	index := app.Arg("index", "Table name (root name if nested schema)").Required().String()
	region := app.Arg("region", "AWS region").Default("us-east-1").String()
	port := app.Arg("port", "Port number for service").Default("4000").Int32()
	bufSize := app.Flag("buf-size", "Buffer size").Default("1000000").Int32()
	environment := app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()

	core.InitLogging("WARN", *environment, "Kinesis-Consumer", Version, "Quanta")

    builtins.LoadAllBuiltins()

	kingpin.MustParse(app.Parse(os.Args[1:]))

	main := NewMain()
	main.Stream = *stream
	main.Region = *region
	main.Index = *index
	main.BufferSize = uint(*bufSize)
	main.Port = int(*port)
	main.ConsulAddr = *consul

	log.Printf("Kinesis stream %v.", main.Stream)
	log.Printf("Kinesis region %v.", main.Region)
	log.Printf("Index name %v.", main.Index)
	log.Printf("Buffer size %d.", main.BufferSize)
	log.Printf("Service port %d.", main.Port)
	log.Printf("Consul agent at [%s]\n", main.ConsulAddr)

	var err error

	if main.ShardCount, err = main.Init(); err != nil {
		log.Fatal(err)
	}

	msgChan := make(chan *consumer.Record, main.BufferSize)

	var ticker *time.Ticker
	ticker = main.printStats()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Printf("Interrupted,  Bytes processed: %s, Records: %v", core.Bytes(main.BytesProcessed()),
				main.totalRecs.Get())
			close(msgChan)
			//main.consumer.Close()
			ticker.Stop()
			os.Exit(0)
		}
	}()

	// Spin up workers
    workers := runtime.NumCPU()
    if workers > main.ShardCount {
		workers = main.ShardCount
	}
	log.Printf("Spinning up %d worker threads.  CPUs = %d, Shard count = %d.", workers, runtime.NumCPU(),
		main.ShardCount)
	main.conns = make([]*core.Connection, workers)
	for n := 0; n < workers; n++ {
		go func(i int) {
			var err error
			main.conns[i], err = core.OpenConnection("", main.Index, true,
				main.BufferSize, main.Port, main.ConsulClient)
			if err != nil {
				log.Fatalf("Error opening connection %v", err)
			}
			tbuf := main.conns[i].TableBuffers[main.Index]
			avroSchema := shared.ToAvroSchema(tbuf.Table.BasicTable)
			for msg := range msgChan {
				out := make(map[string]interface{})
				err = avro.Unmarshal(avroSchema, msg.Data, &out)
				if err != nil {
					log.Printf("ERROR %v", err)
					continue
				}
				err = main.conns[i].PutRow(main.Index, out, 0)
				if err != nil {
					log.Printf("ERROR in PutRow - %v", err)
					continue
				}
				main.conns[i].Flush()
				main.totalRecs.Add(1)
				main.AddBytes(len(msg.Data))
			}
			main.conns[i].CloseConnection()
		}(n)
	}

	// Main processing loop
	go func() {
		err = main.consumer.Scan(context.TODO(), func(r *consumer.Record) error {
			msgChan <- r
			return nil // continue scanning
		})

		if err != nil {
			log.Fatalf("scan error: %v", err)
		}
	}()
	<-c
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

// Init function initilizations loader.
// Establishes session with bitmap server and Kinesis
func (m *Main) Init() (int, error) {

	var err error

	m.ConsulClient, err = api.NewClient(&api.Config{Address: m.ConsulAddr})
	if err != nil {
		return 0, err
	}

	sess, errx := session.NewSession(&aws.Config{
		Region: aws.String(m.Region),
	})

	if errx != nil {
		return 0, errx
	}

	kc := kinesis.New(sess)
	streamName := aws.String(m.Stream)
	shout, err := kc.ListShards(&kinesis.ListShardsInput{StreamName: streamName})
	if err != nil {
		return 0, err
	}
    shardCount := len(shout.Shards)


	//var counter = expvar.NewMap("counters")

	m.consumer, err = consumer.New(
		m.Stream,
		consumer.WithClient(kc),
		//consumer.WithCounter(counter),
	)
	if err != nil {
		return 0, err
	}

	log.Printf("Created consumer. ")
	return shardCount, nil
}

// printStats outputs to Log current status of Kinesis consumer
// Includes data on processed: bytes, records, time duration in seconds, and rate of bytes per sec"
func (m *Main) printStats() *time.Ticker {
	t := time.NewTicker(time.Second * 10)
	start := time.Now()
	go func() {
		for range t.C {
			duration := time.Since(start)
			bytes := m.BytesProcessed()
			log.Printf("Bytes: %s, Records: %v, Duration: %v, Rate: %v/s", core.Bytes(bytes), m.totalRecs.Get(), duration, core.Bytes(float64(bytes)/duration.Seconds()))
			for i := 0; i < len(m.conns); i++ {
				m.conns[i].Flush()
			}
		}
	}()
	return t
}

// AddBytes provides thread safe processing to set the total bytes processed.
// Adds the bytes parameter to total bytes processed.
func (m *Main) AddBytes(n int) {
	m.bytesLock.Lock()
	m.totalBytes += int64(n)
	m.bytesLock.Unlock()
}

// BytesProcessed provides thread safe read of total bytes processed.
func (m *Main) BytesProcessed() (num int64) {
	m.bytesLock.Lock()
	num = m.totalBytes
	m.bytesLock.Unlock()
	return
}

// Counter - Generic counter with mutex (threading) support
type Counter struct {
	num  int64
	lock sync.Mutex
}

// Add function provides thread safe addition of counter value based on input parameter.
func (c *Counter) Add(n int) {
	c.lock.Lock()
	c.num += int64(n)
	c.lock.Unlock()
}

// Get function provides thread safe read of counter value.
func (c *Counter) Get() (ret int64) {
	c.lock.Lock()
	ret = c.num
	c.lock.Unlock()
	return
}
