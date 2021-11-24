package main

import (
	"context"
	//"expvar"
	"fmt"
	"github.com/araddon/dateparse"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/aws-sdk-go/aws"
	_ "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/disney/quanta/client"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/shared"
	"github.com/hamba/avro"
	"github.com/harlow/kinesis-consumer"
	store "github.com/harlow/kinesis-consumer/store/ddb"
	"github.com/hashicorp/consul/api"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"os"
	"os/signal"
	//"runtime"
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
	//partitionChannelSize = 10000
	partitionChannelSize = 2000000
	batchSize            = 100000
)

// Main strct defines command line arguments variables and various global meta-data associated with record loads.
type Main struct {
	Stream        string
	Region        string
	Index         string
	BufferSize    uint
	totalBytes    int64
	bytesLock     sync.RWMutex
	totalRecs     *Counter
	errorCount    *Counter
	Port          int
	ConsulAddr    string
	ShardCount    int
	lock          *api.Lock
	consumer      *consumer.Consumer
	Table         *shared.BasicTable
	Schema        avro.Schema
	InitialPos    string
	partitionMap  map[string]*Partition
	partitionLock sync.Mutex
	timeLocation  *time.Location
	processedRecs *Counter
	sessionPool   *core.SessionPool
}

// NewMain allocates a new pointer to Main struct with empty record counter
func NewMain() *Main {
	m := &Main{
		totalRecs:     &Counter{},
		processedRecs: &Counter{},
		errorCount:    &Counter{},
		partitionMap:  make(map[string]*Partition),
	}
	loc, _ := time.LoadLocation("UTC")
	m.timeLocation = loc
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
	trimHorizon := app.Flag("trim-horizon", "Set initial position to TRIM_HORIZON").Bool()

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
	if *trimHorizon == true {
		main.InitialPos = "TRIM_HORIZON"
		log.Printf("Initial position = TRIM_HORIZON")
	} else {
		main.InitialPos = "LATEST"
		log.Printf("Initial position = LATEST")
	}

	var err error

	if main.ShardCount, err = main.Init(); err != nil {
		log.Fatal(err)
	}

	var ticker *time.Ticker
	ticker = main.printStats()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Printf("Interrupted,  Bytes processed: %s, Records: %v", core.Bytes(main.BytesProcessed()),
				main.totalRecs.Get())
			//close(msgChan)
			//main.consumer.Close()
			ticker.Stop()
			os.Exit(0)
		}
	}()

	// Main processing loop
	go func() {
		err = main.consumer.Scan(context.TODO(), func(v *consumer.Record) error {
			ts, err := dateparse.ParseIn(*v.PartitionKey, main.timeLocation)
			if err != nil {
				log.Fatalf("Date parse error for partition key %s - %v", *v.PartitionKey, err)
			}
			tFormat := shared.YMDTimeFmt
			if main.Table.TimeQuantumType == "YMDH" {
				tFormat = shared.YMDHTimeFmt
			}
			partition := ts.Format(tFormat)

			out := make(map[string]interface{})
			err = avro.Unmarshal(main.Schema, v.Data, &out)
			if err != nil {
				log.Printf("Unmarshal ERROR %v", err)
				return nil
			}
			if main.ShardCount > 1 {
				c := main.getPartition(partition)
				select {
				case c.Data <- out:
				}
			} else { // Bypass partition worker dispatching
				if err := main.processBatch([]map[string]interface{}{out}, partition); err != nil {
					log.Printf("processBatch ERROR %v", err)
					return nil // continue processing
				}
			}
			main.totalRecs.Add(1)
			main.AddBytes(len(v.Data))

			return nil // continue scanning
		})

		if err != nil {
			log.Fatalf("scan error: %v", err)
		}
	}()

	// Stale partition cleanup
	go func() {
		for {
			itemsOutstanding := 0
			stalePartitions := 0
			main.partitionLock.Lock()
			for k, v := range main.partitionMap {
				if time.Since(v.ModTime) >= time.Duration(1000*time.Second) {
					stalePartitions++
					if len(v.Data) == 0 {
						delete(main.partitionMap, k)
					}
				}
				if time.Since(v.ModTime) >= time.Duration(10*time.Second) || len(v.Data) >= batchSize {
					batch := v.GetDataRows()
					key := k
					go func(rows []map[string]interface{}, part string) {
						if err := main.processBatch(rows, part); err != nil {
							main.errorCount.Add(1)
						}
					}(batch, key)
				}
				itemsOutstanding += len(v.Data)
			}
			main.partitionLock.Unlock()
			//_, inUse := main.sessionPool.Metrics()
			//log.Printf("PARTITIONS %d, STALE %d, OUTSTANDING ITEMS = %d, POOL = %d",
			//		len(main.partitionMap), stalePartitions, itemsOutstanding, inUse)
			select {
			case _, done := <-c:
				if done {
					break
				}
			case <-time.After(10 * time.Second):
			}
		}
	}()

	<-c
}

func (m *Main) processBatch(rows []map[string]interface{}, partition string) error {

	conn, err := m.sessionPool.Borrow(m.Index)
	if err != nil {
		return fmt.Errorf("Error opening Quanta session %v", err)
	}
	defer m.sessionPool.Return(m.Index, conn)
	for i := 0; i < len(rows); i++ {
		err = conn.PutRow(m.Index, rows[i], 0)
		if err != nil {
			log.Printf("ERROR in PutRow, partition %s - %v", partition, err)
			continue
		}
		m.processedRecs.Add(1)
	}
	return nil
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

// Init function initilizations loader.
// Establishes session with bitmap server and Kinesis
func (m *Main) Init() (int, error) {

	consulClient, err := api.NewClient(&api.Config{Address: m.ConsulAddr})
	if err != nil {
		return 0, err
	}

	clientConn := quanta.NewDefaultConnection()
	clientConn.ServicePort = m.Port
	clientConn.Quorum = 3
	if err := clientConn.Connect(consulClient); err != nil {
		log.Fatal(err)
	}

	m.sessionPool = core.NewSessionPool(clientConn, nil, "")

	m.Table, err = shared.LoadSchema("", m.Index, consulClient)
	if err != nil {
		return 0, err
	}
	m.Schema = shared.ToAvroSchema(m.Table)

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

	// Override the Kinesis if any needs on session (e.g. assume role)
	//myDynamoDbClient := dynamodb.New(session.New(aws.NewConfig()))
	dynamoDbClient := dynamodb.New(sess)

	// For versions of AWS sdk that fixed config being picked up properly, the example of
	// setting region should work.
	//    myDynamoDbClient := dynamodb.New(session.New(aws.NewConfig()), &aws.Config{
	//        Region: aws.String("us-west-2"),
	//    })

	db, err := store.New(m.Index, m.Index, store.WithDynamoClient(dynamoDbClient), store.WithRetryer(&QuantaRetryer{}))
	if err != nil {
		log.Fatalf("checkpoint storage initialization error: %v", err)
	}

	m.consumer, err = consumer.New(
		m.Stream,
		consumer.WithClient(kc),
		//consumer.WithShardIteratorType(types.ShardIteratorTypeTrimHorizon),
		consumer.WithShardIteratorType(m.InitialPos),
		consumer.WithStore(db),
		//consumer.WithCounter(counter),
	)
	if err != nil {
		return 0, err
	}
	log.Printf("Created consumer. ")
	return shardCount, nil
}

type Partition struct {
	ModTime      time.Time
	PartitionKey string
	Data         chan map[string]interface{}
}

func NewPartition(partition string) *Partition {
	return &Partition{PartitionKey: partition, Data: make(chan map[string]interface{}, partitionChannelSize),
		ModTime: time.Now().UTC()}
}

func (p *Partition) GetDataRows() []map[string]interface{} {

	size := len(p.Data)
	if size > batchSize {
		size = batchSize
	}
	rows := make([]map[string]interface{}, size)
	for i := 0; i < len(rows); i++ {
		select {
		case row := <-p.Data:
			rows[i] = row
		}
	}
	return rows
}

func (m *Main) getPartition(partition string) *Partition {

	m.partitionLock.Lock()
	defer m.partitionLock.Unlock()

	c, ok := m.partitionMap[partition]
	if !ok {
		c = NewPartition(partition)
		m.partitionMap[partition] = c
	}
	c.ModTime = time.Now().UTC()
	return c
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
			log.Printf("Bytes: %s, Records: %v, Processed: %v, Errors: %v, Duration: %v, Rate: %v/s",
				core.Bytes(bytes), m.totalRecs.Get(), m.processedRecs.Get(), m.errorCount.Get(), duration,
				core.Bytes(float64(bytes)/duration.Seconds()))
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

// QuantaRetryer used for storage
type QuantaRetryer struct {
	store.Retryer
}

// ShouldRetry implements custom logic for when errors should retry
func (r *QuantaRetryer) ShouldRetry(err error) bool {
	switch err.(type) {
	case *types.ProvisionedThroughputExceededException, *types.LimitExceededException:
		return true
	}
	return false
}
