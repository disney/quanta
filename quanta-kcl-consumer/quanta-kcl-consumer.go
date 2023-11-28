package main

import (
	"fmt"

	"github.com/araddon/dateparse"
	"github.com/aws/aws-sdk-go/aws"
	_ "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/qlbridge/expr/builtins"
	"github.com/disney/quanta/shared"
	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
	"github.com/hashicorp/consul/api"
	cfg "github.com/vmware/vmware-go-kcl/clientlibrary/config"
	kc "github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl/clientlibrary/metrics"
	"github.com/vmware/vmware-go-kcl/clientlibrary/metrics/cloudwatch"
	wk "github.com/vmware/vmware-go-kcl/clientlibrary/worker"

	//"golang.org/x/sync/errgroup"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
)

// Variables to identify the build
var (
	Version  string
	Build    string
	EPOCH, _ = time.ParseInLocation(time.RFC3339, "2000-01-01T00:00:00+00:00", time.UTC)
)

// Exit Codes
const (
	Success              = 0
	appName              = "Quanta"
	consumerName         = "enhanced-fan-out-consumer"
	metricsSystem        = "cloudwatch"
	partitionChannelSize = 500000
)

// Main strct defines command line arguments variables and various global meta-data associated with record loads.
type Main struct {
	Stream          string
	Region          string
	Index           string
	BufferSize      uint
	totalBytes      int64
	bytesLock       sync.RWMutex
	totalRecs       *Counter
	processedRecs   *Counter
	Port            int
	ConsulAddr      string
	ConsulClient    *api.Client
	ShardCount      int
	WorkerID        string
	InitialPos      cfg.InitialPositionInStream
	sessions        *Counter
	Table           *shared.BasicTable
	Schema          avro.Schema
	partitionMap    sync.Map
	partitionLock   sync.Mutex
	timeLocation    *time.Location
	clientConn      *shared.Conn
	partitionNotify chan string
}

// NewMain allocates a new pointer to Main struct with empty record counter
func NewMain() *Main {
	m := &Main{
		totalRecs:     &Counter{},
		processedRecs: &Counter{},
		sessions:      &Counter{},
		partitionMap:  sync.Map{},
	}
	name, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	m.WorkerID = fmt.Sprintf("%s:%s", name, uuid.New().String())
	loc, _ := time.LoadLocation("UTC")
	m.timeLocation = loc
	m.partitionNotify = make(chan string, 1000)
	return m
}

func main() {

	app := kingpin.New(os.Args[0], "Quanta Kinesis data consumer").DefaultEnvars()
	app.Version("Version: " + Version + "\nBuild: " + Build)

	stream := app.Arg("stream", "Kinesis stream name.").Required().String()
	index := app.Arg("index", "Table name (root name if nested schema)").Required().String()
	region := app.Arg("region", "AWS region").Default("us-east-1").String()
	port := app.Arg("port", "Port number for Quanta services").Default("4000").Int32()
	bufSize := app.Flag("buf-size", "Buffer size").Default("1000000").Int32()
	environment := app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()
	trimHorizon := app.Flag("trim-horizon", "Set initial position to TRIM_HORIZON").Bool()

	shared.InitLogging("WARN", *environment, "Kinesis-Consumer", Version, appName)

	kingpin.MustParse(app.Parse(os.Args[1:]))
	builtins.LoadAllBuiltins()

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
	log.Printf("Consul agent at [%s]", main.ConsulAddr)
	log.Printf("KCL Worker ID %s", main.WorkerID)
	if *trimHorizon == true {
		main.InitialPos = cfg.TRIM_HORIZON
		log.Printf("Initial position = TRIM_HORIZON")
	} else {
		main.InitialPos = cfg.LATEST
		log.Printf("Initial position = LATEST")
	}

	worker, err := main.Init()
	if err != nil {
		log.Fatal(err)
	}

	/*
	       // Receive notifications to startup new Quanta processors for partitions
	       go func() {
	           for partition := range main.partitionNotify {
	               log.Printf("Starting new Quanta processor for partition %s",  partition)
	   			var eg errgroup.Group
	   	        eg.Go(func() error {
	   	            conn, err := core.OpenConnection("", main.Index, true,
	   	                main.BufferSize, main.Port, main.clientConn)
	   	            if err != nil {
	   	                return fmt.Errorf("Error opening connection %v", err)
	   				}
	   				putCount := 0
	   				for {
	   					select {
	   					case row, ok := <- main.getPartition(partition):
	   						if !ok {
	   							break
	   						}
	   						err = conn.PutRow(main.Index, row, 0)
	   						if err != nil {
	   							log.Printf("ERROR in PutRow, partition %s - %v", partition, err)
	   							continue
	   						}
	   						_ = row
	   						putCount++
	   						main.processedRecs.Add(1)
	   					}
	   	            }
	   				//conn.CloseConnection()
	   	            return nil
	   	        })
	   			if err := eg.Wait(); err != nil {
	   			    log.Fatalf("ERROR starting Quanta processor for partition %s - %v", partition, err)
	   			}
	           }
	   		// TODO: Add shutdown/cleanup
	       }()
	*/

	go func() {
		for {
			main.partitionMap.Range(func(k, v interface{}) bool {
				//close(v.(chan map[string]interface{}))
				pc := v.(chan map[string]interface{})
				if len(pc) >= 1000 {
					for i := 0; i < 1000; i++ {
						select {
						case row := <-pc:
							_ = row
							main.processedRecs.Add(1)
						}
					}
				}

				return true
			})
		}
	}()

	var ticker *time.Ticker
	ticker = main.printStats()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for range c {
			log.Printf("Interrupted,  Bytes processed: %s, Records: %v", core.Bytes(main.BytesProcessed()),
				main.totalRecs.Get())
			ticker.Stop()
			worker.Shutdown()
			os.Exit(0)
		}
	}()

	<-c
	worker.Shutdown()
}

// Init function initilizations loader.
// Establishes session with bitmap server and Kinesis
func (m *Main) Init() (*wk.Worker, error) {

	var err error

	m.ConsulClient, err = api.NewClient(&api.Config{Address: m.ConsulAddr})
	if err != nil {
		return nil, err
	}

	m.clientConn = shared.NewDefaultConnection()
	m.clientConn.ServicePort = m.Port
	m.clientConn.Quorum = 3
	if err := m.clientConn.Connect(m.ConsulClient); err != nil {
		log.Fatal(err)
	}

	m.Table, err = shared.LoadSchema("", m.Index, m.ConsulClient)
	if err != nil {
		return nil, err
	}
	m.Schema = shared.ToAvroSchema(m.Table)

	kclConfig := cfg.NewKinesisClientLibConfig(m.Index, m.Stream, m.Region, m.WorkerID).
		WithInitialPositionInStream(m.InitialPos).
		//WithLeaseStealing(true).
		//WithMaxRecords(100).
		WithIdleTimeBetweenReadsInMillis(50).
		WithMaxLeasesForWorker(runtime.NumCPU() * 3).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000)

	kclConfig.WithMonitoringService(getMetricsConfig(kclConfig, metricsSystem))

	sess, errx := session.NewSession(&aws.Config{
		Region:      aws.String(m.Region),
		Endpoint:    aws.String(kclConfig.KinesisEndpoint),
		Credentials: kclConfig.KinesisCredentials,
	})

	if errx != nil {
		return nil, errx
	}
	kc := kinesis.New(sess)
	streamName := aws.String(m.Stream)
	shout, err := kc.ListShards(&kinesis.ListShardsInput{StreamName: streamName})
	if err != nil {
		return nil, err
	}
	m.ShardCount = len(shout.Shards)
	worker := wk.NewWorker(recordProcessorFactory(m), kclConfig)
	worker.Start()

	log.Printf("Created worker.")
	return worker, nil
}

func (m *Main) getPartition(partition string) chan map[string]interface{} {

	var c chan map[string]interface{}
	s, ok := m.partitionMap.Load(partition)
	if !ok {
		//m.partitionLock.Lock()
		//defer m.partitionLock.Unlock()
		c = make(chan map[string]interface{}, partitionChannelSize)
		m.partitionMap.Store(partition, c)
		select {
		case m.partitionNotify <- partition:
		default:
			log.Printf("blocking on creating new partition queue %s", partition)
		}
	} else {
		c = s.(chan map[string]interface{})
	}
	return c
}

func getMetricsConfig(kclConfig *cfg.KinesisClientLibConfiguration, service string) metrics.MonitoringService {

	if service == "cloudwatch" {
		return cloudwatch.NewMonitoringServiceWithOptions(kclConfig.RegionName,
			kclConfig.KinesisCredentials,
			kclConfig.Logger,
			cloudwatch.DEFAULT_CLOUDWATCH_METRICS_BUFFER_DURATION)
	}

	/*
		if service == "prometheus" {
			return prometheus.NewMonitoringService(":8080", regionName, kclConfig.Logger)
		}
	*/

	return nil
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
			log.Printf("Bytes: %s, Records: %v, Shards: %v, Processed: %v, Duration: %v, Rate: %v/s", core.Bytes(bytes), m.totalRecs.Get(), m.sessions.Get(), m.processedRecs.Get(), duration, core.Bytes(float64(bytes)/duration.Seconds()))
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
	defer c.lock.Unlock()
	c.num += int64(n)
}

// Get function provides thread safe read of counter value.
func (c *Counter) Get() (ret int64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	ret = c.num
	return
}

// Record processor factory
func recordProcessorFactory(m *Main) kc.IRecordProcessorFactory {
	return &quantaRecordProcessorFactory{main: m}
}

type quantaRecordProcessorFactory struct {
	main *Main
}

// CreateProcess - Factory method implementation.
func (f *quantaRecordProcessorFactory) CreateProcessor() kc.IRecordProcessor {
	return &quantaRecordProcessor{main: f.main}
}

// Quanta record processor state.
type quantaRecordProcessor struct {
	ShardID string
	main    *Main
	count   int
}

// Initialize record processor.
func (p *quantaRecordProcessor) Initialize(input *kc.InitializationInput) {

	log.Printf("Processing ShardID: %v at checkpoint: %v", input.ShardId,
		aws.StringValue(input.ExtendedSequenceNumber.SequenceNumber))
	p.ShardID = input.ShardId
	p.count = 0
	p.main.sessions.Add(1)
}

// ProcessRecords - Batch process records.
func (p *quantaRecordProcessor) ProcessRecords(input *kc.ProcessRecordsInput) {

	// don't process empty record
	if len(input.Records) == 0 {
		return
	}

	for _, v := range input.Records {

		ts, err := dateparse.ParseIn(*v.PartitionKey, p.main.timeLocation)
		if err != nil {
			log.Fatalf("Date parse error for partition key %s - %v", *v.PartitionKey, err)
		}
		tFormat := shared.YMDTimeFmt
		if p.main.Table.TimeQuantumType == "YMDH" {
			tFormat = shared.YMDHTimeFmt
		}
		partition := ts.Format(tFormat)

		out := make(map[string]interface{})
		err = avro.Unmarshal(p.main.Schema, v.Data, &out)
		if err != nil {
			log.Printf("ERROR %v", err)
			continue
		}
		c := p.main.getPartition(partition)
		select {
		case c <- out:
		default:
			log.Printf("record processor for shard %s blocked on partion queue %s", p.ShardID, partition)
		}
		p.count++
		p.main.totalRecs.Add(1)
		p.main.AddBytes(len(v.Data))
	}

	// checkpoint it after processing this batch.
	// Especially, for processing de-aggregated KPL records, checkpointing has to happen at the end of batch
	// because de-aggregated records share the same sequence number.
	lastRecordSequenceNumber := input.Records[len(input.Records)-1].SequenceNumber
	// Calculate the time taken from polling records and delivering to record processor for a batch.
	//diff := input.CacheExitTime.Sub(*input.CacheEntryTime)
	//log.Printf("Checkpoint progress at: %v,  MillisBehindLatest = %v, KCLProcessTime = %v", lastRecordSequenceNumber, input.MillisBehindLatest, diff)
	input.Checkpointer.Checkpoint(lastRecordSequenceNumber)
}

// Shutdown - Shut down processor.
func (p *quantaRecordProcessor) Shutdown(input *kc.ShutdownInput) {

	log.Printf("Shutdown Reason: %v", aws.StringValue(kc.ShutdownReasonMessage(input.ShutdownReason)))
	log.Printf("Processed Record Count = %d", p.count)

	// When the value of {@link ShutdownInput#getShutdownReason()} is
	// {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason#TERMINATE} it is required that you
	// checkpoint. Failure to do so will result in an IllegalArgumentException, and the KCL no longer making progress.
	if input.ShutdownReason == kc.TERMINATE {
		input.Checkpointer.Checkpoint(nil)
	}

	//p.Connection.CloseConnection()
	p.main.partitionMap.Range(func(k, v interface{}) bool {
		close(v.(chan map[string]interface{}))
		return true
	})
	p.main.sessions.Add(-1)
}
