package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	_ "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/shared"
	"github.com/hamba/avro"
	"github.com/hashicorp/consul/api"
	"github.com/google/uuid"
	cfg "github.com/vmware/vmware-go-kcl/clientlibrary/config"
	kc "github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl/clientlibrary/metrics"
	"github.com/vmware/vmware-go-kcl/clientlibrary/metrics/cloudwatch"
	wk "github.com/vmware/vmware-go-kcl/clientlibrary/worker"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
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
	Success       = 0
	appName       = "Quanta"
	consumerName  = "enhanced-fan-out-consumer"
	metricsSystem = "cloudwatch"
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
	WorkerID     string
}

// NewMain allocates a new pointer to Main struct with empty record counter
func NewMain() *Main {
	m := &Main{
		totalRecs: &Counter{},
	}
	name, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	m.WorkerID = fmt.Sprintf("%s:%s", name, uuid.New().String())
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

	core.InitLogging("WARN", *environment, "Kinesis-Consumer", Version, appName)

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
	log.Printf("KCL Worker ID %s\n", main.WorkerID)


	worker, err := main.Init()
	if err != nil {
		log.Fatal(err)
	}

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

	kclConfig := cfg.NewKinesisClientLibConfig(appName, m.Stream, m.Region, m.WorkerID).
		WithInitialPositionInStream(cfg.LATEST).
		WithLeaseStealing(true).
		WithMaxRecords(100).
		WithMaxLeasesForWorker(runtime.NumCPU()).
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

	log.Printf("Created worker. ")
	return worker, nil
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
			log.Printf("Bytes: %s, Records: %v, Duration: %v, Rate: %v/s", core.Bytes(bytes), m.totalRecs.Get(), duration, core.Bytes(float64(bytes)/duration.Seconds()))
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

	c, err := core.OpenConnection("", f.main.Index, true, f.main.BufferSize, f.main.Port, f.main.ConsulClient)
	if err != nil {
		log.Fatalf("Error opening connection %v", err)
		return nil
	}
	tbuf := c.TableBuffers[f.main.Index]
	return &quantaRecordProcessor{Connection: c, Table: tbuf.Table.BasicTable, main: f.main}
}

// Quanta record processor state.
type quantaRecordProcessor struct {
	ShardID    string
	Connection *core.Connection
	Table      *shared.BasicTable
	Schema     avro.Schema
	main       *Main
	count      int
}

// Initialize record processor.
func (p *quantaRecordProcessor) Initialize(input *kc.InitializationInput) {

	log.Printf("Processing ShardID: %v at checkpoint: %v", input.ShardId,
		aws.StringValue(input.ExtendedSequenceNumber.SequenceNumber))
	p.ShardID = input.ShardId
	p.count = 0
	p.Schema = shared.ToAvroSchema(p.Table)
}

// ProcessRecords - Batch process records.
func (p *quantaRecordProcessor) ProcessRecords(input *kc.ProcessRecordsInput) {

	// don't process empty record
	if len(input.Records) == 0 {
		return
	}

	for _, v := range input.Records {
		out := make(map[string]interface{})
		err := avro.Unmarshal(p.Schema, v.Data, &out)
		if err != nil {
			log.Printf("ERROR %v", err)
			continue
		}
		err = p.Connection.PutRow(p.main.Index, out, 0)
		if err != nil {
			log.Printf("ERROR in PutRow - %v", err)
			continue
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
	p.Connection.Flush()
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

	p.Connection.CloseConnection()
}
