package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/custom/functions"
	"github.com/disney/quanta/shared"
	"github.com/hamba/avro"
	consumer "github.com/harlow/kinesis-consumer"
	store "github.com/harlow/kinesis-consumer/store/ddb"
	"github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stvp/rendezvous"
	"golang.org/x/sync/errgroup"
	"gopkg.in/alecthomas/kingpin.v2"
)

// Variables to identify the build
var (
	Version             string
	Build	            string
	ShardChannelSize    = 10000
	EPOCH, _ = time.ParseInLocation(time.RFC3339, "2000-01-01T00:00:00+00:00", time.UTC)
)

// Exit Codes
const (
	Success = 0
	appName			  = "Kinesis-Consumer"
)

// Main struct defines command line arguments variables and various global meta-data associated with record loads.
type Main struct {
	Stream				string
	Region				string
	Index				string
	BufferSize			uint
	totalBytes			*Counter
	totalBytesL			*Counter
	totalRecs			*Counter
	totalRecsL			*Counter
	errorCount			*Counter
	poolPercent			*Counter
	Port				int
	ConsulAddr			string
	ShardCount			int
	lock				*api.Lock
	consumer			*consumer.Consumer
	Table				*shared.BasicTable
	Schema				avro.Schema
	InitialPos			string
	IsAvro				bool
	CheckpointDB		bool
	CheckpointTable		string
	AssumeRoleArn		string
	AssumeRoleArnRegion string
	Deaggregate			bool
	Collate				bool
	Preselector			expr.Node
	selectorIdentities  []string
	ShardKey            string
	HashTable           *rendezvous.Table
	shardChannels       map[string]chan map[string]interface{}
	eg                  errgroup.Group
	cancelFunc          context.CancelFunc
	CommitIntervalMs    int
	processedRecs		*Counter
	processedRecL		*Counter
	scanInterval		int
	metrics				*cloudwatch.CloudWatch
}

// NewMain allocates a new pointer to Main struct with empty record counter
func NewMain() *Main {
	m := &Main{
		totalRecs:	 &Counter{},
		totalRecsL:	&Counter{},
		totalBytes:	&Counter{},
		totalBytesL:   &Counter{},
		processedRecs: &Counter{},
		processedRecL: &Counter{},
		errorCount:	&Counter{},
	}
	return m
}

func main() {

	app := kingpin.New(os.Args[0], "Quanta Kinesis data consumer").DefaultEnvars()
	app.Version("Version: " + Version + "\nBuild: " + Build)

	stream := app.Arg("stream", "Kinesis stream name.").Required().String()
	index := app.Arg("index", "Table name (root name if nested schema)").Required().String()
	shardKey := app.Arg("shard-key", "Shard Key").Required().String()
	region := app.Arg("region", "AWS region").Default("us-east-1").String()
	port := app.Flag("port", "Port number for service").Default("4000").Int32()
	bufSize := app.Flag("buf-size", "Buffer size").Default("1000000").Int32()
	withAssumeRoleArn := app.Flag("assume-role-arn", "Assume role ARN.").String()
	withAssumeRoleArnRegion := app.Flag("assume-role-arn-region", "Assume role ARN region.").String()
	environment := app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()
	trimHorizon := app.Flag("trim-horizon", "Set initial position to TRIM_HORIZON").Bool()
	noCheckpointer := app.Flag("no-checkpoint-db", "Disable DynamoDB checkpointer.").Bool()
	checkpointTable := app.Flag("checkpoint-table", "DynamoDB checkpoint table name.").String()
	avroPayload := app.Flag("avro-payload", "Payload is Avro.").Bool()
	deaggregate := app.Flag("deaggregate", "Incoming payload records are aggregated.").Bool()
	preselector := app.Flag("preselector", "Filter expression (url encoded).").String()
	scanInterval := app.Flag("scan-interval", "Scan interval (milliseconds)").Default("1000").Int()
	commitInterval := app.Flag("commit-interval", "Commit interval (milliseconds)").Int()
	logLevel := app.Flag("log-level", "Log Level [ERROR, WARN, INFO, DEBUG]").Default("WARN").String()

	kingpin.MustParse(app.Parse(os.Args[1:]))

	shared.InitLogging(*logLevel, *environment, appName, Version, "Quanta")

	builtins.LoadAllBuiltins()
	functions.LoadAll()

	main := NewMain()
	main.Stream = *stream
	main.Region = *region
	main.Index = *index
	main.BufferSize = uint(*bufSize)
	main.scanInterval = *scanInterval
	main.Port = int(*port)
	main.ConsulAddr = *consul

	log.Printf("Set Logging level to %v.", *logLevel)
	log.Printf("Kinesis stream %v.", main.Stream)
	log.Printf("Kinesis region %v.", main.Region)
	log.Printf("Index name %v.", main.Index)
	log.Printf("Buffer size %d.", main.BufferSize)
	log.Printf("Scan interval (milliseconds) %d.", main.scanInterval)
	if *commitInterval == 0 {
		log.Printf("Commits will occur after each row.")
	} else {
		main.CommitIntervalMs = *commitInterval
		log.Printf("Commits will occur every %d milliseconds.", main.CommitIntervalMs)
	}
	log.Printf("Service port %d.", main.Port)
	log.Printf("Consul agent at [%s]\n", main.ConsulAddr)
	if *trimHorizon {
		main.InitialPos = "TRIM_HORIZON"
		log.Printf("Initial position = TRIM_HORIZON")
	} else {
		main.InitialPos = "LATEST"
		log.Printf("Initial position = LATEST")
	}
	if *noCheckpointer {
		main.CheckpointDB = false
		log.Printf("Checkpoint DB disabled.")
	} else {
		main.CheckpointDB = true
		log.Printf("Checkpoint DB enabled.")
		if *checkpointTable != "" {
			main.CheckpointTable = *checkpointTable
		} else {
			main.CheckpointTable = main.Index
		}
		log.Printf("DynamoDB checkpoint table name [%s]", main.CheckpointTable)
	}
	if shardKey != nil  {
		v := *shardKey
		var path string
		if v[0] == '/' {
			path = v[1:]
		} else {
			path = v
		}
		main.ShardKey = path
		log.Printf("Shard key = %v.", main.ShardKey)
	}
	if *withAssumeRoleArn != "" {
		main.AssumeRoleArn = *withAssumeRoleArn
		log.Printf("With assume role ARN [%s]", main.AssumeRoleArn)
		if *withAssumeRoleArnRegion != "" {
			main.AssumeRoleArnRegion = *withAssumeRoleArnRegion
			log.Printf("With assume role ARN region [%s]", main.AssumeRoleArnRegion)
		} else {
			main.AssumeRoleArnRegion = *region
			log.Printf("With assume role ARN region [%s]", main.AssumeRoleArnRegion)
		}
	}
	if *avroPayload {
		main.IsAvro = true
		log.Printf("Payload is Avro.")
	} else {
		main.IsAvro = false
		log.Printf("Payload is JSON.")
	}
	if *deaggregate {
		main.Deaggregate = true
		log.Printf("Payload is aggregated, de-aggregation is enabled in consumer.")
	} else {
		main.Deaggregate = false
	}

	var err error

	if *preselector != "" {
		decodedValue, err := url.QueryUnescape(*preselector)
		if err != nil {
			u.Error(err)
			os.Exit(1)
		}
		main.Preselector, err = expr.ParseExpression(decodedValue)
		if err != nil {
			u.Error(err)
			os.Exit(1)
		}
		main.selectorIdentities = expr.FindAllIdentityField(main.Preselector)
		log.Printf("Preseletor enabled -> %v <-", main.Preselector)
	}

	if main.ShardCount, err = main.Init(); err != nil {
		u.Error(err)
		os.Exit(1)
	}

	go func() {
		// Start Prometheus endpoint
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()

	var ticker *time.Ticker
	ticker = main.printStats()

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for range c {
			u.Warnf("Interrupted,  Bytes processed: %s, Records: %v", core.Bytes(main.totalBytes.Get()),
				main.totalRecs.Get())
			u.Errorf("Interrupt received, calling cancel function")
			if main.cancelFunc != nil {
				main.cancelFunc()
			}
		}
	}()

	// Main processing loop will continue forever until a SIGKILL is received.
	var ctx context.Context
	for  {
		ctx, main.cancelFunc = context.WithCancel(context.Background())
		scanErr := main.consumer.Scan(ctx, main.scanAndProcess)
		main.destroy()
		if err := main.eg.Wait(); err != nil {
			u.Errorf("session error: %v", err)
		}
		if scanErr != nil {
			u.Errorf("scan error: %v", scanErr)
		} else {
			u.Warnf("Received Cancellation.")
		}
		if main.InitialPos == "TRIM_HORIZON" {
			u.Error("can't re-initialize 'in-place' if set to TRIM_HORIZON, exiting")
			os.Exit(1)
		}
		u.Warnf("Re-initializing.")
		if main.ShardCount, err = main.Init(); err != nil {
			u.Errorf("initialization error: %v", err)
			u.Errorf("Exiting process.")
			os.Exit(1)
		}
	}
	ticker.Stop()
}

func (m *Main) destroy() {

	m.cancelFunc = nil
	for _, v := range m.shardChannels {
		close(v)
	}
	time.Sleep(time.Second * 5)  // Allow time for completion
}

func (m *Main) scanAndProcess(v *consumer.Record) error {

	out := make(map[string]interface{})

	var err error
	if m.IsAvro {
		err = avro.Unmarshal(m.Schema, v.Data, &out)
	} else {
		err = json.Unmarshal(v.Data, &out)
	}
	if err != nil {
		u.Errorf("Unmarshal ERROR %v", err)
		m.errorCount.Add(1)
		return nil
	}

	// Got record at this point
	if m.Preselector != nil {
		if !m.preselect(out) {
			return nil // continue processing
		}
	}

	// push into the appropriate shard channel
	if key, err := shared.GetPath(m.ShardKey, out, false, false); err != nil {
		return err
	} else {
		shard := m.HashTable.GetN(1, key.(string))
		ch, ok := m.shardChannels[shard[0]]
		if !ok {
			return fmt.Errorf("Cannot locate channel for shard key %v", key)
		}
		ch <- out
		m.totalRecs.Add(1)
		m.totalBytes.Add(len(v.Data))
	}
	return nil // continue scanning
}

// filter row per expression
func (m *Main) preselect(row map[string]interface{}) bool {

	ctx := m.buildEvalContext(row)
	val, ok := vm.Eval(ctx, m.Preselector)
	if !ok {
		u.Errorf("Preselect expression %s failed to evaluate ", m.Preselector.String())
		os.Exit(1)
	}
	if val.Type() != value.BoolType {
		u.Errorf("Preselect expression %s does not evaluate to a boolean value", m.Preselector.String())
		os.Exit(1)
	}

	return val.Value().(bool)
}

func (m *Main) buildEvalContext(row map[string]interface{}) *datasource.ContextSimple {

	data := make(map[string]interface{})
	for _, v := range m.selectorIdentities {
		var path string
		if v[0] == '/' {
			path = v[1:]
		} else {
			path = v
		}
		if l, err := shared.GetPath(path, row, false, false); err == nil {
			data[v] = l
		}
	}
	return datasource.NewContextSimpleNative(data)
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

/*
// MemberLeft - Implements member leave notification due to failure.
func (m *Main) MemberLeft(nodeID string, index int) {

	u.Warnf("node %v left the cluster, purging sessions", nodeID)
	go m.recoverInflight(m.sessionPool.Recover)
}

// MemberJoined - A new node joined the cluster.
func (m *Main) MemberJoined(nodeID, ipAddress string, index int) {

	u.Warnf("node %v joined the cluster, purging sessions", nodeID)
	go m.recoverInflight(m.sessionPool.Recover)
}

// recover in-flight data in new thread
func (m *Main) recoverInflight(recoverFunc func(unflushedCh chan *shared.BatchBuffer)) {

	sess, err := m.sessionPool.NewSession(m.Index)
	if err != nil {
		u.Errorf("recoverInflight error creating recovery session %v", err)
	}
	rc := make(chan *shared.BatchBuffer, runtime.NumCPU())
	recoverFunc(rc)
	close(rc)
	i := 1
	for batch := range rc {
		u.Infof("recoverInflight session %d", i)
		batch.MergeInto(sess.BatchBuffer)
		i++
	}
	err = sess.CloseSession() // Will flush first
	if err != nil {
		u.Errorf("recoverInflight error closing session %v", err)
	}
}
*/


func (m *Main) schemaChangeListener(e shared.SchemaChangeEvent) {

	if m.cancelFunc != nil {
		m.cancelFunc()
	}
	switch e.Event {
	case shared.Drop:
		//m.sessionPool.Recover(nil)
		u.Warnf("Dropped table %s", e.Table)
	case shared.Modify:
		u.Warnf("Truncated table %s", e.Table)
	case shared.Create:
		//m.sessionPool.Recover(nil)
		u.Warnf("Created table %s", e.Table)
	}
}

// Init function initilizations loader.
// Establishes session with bitmap server and Kinesis
func (m *Main) Init() (int, error) {

	consulConfig := &api.Config{Address: m.ConsulAddr}
	consulClient, err := api.NewClient(consulConfig)
	if err != nil {
		return 0, err
	}

	// Register for Schema changes
	err = shared.RegisterSchemaChangeListener(consulConfig, m.schemaChangeListener)
	if err != nil {
		return 0, err
	}

	clientConn := shared.NewDefaultConnection()
	clientConn.ServicePort = m.Port
	clientConn.Quorum = 3
	if err := clientConn.Connect(consulClient); err != nil {
		u.Error(err)
		os.Exit(1)
	}

	// Register member leave/join
	//clientConn.RegisterService(m)

	m.Table, err = shared.LoadSchema("", m.Index, consulClient)
	if err != nil {
		return 0, err
	}
	if m.IsAvro {
		m.Schema = shared.ToAvroSchema(m.Table)
	}

	sess, errx := session.NewSession(&aws.Config{
		Region: aws.String(m.Region),
	})
	if errx != nil {
		return 0, errx
	}

	var kc *kinesis.Kinesis
	if m.AssumeRoleArn != "" {
		creds := stscreds.NewCredentials(sess, m.AssumeRoleArn)
		config := aws.NewConfig().
			WithCredentials(creds).
			WithRegion(m.AssumeRoleArnRegion).
			WithMaxRetries(10)
		kc = kinesis.New(sess, config)
	} else {
		kc = kinesis.New(sess)
	}

	streamName := aws.String(m.Stream)
	shout, err := kc.ListShards(&kinesis.ListShardsInput{StreamName: streamName})
	if err != nil {
		return 0, err
	}
	shardCount := len(shout.Shards)
	dynamoDbClient := dynamodb.New(sess)

	db, err := store.New(appName, m.CheckpointTable, store.WithDynamoClient(dynamoDbClient), store.WithRetryer(&QuantaRetryer{}))
	if err != nil {
		u.Errorf("checkpoint storage initialization error: %v", err)
		os.Exit(1)
	}

	if m.CheckpointDB {
		m.consumer, err = consumer.New(
			m.Stream,
			consumer.WithClient(kc),
			consumer.WithShardIteratorType(m.InitialPos),
			consumer.WithStore(db),
			consumer.WithAggregation(m.Deaggregate),
			consumer.WithScanInterval(time.Duration(m.scanInterval) * time.Millisecond),
			//consumer.WithCounter(counter),
		)
	} else {
		m.consumer, err = consumer.New(
			m.Stream,
			consumer.WithClient(kc),
			consumer.WithShardIteratorType(m.InitialPos),
			consumer.WithAggregation(m.Deaggregate),
			consumer.WithScanInterval(time.Duration(m.scanInterval) * time.Millisecond),
			//consumer.WithCounter(counter),
		)
	}
	if err != nil {
		return 0, err
	}
	m.metrics = cloudwatch.New(sess)

	// Initialize shard channels
	u.Warnf("Shard count = %d", shardCount)
	m.shardChannels = make(map[string]chan map[string]interface{})
	shardIds := make([]string, shardCount)
	core.ClearTableCache()
	for i := 0; i < shardCount; i++ {
		k := fmt.Sprintf("shard%v", i)
		shardIds[i] = k
		m.shardChannels[k] = make(chan map[string]interface{}, ShardChannelSize)
		shardId := k
		m.eg.Go(func() error {
			conn, err := core.OpenSession("", m.Index, false, clientConn)
			if err != nil {
				return err
			}
			nextCommit := time.Now().Add(time.Millisecond * time.Duration(m.CommitIntervalMs))
			for data := range m.shardChannels[shardId] {
				err = conn.PutRow(m.Index, data, 0, false, false)
				if err != nil {
					u.Errorf("ERROR in PutRow, shard %s - %v", shardId, err)
					m.errorCount.Add(1)
					return err
				}
				m.processedRecs.Add(1)
				if time.Now().After(nextCommit) {
					conn.Flush()
					nextCommit = time.Now().Add(time.Millisecond * time.Duration(m.CommitIntervalMs))
				}
			}
			conn.CloseSession()
			return nil
		})
	}
	m.HashTable = rendezvous.New(shardIds)
	u.Infof("Created consumer. ")

	return shardCount, nil
}

// printStats outputs to Log current status of Kinesis consumer
// Includes data on processed: bytes, records, time duration in seconds, and rate of bytes per sec"
func (m *Main) printStats() *time.Ticker {
	t := time.NewTicker(time.Second * 10)
	start := time.Now()
	lastTime := time.Now()
	go func() {
		for range t.C {
			duration := time.Since(start)
			bytes := m.totalBytes.Get()
			u.Infof("Bytes: %s, Records: %v, Processed: %v, Errors: %v, Duration: %v, Rate: %v/s",
				core.Bytes(bytes), m.totalRecs.Get(), m.processedRecs.Get(), m.errorCount.Get(), duration,
				core.Bytes(float64(bytes)/duration.Seconds()))
			lastTime = m.publishMetrics(duration, lastTime)
		}
	}()
	return t
}

// Global storage for Prometheus metrics
var (
	totalRecs = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "consumer_records_arrived",
		Help: "The total number of records consumed",
	})

	totalRecsPerSec = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "consumer_records_arrived_per_sec",
		Help: "The total number of records consumed per second",
	})

	processedRecs = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "consumer_records_processed",
		Help: "The number of records processed",
	})

	processedRecsPerSec = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "consumer_records_processed_per_sec",
		Help: "The number of records processed per second",
	})

	totalBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "consumer_bytes_arrived",
		Help: "The total number of bytes consumed",
	})

	errors = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "total_errors",
		Help: "The total number of errors",
	})

	processedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "processed_bytes",
		Help: "The total number of processed bytes",
	})

	processedBytesPerSec = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "processed_bytes_per_second",
		Help: "The total number of processed bytes per second",
	})

	uptimeHours = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "uptime_hours",
		Help: "Hours of up time",
	})
)

func (m *Main) publishMetrics(upTime time.Duration, lastPublishedAt time.Time) time.Time {

	interval := time.Since(lastPublishedAt).Seconds()
	_, err := m.metrics.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: aws.String("Quanta-Consumer/Records"),
		MetricData: []*cloudwatch.MetricDatum{
			{
				MetricName: aws.String("Arrived"),
				Unit:	   aws.String("Count"),
				Value:	  aws.Float64(float64(m.totalRecs.Get())),
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  aws.String("Stream"),
						Value: aws.String(m.Stream),
					},
				},
			},
			{
				MetricName: aws.String("RecordsPerSec"),
				Unit:	   aws.String("Count/Second"),
				Value:	  aws.Float64(float64(m.totalRecs.Get()-m.totalRecsL.Get()) / interval),
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  aws.String("Stream"),
						Value: aws.String(m.Stream),
					},
				},
			},
			{
				MetricName: aws.String("Processed"),
				Unit:	   aws.String("Count"),
				Value:	  aws.Float64(float64(m.processedRecs.Get())),
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  aws.String("Table"),
						Value: aws.String(m.Index),
					},
				},
			},
			{
				MetricName: aws.String("ProcessedPerSecond"),
				Unit:	   aws.String("Count/Second"),
				Value:	  aws.Float64(float64(m.processedRecs.Get()-m.processedRecL.Get()) / interval),
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  aws.String("Table"),
						Value: aws.String(m.Index),
					},
				},
			},
			{
				MetricName: aws.String("Errors"),
				Unit:	   aws.String("Count"),
				Value:	  aws.Float64(float64(m.errorCount.Get())),
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  aws.String("Table"),
						Value: aws.String(m.Index),
					},
				},
			},
			{
				MetricName: aws.String("ProcessedBytes"),
				Unit:	   aws.String("Bytes"),
				Value:	  aws.Float64(float64(m.totalBytes.Get())),
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  aws.String("Table"),
						Value: aws.String(m.Index),
					},
				},
			},
			{
				MetricName: aws.String("BytesPerSec"),
				Unit:	   aws.String("Bytes/Second"),
				Value:	  aws.Float64(float64(m.totalBytes.Get()-m.totalBytesL.Get()) / interval),
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  aws.String("Table"),
						Value: aws.String(m.Index),
					},
				},
			},
			{
				MetricName: aws.String("UpTimeHours"),
				Unit:	   aws.String("Count"),
				Value:	  aws.Float64(float64(upTime) / float64(1000000000*3600)),
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  aws.String("Table"),
						Value: aws.String(m.Index),
					},
				},
			},
		},
	})
	// Set Prometheus values
	totalRecs.Set(float64(m.totalRecs.Get()))
	totalRecsPerSec.Set(float64(m.totalRecs.Get()-m.totalRecsL.Get()) / interval)
	processedRecs.Set(float64(m.processedRecs.Get()))
	processedRecsPerSec.Set(float64(m.processedRecs.Get()-m.processedRecL.Get()) / interval)
	errors.Set(float64(m.errorCount.Get()))
	processedBytes.Set(float64(m.totalBytes.Get()))
	processedBytesPerSec.Set(float64(m.totalBytes.Get()-m.totalBytesL.Get()) / interval)
	uptimeHours.Set(float64(upTime) / float64(1000000000*3600))

	m.totalRecsL.Set(m.totalRecs.Get())
	m.processedRecL.Set(m.processedRecs.Get())
	m.totalBytesL.Set(m.totalBytes.Get())
	if err != nil {
		u.Error(err)
	}
	return time.Now()
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

// Set function provides thread safe set of counter value.
func (c *Counter) Set(n int64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.num = n
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


