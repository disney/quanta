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
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/araddon/dateparse"
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
	"gopkg.in/alecthomas/kingpin.v2"
)

// Variables to identify the build
var (
	Version  string
	Build	string
	EPOCH, _ = time.ParseInLocation(time.RFC3339, "2000-01-01T00:00:00+00:00", time.UTC)
)

// Exit Codes
const (
	Success = 0
	//partitionChannelSize = 10000
	partitionChannelSize = 2000000
	batchSize			= 100000
	appName			  = "Kinesis-Consumer"
)

// Main strct defines command line arguments variables and various global meta-data associated with record loads.
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
	partitionMap		map[string]*Partition
	partitionLock		sync.Mutex
	timeLocation		*time.Location
	processedRecs		*Counter
	processedRecL		*Counter
	sessionPool			*core.SessionPool
	sessionPoolSize		int
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
		poolPercent:   &Counter{},
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
	port := app.Flag("port", "Port number for service").Default("4000").Int32()
	bufSize := app.Flag("buf-size", "Buffer size").Default("1000000").Int32()
	poolSize := app.Flag("session-pool-size", "Session pool size").Int()
	withAssumeRoleArn := app.Flag("assume-role-arn", "Assume role ARN.").String()
	withAssumeRoleArnRegion := app.Flag("assume-role-arn-region", "Assume role ARN region.").String()
	environment := app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()
	trimHorizon := app.Flag("trim-horizon", "Set initial position to TRIM_HORIZON").Bool()
	noCheckpointer := app.Flag("no-checkpoint-db", "Disable DynamoDB checkpointer.").Bool()
	checkpointTable := app.Flag("checkpoint-table", "DynamoDB checkpoint table name.").String()
	avroPayload := app.Flag("avro-payload", "Payload is Avro.").Bool()
	deaggregate := app.Flag("deaggregate", "Incoming payload records are aggregated.").Bool()
	collate := app.Flag("collate", "Collate and partation shards.").Bool()
	preselector := app.Flag("preselector", "Filter expression (url encoded).").String()
	scanInterval := app.Flag("scan-interval", "Scan interval (milliseconds)").Default("1000").Int()
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
	// If the pool size is not configured then set it to the number of available CPUs
	main.sessionPoolSize = *poolSize
	if main.sessionPoolSize == 0 {
		main.sessionPoolSize = runtime.NumCPU()
		log.Printf("Session Pool Size not set, defaulting to number of available CPUs = %d", main.sessionPoolSize)
	} else {
		log.Printf("Session Pool Size = %d", main.sessionPoolSize)
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
	if *collate {
		main.Collate = true
		log.Printf("Shard collation enabled.")
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

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			u.Infof("Interrupted,  Bytes processed: %s, Records: %v", core.Bytes(main.totalBytes.Get()),
				main.totalRecs.Get())
			//main.consumer.Close()
			ticker.Stop()
			os.Exit(0)
		}
	}()

	// Main processing loop
	go func() {
		for  {
			select {
			case _, done := <-c:
				if done {
					break
				}
			default:
			}
			err = main.consumer.Scan(context.TODO(), main.scanAndProcess)
			if err != nil {
				u.Errorf("scan error: %v", err)
				u.Errorf("Re-initializing.")
    			if main.ShardCount, err = main.Init(); err != nil {
        			u.Error(err)
					u.Errorf("Exiting process.")
        			os.Exit(1)
    			}
				//os.Exit(1)
			}
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
							if err == core.ErrPoolDrained {
								// Couldn't process, put data back in partition channel
								time.Sleep(1 * time.Second)
								main.getPartition(part).PutDataRows(rows)
							} else {
								main.errorCount.Add(1)
								u.Error(err)
							}
						}
					}(batch, key)
				}
				itemsOutstanding += len(v.Data)
			}
			main.partitionLock.Unlock()
			poolSize, inUse, _, _ := main.sessionPool.Metrics()
			if itemsOutstanding > 0 {
				u.Infof("Partitions %d, Outstanding Items = %d, Processors in use = %d",
					len(main.partitionMap), itemsOutstanding, inUse)
			}
			main.poolPercent.Set(int64(float64(inUse/poolSize) * 100))
			select {
			case _, done := <-c:
				if done {
					break
				}
			case <-time.After(1 * time.Second):
			}
		}
	}()

	<-c
}

func (m *Main) scanAndProcess(v *consumer.Record) error {

	ts, err := dateparse.ParseIn(*v.PartitionKey, m.timeLocation)
	if err != nil {
		u.Errorf("Date parse error for partition key %s - %v", *v.PartitionKey, err)
		os.Exit(1)
	}
	tFormat := shared.YMDTimeFmt
	if m.Table.TimeQuantumType == "YMDH" {
		tFormat = shared.YMDHTimeFmt
	}
	partition := ts.Format(tFormat)

	out := make(map[string]interface{})

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

	if m.ShardCount > 1 && m.Collate {
		c := m.getPartition(partition)
		select {
		case c.Data <- out:
		}
	} else { // Bypass partition worker dispatching
		err := shared.Retry(3, 5 * time.Second, func() (err error) {
			err = m.processBatch([]map[string]interface{}{out}, partition)
			return
		})
		if err != nil {
			u.Errorf("processBatch ERROR %v", err)
			return nil // continue processing
		}
	}
	m.totalRecs.Add(1)
	m.totalBytes.Add(len(v.Data))

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

/*
	selectit := val.Value().(bool) 
	if selectit {
		u.Errorf("%#v - %#v", ctx, row)
		os.Exit(1)
	}
*/
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

func (m *Main) processBatch(rows []map[string]interface{}, partition string) error {

	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("Panic recover: \n" + string(debug.Stack()))
			u.Error(err)
		}
	}()

	conn, err := m.sessionPool.Borrow(m.Index)
	if err != nil {
		if err == core.ErrPoolDrained {
			return err
		}
		return fmt.Errorf("Error opening Quanta session %v", err)
	}
	defer m.sessionPool.Return(m.Index, conn)
	for i := 0; i < len(rows); i++ {
		err = conn.PutRow(m.Index, rows[i], 0, false, false)
		if err != nil {
			u.Errorf("ERROR in PutRow, partition %s - %v", partition, err)
			m.errorCount.Add(1)
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

func (m *Main) schemaChangeListener(e shared.SchemaChangeEvent) {

	core.ClearTableCache()
	switch e.Event {
	case shared.Drop:
		m.sessionPool.Recover(nil)
		u.Warnf("Dropped table %s", e.Table)
	case shared.Modify:
		u.Warnf("Truncated table %s", e.Table)
	case shared.Create:
		m.sessionPool.Recover(nil)
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
	clientConn.RegisterService(m)

	m.sessionPool = core.NewSessionPool(clientConn, nil, "", m.sessionPoolSize)

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
	u.Infof("Created consumer. ")

	return shardCount, nil
}

// Partition - A partition is a Quanta concept and defines an daily or hourly time segment.
type Partition struct {
	ModTime	  time.Time
	PartitionKey string
	Data		 chan map[string]interface{}
}

// NewPartition - Construct a new partition.
func NewPartition(partition string) *Partition {
	return &Partition{PartitionKey: partition, Data: make(chan map[string]interface{}, partitionChannelSize),
		ModTime: time.Now().UTC()}
}

// GetDataRows - Extract data rows from partition channel.  Number of rows are larger of outstanding data or batchSize.
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

// PutDataRows - Put batch back in channel for retry.
func (p *Partition) PutDataRows(rows []map[string]interface{}) {

	for i := 0; i < len(rows); i++ {
		select {
		case p.Data <- rows[i]:
		}
	}
}

// Lookup partition with the intent to write to it
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

	connPoolSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "connection_pool_size",
		Help: "The size of the Quanta session pool",
	})

	connInUse = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "connections_in_use",
		Help: "Number of Quanta sessions currently (actively) in use.",
	})

	connPooled = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "connections_in_pool",
		Help: "Number of Quanta sessions currently pooled.",
	})

	connMaxInUse = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "max_connections_in_use",
		Help: "Maximum nunber of Quanta sessions in use.",
	})
)

func (m *Main) publishMetrics(upTime time.Duration, lastPublishedAt time.Time) time.Time {

	connectionPoolSize, connectionsInUse, pooled, maxUsed := m.sessionPool.Metrics()
	interval := time.Since(lastPublishedAt).Seconds()
	_, err := m.metrics.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: aws.String("Quanta-Consumer/Records"),
		MetricData: []*cloudwatch.MetricDatum{
			{
				MetricName: aws.String("ConnectionPoolSize"),
				Unit:	   aws.String("Count"),
				Value:	  aws.Float64(float64(connectionPoolSize)),
			},
			{
				MetricName: aws.String("ConnectionsInUse"),
				Unit:	   aws.String("Count"),
				Value:	  aws.Float64(float64(connectionsInUse)),
			},
			{
				MetricName: aws.String("MaxConnectionsInUse"),
				Unit:	   aws.String("Count"),
				Value:	  aws.Float64(float64(maxUsed)),
			},
			{
				MetricName: aws.String("ConnectionsInPool"),
				Unit:	   aws.String("Count"),
				Value:	  aws.Float64(float64(pooled)),
			},
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
	connPoolSize.Set(float64(connectionPoolSize))
	connInUse.Set(float64(connectionsInUse))
	connMaxInUse.Set(float64(maxUsed))
	connPooled.Set(float64(pooled))

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


