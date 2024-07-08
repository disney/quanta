package q_kinesis_lib

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	u "github.com/araddon/gou"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/qlbridge/datasource"
	"github.com/disney/quanta/qlbridge/expr"
	"github.com/disney/quanta/qlbridge/value"
	"github.com/disney/quanta/qlbridge/vm"
	"github.com/disney/quanta/shared"
	"github.com/hamba/avro/v2"
	consumer "github.com/harlow/kinesis-consumer"
	store "github.com/harlow/kinesis-consumer/store/ddb"
	"github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stvp/rendezvous"
	"golang.org/x/sync/errgroup"
)

const (
	Success          = 0 // Exit code for success
	AppName          = "Kinesis-Consumer"
	ShardChannelSize = 10000
)

// Main struct defines command line arguments variables and various global meta-data associated with record loads.
type Main struct {
	Stream      string
	Region      string
	Schema      string
	TotalBytes  *Counter
	totalBytesL *Counter
	TotalRecs   *Counter
	totalRecsL  *Counter
	errorCount  *Counter
	OpenSessions *Counter
	// poolPercent         *Counter
	Port       int
	ConsulAddr string
	ShardCount int
	// lock                *api.Lock
	Consumer            *consumer.Consumer
	InitialPos          string
	IsAvro              bool
	CheckpointDB        bool
	CheckpointTable     string
	AssumeRoleArn       string
	AssumeRoleArnRegion string
	Deaggregate         bool
	Collate             bool
	ShardKey            string
	HashTable           *rendezvous.Table
	shardChannels       map[string]chan DataRecord
	hwmChannels			map[string]chan bool
	isFlushing			sync.Map
	shardSessionCache   sync.Map
	eg                  errgroup.Group
	CancelFunc          context.CancelFunc
	processedRecs       *Counter
	processedRecL       *Counter
	ScanInterval        int
	metrics             *cloudwatch.CloudWatch
	tableCache          *core.TableCacheStruct
	metricsTicker       *time.Ticker
}

// NewMain allocates a new pointer to Main struct with empty record counter
func NewMain() *Main {
	m := &Main{
		TotalRecs:     &Counter{},
		totalRecsL:    &Counter{},
		TotalBytes:    &Counter{},
		totalBytesL:   &Counter{},
		processedRecs: &Counter{},
		processedRecL: &Counter{},
		errorCount:    &Counter{},
		OpenSessions:  &Counter{},
	}
	m.tableCache = core.NewTableCacheStruct()
	return m
}

type DataRecord struct {
	TableName string
	Data      map[string]interface{}
}

// Init function initilizations loader.
// Establishes session with bitmap server and Kinesis
func (m *Main) Init(customEndpoint string) (int, error) {

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

	clientConn := shared.NewDefaultConnection("kinesis-consumer")
	clientConn.ServicePort = m.Port
	clientConn.Quorum = 3
	if err := clientConn.Connect(consulClient); err != nil {
		u.Error(err)
		os.Exit(1)
	}

	// Register member leave/join
	//clientConn.RegisterService(m)
	sess, errx := session.NewSession(&aws.Config{
		Region:   aws.String(m.Region),
		Endpoint: aws.String(customEndpoint),
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

	db, err := store.New(AppName, m.CheckpointTable, store.WithDynamoClient(dynamoDbClient), store.WithRetryer(&QuantaRetryer{}))
	if err != nil {
		u.Errorf("checkpoint storage initialization error: %v", err)
		os.Exit(1)
	}

	if m.CheckpointDB {
		m.Consumer, err = consumer.New(
			m.Stream,
			consumer.WithClient(kc),
			consumer.WithShardIteratorType(m.InitialPos),
			consumer.WithStore(db),
			consumer.WithAggregation(m.Deaggregate),
			consumer.WithScanInterval(time.Duration(m.ScanInterval)*time.Millisecond),
			//consumer.WithCounter(counter),
		)
	} else {
		m.Consumer, err = consumer.New(
			m.Stream,
			consumer.WithClient(kc),
			consumer.WithShardIteratorType(m.InitialPos),
			consumer.WithAggregation(m.Deaggregate),
			consumer.WithScanInterval(time.Duration(m.ScanInterval)*time.Millisecond),
			//consumer.WithCounter(counter),
		)
	}
	if err != nil {
		return 0, err
	}
	m.metrics = cloudwatch.New(sess)

	// Initialize shard channels
	u.Warnf("Shard count = %d", shardCount)
	m.shardChannels = make(map[string]chan DataRecord)
	m.hwmChannels = make(map[string]chan bool)
	shardIds := make([]string, shardCount)
	// ClearTableCache
	for k := range m.tableCache.TableCache {
		delete(m.tableCache.TableCache, k)
	}

	// open all tables to populate the TableCache
	tables, err := shared.GetTables(consulClient)
	if err != nil {
		return 0, err
	}
	for _, tableName := range tables {
		u.Infof("Opening session for table %s", tableName)
		// what are these orphans sessions for?
		conn, _ := core.OpenSession(m.tableCache, "", tableName, true, clientConn)
		if conn != nil {
			conn.CloseSession()
		}
	}

	// we have to do this in two passes
	// we don't want to lookup shardChannels while the init is still writing that map.
	// 1.  Create the shard channels
	// 2.  Start the go routines

	for i := 0; i < shardCount; i++ {
		k := fmt.Sprintf("shard%v", i)
		shardIds[i] = k
		m.shardChannels[k] = make(chan DataRecord, ShardChannelSize)
		m.isFlushing.Store(k, false)
		m.hwmChannels[k] = make(chan bool, 1)
	}

	startedShardProcessing := time.Now()
	printedMessage := false
	for i := 0; i < shardCount; i++ {

		k := fmt.Sprintf("shard%v", i)
		// shardIds[i] = k
		// m.shardChannels[k] = make(chan DataRecord, ShardChannelSize)
		shardId := k
		theChan := m.shardChannels[k]
		hwmChan := m.hwmChannels[k]
		m.eg.Go(func() error {
			var shardTableKeys sync.Map
			for {
				select {
					case rec, open := <- theChan:
					if !open { 
						goto exitloop
					} 
					shardTableKey := fmt.Sprintf("%v+%v", shardId, rec.TableName)
					conn, ok := m.shardSessionCache.Load(shardTableKey)
					if !ok {
						conn, err = core.OpenSession(m.tableCache, "", rec.TableName, true, clientConn)
						if err != nil {
							return err
						}
						m.OpenSessions.Add(1)
						m.shardSessionCache.Store(shardTableKey, conn)
						shardTableKeys.Store(shardTableKey, conn)
					}

					// fmt.Printf("Kinesis PutRow %v %v %v\n", rec.TableName, rec.Data, shardId)
					err = conn.(*core.Session).PutRow(rec.TableName, rec.Data, 0, false, false)
	
					if err != nil {
						u.Errorf("ERROR in PutRow, shard %s - %v", shardId, err)
						m.errorCount.Add(1)
						return err
					}
					m.processedRecs.Add(1)
					case <- hwmChan:
						shardTableKeys.Range(func(k, v interface{}) bool {
							if v.(*core.Session).BatchBuffer.IsEmpty() {
								return true
							}
							start := time.Now()
							v.(*core.Session).Flush()
							u.Infof("FLUSH DURATION = %v, QUEUE LEN = %d", time.Since(start), len(theChan))
							return true
						})
					default:
						shardTableKeys.Range(func(k, v interface{}) bool {
							v.(*core.Session).CloseSession()
							shardTableKeys.Delete(k)
							m.shardSessionCache.Delete(k)
							m.OpenSessions.Add(-1)
							return true
						})
						time.Sleep(100 * time.Millisecond)
				}
				if !printedMessage && m.processedRecs.Get() > 0 && m.OpenSessions.Get() == 0 {
					u.Infof("FINISHED FLUSHING ALL SESSIONS IN %v !!!", time.Since(startedShardProcessing))
					printedMessage = true
				}
			}
			exitloop:
			u.Errorf("shard channel closed. %v", shardId)
			// sharedChannels was closed, clean up.
			m.shardSessionCache.Range(func(k, v interface{}) bool {
				v.(*core.Session).CloseSession()
				m.shardSessionCache.Delete(k)
				m.OpenSessions.Add(-1)
				return true
			})
			return nil
		})
	}
	m.HashTable = rendezvous.New(shardIds)
	m.metricsTicker = m.PrintStats()
	u.Infof("Created consumer. ")

	return shardCount, nil
}

// MainProcessingLoop function is the main processing loop for the Kinesis consumer.
func (m *Main) MainProcessingLoop() error {

	// // Main processing loop will continue forever until a SIGKILL is received.
	var ctx context.Context
	for {
		ctx, m.CancelFunc = context.WithCancel(context.Background())
		scanErr := m.Consumer.Scan(ctx, m.scanAndProcess)
		u.Debugf("kinesis scan returned: %v", scanErr)
		m.Destroy()
		if err := m.eg.Wait(); err != nil {
			u.Errorf("session error: %v", err)
		}
		if scanErr != nil {
			u.Errorf("scan error: %v", scanErr)
		} else {
			u.Warnf("Received Cancellation.")
		}
		if m.InitialPos == "TRIM_HORIZON" {
			u.Error("can't re-initialize 'in-place' if set to TRIM_HORIZON, exiting")
			// os.Exit(1)
			return fmt.Errorf("can't re-initialize 'in-place' if set to TRIM_HORIZON")
		}
		u.Warnf("Re-initializing.")
		var err error
		if m.ShardCount, err = m.Init(""); err != nil {
			u.Errorf("initialization error: %v", err)
			u.Errorf("Exiting process.")
			// os.Exit(1)
			return fmt.Errorf("initialization error: %v", err)
		}
	}
	// return nil
}

func (m *Main) Destroy() {

	if m.PrintStats != nil {
		m.metricsTicker.Stop()
	}
	m.CancelFunc = nil
	for _, v := range m.shardChannels {
		close(v)
	}
	for _, v := range m.hwmChannels {
		close(v)
	}
	time.Sleep(time.Second * 5) // Allow time for completion
}

func (m *Main) scanAndProcess(v *consumer.Record) error {

	out := make(map[string]interface{})
	var table *core.Table

	//u.Debugf("Kinesis scanAndProcess top %v\n", v)

	for _, x := range m.tableCache.TableCache {
		if x.SelectorNode == nil {
			continue
		}
		if m.IsAvro {
			errx := avro.Unmarshal(x.AvroSchema, v.Data, &out)
			if errx != nil {
				// Could fail for a number of reasons but most often the 'shape' of the data is different
				// Ideally we would grok the schema from the partition key somehow
				continue
			}

		} else { // Default is JSON
			err := json.Unmarshal(v.Data, &out)
			if err != nil {
				m.errorCount.Add(1)
				u.Errorf("Unmarshal ERROR %v", err)
				return nil
			}
		}
		if m.preselect(x.SelectorNode, x.SelectorIdentities, out) {
			table = x
			break
		}
	}
	if table == nil { // no match, continue
		return nil
	}

	// Got record at this point
	// push into the appropriate shard channel
	if key, err := shared.GetPath(m.ShardKey, out, false, false); err != nil {
		return err
	} else {
		shard := m.HashTable.GetN(1, key.(string))
		ch, ok := m.shardChannels[shard[0]]
		if !ok {
			return fmt.Errorf("cannot locate channel for shard key %v", key)
		}
		hwm, ok2 := m.hwmChannels[shard[0]]
		if !ok2 {
			return fmt.Errorf("cannot locate signal channel for shard key %v", key)
		}
		var isFlushing bool
		isf, ok3 := m.isFlushing.Load(shard[0])
		if !ok3 {
			return fmt.Errorf("cannot locate isFlushing flag for shard key %v", key)
		}
		isFlushing = isf.(bool)
		rec := DataRecord{TableName: table.Name, Data: out}
		// fmt.Println("Pushing record to channel", rec, shard[0])
		select {
		case ch <- rec:
			if !isFlushing && len(ch) > int(ShardChannelSize * .9) {
				u.Infof("HWM REACHED %s QUEUE LEN = %d", shard[0], len(ch))
				select {
					case hwm <- true:
						m.isFlushing.Store(shard[0], true)
					default:
				}
			}
			if isFlushing && len(ch) < int(ShardChannelSize * .2) {
				u.Infof("LWM REACHED %s QUEUE LEN = %d", shard[0], len(ch))
				m.isFlushing.Store(shard[0], false)
			}
		}
		m.TotalRecs.Add(1)
		m.TotalBytes.Add(len(v.Data))
		totalBytes.Add(float64(len(v.Data))) // tell prometheus

	}
	return nil // continue scanning
}

// filter row per expression
func (m *Main) preselect(selector expr.Node, identities []string, row map[string]interface{}) bool {

	ctx := m.buildEvalContext(identities, row)
	if ctx == nil {
		return false
	}
	val, ok := vm.Eval(ctx, selector)
	if !ok {
		u.Errorf("Preselect expression %s failed to evaluate ", selector.String())
		os.Exit(1)
	}
	if val.Type() != value.BoolType {
		u.Errorf("select expression %s does not evaluate to a boolean value", selector.String())
		os.Exit(1)
	}
	return val.Value().(bool)
}

func (m *Main) buildEvalContext(identities []string, row map[string]interface{}) *datasource.ContextSimple {

	data := make(map[string]interface{})
	for _, v := range identities {
		var path string
		if v[0] == '/' {
			path = v[1:]
		} else {
			path = v
		}
		if l, err := shared.GetPath(path, row, false, false); err == nil {
			data[v] = l
		} else {
			return nil
		}
	}
	return datasource.NewContextSimpleNative(data)
}

func (m *Main) schemaChangeListener(e shared.SchemaChangeEvent) {

	if m.CancelFunc != nil {
		m.CancelFunc()
	}
	switch e.Event {
	case shared.Drop:
		//m.sessionPool.Recover(nil)
		u.Warnf("Dropped table %s", e.Table)
		delete(m.tableCache.TableCache, e.Table)
	case shared.Modify:
		u.Warnf("Truncated table %s", e.Table)
	case shared.Create:
		//m.sessionPool.Recover(nil)
		delete(m.tableCache.TableCache, e.Table)
		u.Warnf("Created table %s", e.Table)
	}
	m.shardSessionCache.Range(func(k, v interface{}) bool {
		v.(*core.Session).CloseSession()
		m.shardSessionCache.Delete(k)
		return true
	})
}

// printStats outputs to Log current status of Kinesis consumer
// Includes data on processed: bytes, records, time duration in seconds, and rate of bytes per sec"
func (m *Main) PrintStats() *time.Ticker {
	t := time.NewTicker(time.Second * 10)
	start := time.Now()
	lastTime := time.Now()
	go func() {
		for range t.C {
			duration := time.Since(start)
			bytes := m.TotalBytes.Get()
			u.Infof("Bytes: %s, Records: %v, Processed: %v, Errors: %v, Duration: %v, Rate: %v/s",
				core.Bytes(bytes), m.TotalRecs.Get(), m.processedRecs.Get(), m.errorCount.Get(), duration,
				(float64(m.processedRecs.Get())/duration.Seconds()))
				//core.Bytes(float64(bytes)/duration.Seconds()))
			lastTime = m.publishMetrics(duration, lastTime)
		}
	}()
	return t
}

func (m *Main) publishMetrics(upTime time.Duration, lastPublishedAt time.Time) time.Time {

	interval := time.Since(lastPublishedAt).Seconds()
	_, err := m.metrics.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: aws.String("Quanta-Consumer/Records"),
		MetricData: []*cloudwatch.MetricDatum{
			{
				MetricName: aws.String("Arrived"),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(float64(m.TotalRecs.Get())),
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  aws.String("Stream"),
						Value: aws.String(m.Stream),
					},
				},
			},
			{
				MetricName: aws.String("RecordsPerSec"),
				Unit:       aws.String("Count/Second"),
				Value:      aws.Float64(float64(m.TotalRecs.Get()-m.totalRecsL.Get()) / interval),
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  aws.String("Stream"),
						Value: aws.String(m.Stream),
					},
				},
			},
			{
				MetricName: aws.String("Processed"),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(float64(m.processedRecs.Get())),
				/*
					Dimensions: []*cloudwatch.Dimension{
						{
							Name:  aws.String("Table"),
							Value: aws.String(m.Index),
						},
					},
				*/
			},
			{
				MetricName: aws.String("ProcessedPerSecond"),
				Unit:       aws.String("Count/Second"),
				Value:      aws.Float64(float64(m.processedRecs.Get()-m.processedRecL.Get()) / interval),
				/*
					Dimensions: []*cloudwatch.Dimension{
						{
							Name:  aws.String("Table"),
							Value: aws.String(m.Index),
						},
					},
				*/
			},
			{
				MetricName: aws.String("Errors"),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(float64(m.errorCount.Get())),
				/*
					Dimensions: []*cloudwatch.Dimension{
						{
							Name:  aws.String("Table"),
							Value: aws.String(m.Index),
						},
					},
				*/
			},
			{
				MetricName: aws.String("ProcessedBytes"),
				Unit:       aws.String("Bytes"),
				Value:      aws.Float64(float64(m.TotalBytes.Get())),
				/*
					Dimensions: []*cloudwatch.Dimension{
						{
							Name:  aws.String("Table"),
							Value: aws.String(m.Index),
						},
					},
				*/
			},
			{
				MetricName: aws.String("BytesPerSec"),
				Unit:       aws.String("Bytes/Second"),
				Value:      aws.Float64(float64(m.TotalBytes.Get()-m.totalBytesL.Get()) / interval),
				/*
					Dimensions: []*cloudwatch.Dimension{
						{
							Name:  aws.String("Table"),
							Value: aws.String(m.Index),
						},
					},
				*/
			},
			{
				MetricName: aws.String("UpTimeHours"),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(float64(upTime) / float64(1000000000*3600)),
				/*
					Dimensions: []*cloudwatch.Dimension{
						{
							Name:  aws.String("Table"),
							Value: aws.String(m.Index),
						},
					},
				*/
			},
		},
	})
	// Set Prometheus values
	totalRecs.Set(float64(m.TotalRecs.Get()))
	totalRecsPerSec.Set(float64(m.TotalRecs.Get()-m.totalRecsL.Get()) / interval)
	processedRecs.Set(float64(m.processedRecs.Get()))
	processedRecsPerSec.Set(float64(m.processedRecs.Get()-m.processedRecL.Get()) / interval)
	errors.Set(float64(m.errorCount.Get()))
	processedBytes.Set(float64(m.TotalBytes.Get()))
	processedBytesPerSec.Set(float64(m.TotalBytes.Get()-m.totalBytesL.Get()) / interval)
	uptimeHours.Set(float64(upTime) / float64(1000000000*3600))

	m.totalRecsL.Set(m.TotalRecs.Get())
	m.processedRecL.Set(m.processedRecs.Get())
	m.totalBytesL.Set(m.TotalBytes.Get())
	if err != nil {
		u.Error(err)
	}
	return time.Now()
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
		Name: "uptime_hours_kinesis",
		Help: "Hours of up time",
	})
)

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
