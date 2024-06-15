package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	_ "runtime"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
	"golang.org/x/sync/errgroup"
	"gopkg.in/alecthomas/kingpin.v2"
)

// Variables to identify the build
var (
	Version  string
	Build    string
	EPOCH, _ = time.ParseInLocation(time.RFC3339, "2000-01-01T00:00:00+00:00", time.UTC)
	loc, _   = time.LoadLocation("Local")
)

// Exit Codes
const (
	Success = 0
)

// Main strct defines command line arguments variables and various global meta-data associated with record loads.
type Main struct {
	Index        string
	BatchSize    int
	Path         string
	File         string
	Prefix       string
	Pattern      string
	AWSRegion    string
	//S3svc        *s3.S3
	//S3files      []*s3.Object
	totalBytes   int64
	bytesLock    sync.RWMutex
	totalRecs    *Counter
	failedRecs   *Counter
	Stream       string
	IsNested     bool
	ConsulAddr   string
	ConsulClient *api.Client
	Table        *shared.BasicTable
	outClient    *kinesis.Kinesis
	lock         *api.Lock
	partitionCol *shared.BasicAttribute
}

// NewMain allocates a new pointer to Main struct with empty record counter
func NewMain() *Main {
	m := &Main{
		totalRecs:  &Counter{},
		failedRecs: &Counter{},
	}
	return m
}

func main() {

	app := kingpin.New(os.Args[0], "Quanta TPC-H Kinesis Data Producer").DefaultEnvars()
	app.Version("Version: " + Version + "\nBuild: " + Build)

	filePath := app.Arg("file-path", "Path to TPC-H data files directory.").Required().String()
	index := app.Arg("index", "Table name.").Required().String()
	stream := app.Arg("stream", "Kinesis stream name.").Required().String()
	region := app.Flag("aws-region", "AWS region.").Default("us-east-1").String()
	batchSize := app.Flag("batch-size", "PutRecords batch size").Default("100").Int32()
	environment := app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()

	shared.InitLogging("WARN", *environment, "TPC-H-Producer", Version, "Quanta")

	kingpin.MustParse(app.Parse(os.Args[1:]))

	main := NewMain()
	main.Index = *index
	main.BatchSize = int(*batchSize)
	main.AWSRegion = *region
	main.Stream = *stream
	main.ConsulAddr = *consul

	log.Printf("Table name %v.\n", main.Index)
	log.Printf("Batch size %d.\n", main.BatchSize)
	log.Printf("AWS region %s\n", main.AWSRegion)
	log.Printf("Kinesis stream  %s.\n", main.Stream)
	log.Printf("Consul agent at [%s]\n", main.ConsulAddr)

	if err := main.Init(); err != nil {
		log.Fatal(err)
	}

	main.Path = *filePath

	var eg errgroup.Group
	var ticker *time.Ticker
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Printf("Interrupted,  Bytes processed: %s", core.Bytes(main.BytesProcessed()))
			os.Exit(0)
		}
	}()

	fullPath := fmt.Sprintf("%s/%s.tbl", main.Path, main.Index)
	readFile, err := os.Open(fullPath)
    if err != nil {
		log.Fatalf("Open error %v", err)
    }

	ticker = main.printStats()

	main.processRowsForFile(readFile)

    readFile.Close()

	if err := eg.Wait(); err != nil {
		log.Fatalf("Open error %v", err)
	}
	ticker.Stop()
	log.Printf("Completed, Last Record: %d, Bytes: %s", main.totalRecs.Get(), core.Bytes(main.BytesProcessed()))

}


func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}


func (m *Main) processRowsForFile(readFile *os.File) {

    fileScanner := bufio.NewScanner(readFile)
    fileScanner.Split(bufio.ScanLines)

	putBatch := make([]*kinesis.PutRecordsRequestEntry, 0)

	i := 0
    for fileScanner.Scan() {
		// Split the pipe delimited text line
		s := strings.Split(fileScanner.Text(), "|")
		i++

		// Marshal to JSON
		outData, _ := m.generateJSON(s)

		partitionKey := s[0]
		
		putBatch = append(putBatch, &kinesis.PutRecordsRequestEntry{
			Data:         outData,
			PartitionKey: aws.String(partitionKey),
		})

		if i%m.BatchSize == 0 {
			// put data to stream
			putOutput, err := m.outClient.PutRecords(&kinesis.PutRecordsInput{
				Records:    putBatch,
				StreamName: aws.String(m.Stream),
			})
			if err != nil {
				log.Println(err)
				continue
			}
			m.failedRecs.Add(int(*putOutput.FailedRecordCount))
			putBatch = make([]*kinesis.PutRecordsRequestEntry, 0)
		}

		m.totalRecs.Add(1)
		m.AddBytes(len(outData))
    }

	if len(putBatch) > 0 {
		putOutput, err := m.outClient.PutRecords(&kinesis.PutRecordsInput{
			Records:    putBatch,
			StreamName: aws.String(m.Stream),
		})
		if err != nil {
			log.Println(err)
		}
		m.failedRecs.Add(int(*putOutput.FailedRecordCount))
	}
}

// Generate JSON Payload
func (m *Main) generateJSON(fields []string) ([]byte, error) {

	env := make(map[string]interface{}, 0)
	data := make(map[string]interface{}, 0)

    for _, v := range m.Table.Attributes {
		if v.SourceOrdinal > 0 {
			data[v.FieldName] = fields[v.SourceOrdinal - 1]
			if m.partitionCol.FieldName == v.FieldName {
				env["shardKey"] = fields[v.SourceOrdinal - 1]
			}
		}
	}
	env["data"] = data
	env["type"] = m.Index
	return json.Marshal(env)
}

// Init function initializes process.
func (m *Main) Init() error {

	var err error

	m.ConsulClient, err = api.NewClient(&api.Config{Address: m.ConsulAddr})
	if err != nil {
		return err
	}

	m.Table, err = shared.LoadSchema("", m.Index, m.ConsulClient)
	if err != nil {
		return err
	}

	pkInfo, errx := m.Table.GetPrimaryKeyInfo()
	if errx != nil {
		return errx
	}
	m.partitionCol = pkInfo[0]

	// Initialize AWS client
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(m.AWSRegion)},
	)

	if err != nil {
		return fmt.Errorf("error creating S3 session: %v", err)
	}

	m.outClient = kinesis.New(sess)
	outStreamName := aws.String(m.Stream)
	_, err = m.outClient.DescribeStream(&kinesis.DescribeStreamInput{StreamName: outStreamName})
	if err != nil {
		return fmt.Errorf("error creating kinesis stream %s: %v", m.Stream, err)
	}

	return nil
}

// printStats outputs to Log current status of loader
// Includes data on processed: bytes, records, time duration in seconds, and rate of bytes per sec"
func (m *Main) printStats() *time.Ticker {
	t := time.NewTicker(time.Second * 10)
	start := time.Now()
	go func() {
		for range t.C {
			duration := time.Since(start)
			bytes := m.BytesProcessed()
			log.Printf("Bytes: %s, Records: %v, Failed: %v, Duration: %v, Rate: %v/s", core.Bytes(bytes), m.totalRecs.Get(), m.failedRecs.Get(), duration, core.Bytes(float64(bytes)/duration.Seconds()))
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

