package main

import (
	"context"
	"fmt"
	"github.com/araddon/dateparse"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
	pqs3 "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"golang.org/x/sync/errgroup"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"os"
	"os/signal"
	"path"
	"runtime"
	"sort"
	"strings"
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
	Index        string
	BufferSize   uint
	BucketPath   string
	Bucket       string
	Prefix       string
	Pattern      string
	AWSRegion    string
	S3svc        *s3.S3
	S3files      []*s3.Object
	totalBytes   int64
	bytesLock    sync.RWMutex
	totalRecs    *Counter
	Port         int
	IsNested     bool
	ConsulAddr   string
	ConsulClient *api.Client
	DateFilter   *time.Time
	lock         *api.Lock
}

// NewMain allocates a new pointer to Main struct with empty record counter
func NewMain() *Main {
	m := &Main{
		totalRecs: &Counter{},
	}
	return m
}

func main() {

	app := kingpin.New(os.Args[0], "Quanta S3 data loader").DefaultEnvars()
	app.Version("Version: " + Version + "\nBuild: " + Build)

	bucketName := app.Arg("bucket-path", "AWS S3 Bucket Name/Path (patterns ok) to read from via the data loader.").Required().String()
	index := app.Arg("index", "Table name (root name if nested schema)").Required().String()
	port := app.Arg("port", "Port number for service").Default("4000").Int32()
	region := app.Flag("aws-region", "AWS region of bitmap server host(s)").Default("us-east-1").String()
	bufSize := app.Flag("buf-size", "Import buffer size").Default("1000000").Int32()
	dryRun := app.Flag("dry-run", "Perform a dry run and exit (just print selected file names).").Bool()
	environment := app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	isNested := app.Flag("nested", "Input data is a nested schema. The <index> parameter is root.").Bool()
	dateFilter := app.Flag("date-filter", "If provided, all rows must be within this time partition.").String()
	consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()

	core.InitLogging("WARN", *environment, "Loader", Version, "Quanta")

	kingpin.MustParse(app.Parse(os.Args[1:]))

	main := NewMain()
	main.Index = *index
	main.BufferSize = uint(*bufSize)
	main.AWSRegion = *region
	main.Port = int(*port)
	main.IsNested = *isNested
	main.ConsulAddr = *consul

	log.Printf("Index name %v.\n", main.Index)
	log.Printf("Buffer size %d.\n", main.BufferSize)
	log.Printf("AWS region %s\n", main.AWSRegion)
	log.Printf("Service port %d.\n", main.Port)
	log.Printf("Consul agent at [%s]\n", main.ConsulAddr)
	if main.IsNested {
		log.Printf("Nested Mode.  Input data is a nested schema, Index <%s> should be the root.", main.Index)
	}

	if *dateFilter != "" {
		loc, _ := time.LoadLocation("Local")
		ts, err := dateparse.ParseIn(*dateFilter, loc)
		if err != nil {
			log.Fatalf("Date parse error for time partition filter %s.", *dateFilter)
		}
		sf := ts.Format(shared.YMDTimeFmt)
		tq, _ := time.Parse(shared.YMDTimeFmt, sf)
		main.DateFilter = &tq
		log.Printf("Filtering data for time partition %s.", sf)
	}

	if err := main.Init(); err != nil {
		log.Fatal(err)
	}

	main.BucketPath = *bucketName
	main.LoadBucketContents()

	log.Printf("S3 bucket %s contains %d files for processing.", main.BucketPath, len(main.S3files))

	threads := runtime.NumCPU()
	var eg errgroup.Group
	fileChan := make(chan *s3.Object, threads)
    closeLater := make([]*core.Connection, 0)
	var ticker *time.Ticker
	// Spin up worker threads
	if !*dryRun {
		for i := 0; i < threads; i++ {
			eg.Go(func() error {
				conn, err := core.OpenConnection("", main.Index, main.IsNested,
					main.BufferSize, main.Port, main.ConsulClient)
				if err != nil {
					return err
				}
				if main.DateFilter != nil {
					conn.SetDateFilter(main.DateFilter)
				}
				for file := range fileChan {
					main.processRowsForFile(file, conn)
				}
                closeLater = append(closeLater, conn)
				return nil
			})
		}
		ticker = main.printStats()
	} else {
		log.Println("Performing dry run.")
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Printf("Interrupted,  Bytes processed: %s", core.Bytes(main.BytesProcessed()))
			os.Exit(0)
		}
	}()

	selected := 0
	for i := 0; i < len(main.S3files); i++ {
		fileName := *main.S3files[i].Key
		if main.Prefix != "" {
			fileName = fileName[len(main.Prefix)+1:]
		}
		if fileName == "_SUCCESS" {
			continue
		}
		if ok, err := path.Match(main.Pattern, fileName); ((!ok && main.Pattern != "") || *main.S3files[i].Size == 0) && err == nil {
			continue
		} else if err != nil {
			log.Fatalf("Pattern error %v", err)
		}
		selected++
		log.Printf("Selected bucket import file %s.\n", fileName)
		if !*dryRun {
			fileChan <- main.S3files[i]
		}
	}

	if !*dryRun {
		close(fileChan)
		if err := eg.Wait(); err != nil {
			log.Fatalf("Open error %v", err)
		}
        for _, cc := range closeLater {
            cc.CloseConnection()
        }
		ticker.Stop()
		log.Printf("Completed, Last Record: %d, Bytes: %s", main.totalRecs.Get(), core.Bytes(main.BytesProcessed()))
		log.Printf("%d files processed.", selected)
	} else {
		log.Printf("%d files selected.", selected)
	}

}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

//
// LoadBucketContents - List S3 objects from AWS bucket based on command line argument of the bucket name
// S3 does not actually support nested buckets, instead they use a file prefix.
//
func (m *Main) LoadBucketContents() {

	m.Bucket = path.Dir(m.BucketPath)
	if m.Bucket == "." {
		m.Bucket = m.BucketPath
	}
	m.Pattern = path.Base(m.BucketPath)
	if m.Pattern == m.BucketPath {
		m.Pattern = ""
	}
	idx := strings.Index(m.Bucket, "/")
	if idx >= 0 {
		bucket := m.Bucket[:idx]
		m.Prefix = m.Bucket[idx+1:]
		m.Bucket = bucket
	}
	params := &s3.ListObjectsV2Input{Bucket: aws.String(m.Bucket)}
	if m.Prefix != "" {
		params.Prefix = aws.String(m.Prefix)
	}

	ret := make([]*s3.Object, 0)
	err := m.S3svc.ListObjectsV2Pages(params,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, v := range page.Contents {
				ret = append(ret, v)
			}
			return true
		},
	)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				log.Fatal(fmt.Errorf("%v %v", s3.ErrCodeNoSuchBucket, aerr.Error()))
			default:
				log.Fatal(aerr.Error())
			}
		} else {
			// Return the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Fatal(err.Error())
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		return *(ret[i].Size) > *(ret[j].Size)
	})
	m.S3files = ret
}

func (m *Main) processRowsForFile(s3object *s3.Object, dbConn *core.Connection) {

	pf, err1 := pqs3.NewS3FileReaderWithClient(context.Background(), m.S3svc, m.Bucket,
		*aws.String(*s3object.Key))
	if err1 != nil {
		log.Fatal(err1)
	}

	pr, err1 := reader.NewParquetColumnReader(pf, 4)
	if err1 != nil {
		log.Println("Can't create column reader", err1)
		return
	}
	defer pr.ReadStop()
	defer pf.Close()

	num := int(pr.GetNumRows())
	for i := 1; i <= num; i++ {
		var err error
		m.totalRecs.Add(1)
		err = dbConn.PutRow(m.Index, pr)
		if err != nil {
			// TODO: Improve this by logging into work queue for re-processing
			log.Println(err)
		}
		m.AddBytes(dbConn.BytesRead)
	}
    dbConn.Flush()
}

// Init function initilizations loader.
// Establishes session with bitmap server and AWS S3 client
func (m *Main) Init() error {

	var err error

	m.ConsulClient, err = api.NewClient(&api.Config{Address: m.ConsulAddr})
	if err != nil {
		return err
	}

	// Initialize S3 client
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(m.AWSRegion)},
	)

	if err != nil {
		return fmt.Errorf("Creating S3 session: %v", err)
	}

	// Create S3 service client
	m.S3svc = s3.New(sess)

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
