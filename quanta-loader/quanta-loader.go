package main

import (
	"context"
	"fmt"

	"errors"
	"log"
	"os"
	"os/signal"
	"path"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
	pgs3 "github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/reader"
	"golang.org/x/sync/errgroup"
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
	Success = 0
)

// Main strct defines command line arguments variables and various global meta-data associated with record loads.
type Main struct {
	Index              string
	BufferSize         uint
	BucketPath         string
	Bucket             string
	Prefix             string
	Pattern            string
	AWSRegion          string
	S3svc              *s3.Client
	S3files            []types.Object
	AssumeRoleArn      string
	Acl                string
	SseKmsKeyID        string
	totalBytes         int64
	bytesLock          sync.RWMutex
	totalRecs          *Counter
	Port               int
	IsNested           bool
	ConsulAddr         string
	ConsulClient       *api.Client
	DateFilter         *time.Time
	lock               *api.Lock
	apiHost            *shared.Conn
	ignoreSourcePath   bool
	nerdCapitalization bool
	tableCache         *core.TableCacheStruct
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
	assumeRoleArn := app.Flag("role-arn", "AWS role to assume for bucket access").String()
	acl := app.Flag("acl", "AWS bucket acl - usually bucket-owner-full-control").Default("bucket-owner-full-control").String()
	SseKmsKeyID := app.Flag("kms-key-id", "AWS key to use for this bucket").String()
	region := app.Flag("aws-region", "AWS region of bitmap server host(s)").Default("us-east-1").String()
	bufSize := app.Flag("buf-size", "Import buffer size").Default("1000000").Int32()
	dryRun := app.Flag("dry-run", "Perform a dry run and exit (just print selected file names).").Bool()
	environment := app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	isNested := app.Flag("nested", "Input data is a nested schema. The <index> parameter is root.").Bool()
	consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()
	ignoreSourcePath := app.Flag("ignore-source-path", "Ignore the source path into the parquet file.").Bool()
	nerdCapitalization := app.Flag("nerd-capitalization", "For parquet, field names are capitalized.").Bool()

	shared.InitLogging("WARN", *environment, "Loader", Version, "Quanta")

	kingpin.MustParse(app.Parse(os.Args[1:]))

	main := NewMain()
	main.Index = *index
	main.BufferSize = uint(*bufSize)
	main.AWSRegion = *region
	main.Port = int(*port)
	main.IsNested = *isNested
	main.ConsulAddr = *consul
	main.AssumeRoleArn = *assumeRoleArn
	main.Acl = *acl
	main.SseKmsKeyID = *SseKmsKeyID
	main.ignoreSourcePath = *ignoreSourcePath
	main.nerdCapitalization = *nerdCapitalization

	log.Printf("Index name %v.\n", main.Index)
	log.Printf("Buffer size %d.\n", main.BufferSize)
	log.Printf("AWS region %s\n", main.AWSRegion)
	log.Printf("Service port %d.\n", main.Port)
	log.Printf("Consul agent at [%s]\n", main.ConsulAddr)
	log.Printf("Assume Role Arn %s\n", main.AssumeRoleArn)
	log.Printf("Acl %s\n", main.Acl)
	log.Printf("Kms Key Id %s\n", main.SseKmsKeyID)
	if main.IsNested {
		log.Printf("Nested Mode.  Input data is a nested schema, Index <%s> should be the root.", main.Index)
	}
	if main.ignoreSourcePath {
		log.Printf("Ignoring the source path info in the config file. Using column names and assuming root.")
	}
	if main.nerdCapitalization {
		log.Printf("Column names are capitalized in the source file, normalizing to lower case.")
	}

	if err := main.Init(); err != nil {
		log.Fatal(err)
	}

	main.BucketPath = *bucketName
	main.LoadBucketContents()

	log.Printf("S3 bucket %s contains %d files for processing.", main.BucketPath, len(main.S3files))

	threads := runtime.NumCPU()
	var eg errgroup.Group
	fileChan := make(chan *types.Object, threads)
	closeLater := make([]*core.Session, 0)
	var ticker *time.Ticker
	// Spin up worker threads
	if !*dryRun {
		for i := 0; i < threads; i++ {
			eg.Go(func() error {
				conn, err := core.OpenSession(main.tableCache, "", main.Index, main.IsNested, main.apiHost)
				if err != nil {
					return err
				}
				for file := range fileChan {
					main.processRowsForFile(*file, conn)
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
		if ok, err := path.Match(main.Pattern, fileName); ((!ok && main.Pattern != "") || main.S3files[i].Size == 0) && err == nil {
			continue
		} else if err != nil {
			log.Fatalf("Pattern error %v", err)
		}
		selected++
		log.Printf("Selected bucket import file %s.\n", fileName)
		if !*dryRun {
			fileChan <- &main.S3files[i]
		}
	}

	if !*dryRun {
		close(fileChan)
		for _, cc := range closeLater {
			cc.CloseSession()
		}
		if err := eg.Wait(); err != nil {
			log.Fatalf("Open error %v", err)
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

// LoadBucketContents - List S3 objects from AWS bucket based on command line argument of the bucket name
// S3 does not actually support nested buckets, instead they use a file prefix.
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
	params := &s3.ListObjectsV2Input{Bucket: awsv2.String(m.Bucket)}
	if m.Prefix != "" {
		params.Prefix = awsv2.String(m.Prefix)
	}

	paginator := s3.NewListObjectsV2Paginator(m.S3svc, params, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.Limit = 10
	})

	var err error
	var output *s3.ListObjectsV2Output
	ret := make([]types.Object, 0)
	pageNum := 0
	for paginator.HasMorePages() && pageNum < 3 {
		output, err = paginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("error: %v", err)
			return
		}
		for _, value := range output.Contents {
			ret = append(ret, value)
		}
		pageNum++
	}

	if err != nil {
		var bne *types.NoSuchBucket
		if errors.As(err, &bne) {
			log.Fatal(fmt.Errorf("%v %v", *bne, err.Error()))
		} else {
			log.Fatal(err.Error())
		}
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Size > ret[j].Size
	})
	m.S3files = ret
}

func (m *Main) processRowsForFile(s3object types.Object, dbConn *core.Session) {

	pf, err1 := pgs3.NewS3FileReaderWithClient(context.Background(), m.S3svc, m.Bucket,
		*awsv2.String(*s3object.Key))
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
		err = dbConn.PutRow(m.Index, pr, 0, m.ignoreSourcePath, m.nerdCapitalization)
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

	consul, err := api.NewClient(&api.Config{Address: m.ConsulAddr})
	if err != nil {
		return err
	}

	m.apiHost = shared.NewDefaultConnection("loader")
	m.apiHost.ServicePort = m.Port
	m.apiHost.Quorum = 3
	if err = m.apiHost.Connect(consul); err != nil {
		log.Fatal(err)
	}

	// Initialize S3 client
	cfg, err := config.LoadDefaultConfig(context.TODO())

	if err != nil {
		return fmt.Errorf("Creating S3 session: %v", err)
	}

	if m.AssumeRoleArn != "" {
		client := sts.NewFromConfig(cfg)
		provider := stscreds.NewAssumeRoleProvider(client, m.AssumeRoleArn)

		if err != nil {
			return fmt.Errorf("Failed to retrieve credentials: %v", err)
		}

		cfg.Credentials = awsv2.NewCredentialsCache(provider)
		_, err = cfg.Credentials.Retrieve(context.TODO())
		if err != nil {
			return fmt.Errorf("Failed to retrieve credentials from cache: %v", err)
		}

		m.S3svc = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.Region = m.AWSRegion
			o.Credentials = provider
			o.RetryMaxAttempts = 10
		})
	} else {
		m.S3svc = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.Region = m.AWSRegion
			o.RetryMaxAttempts = 10
		})
	}

	if m.S3svc == nil {
		return fmt.Errorf("Failed creating S3 session.")
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
