package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/test"
)

/** For these tests there must be a local consul running on localhost:8500
 *  and a local s3 running on localhost:4566

	We could use a suite but for now we'll want to run each test individually in debug mode

*/

// You can use testing.T, if you want to test the code without benchmarking
func setupSuite(tb testing.T) func(tb testing.T) {
	fmt.Println("setup suite")

	// TODO: put StartNode in here

	// add the S3 setup here

	// Return a function to teardown the test
	return func(tb testing.T) {
		fmt.Println("teardown suite")
		// put Stop >- true in here
	}
}

func notTestVersionBuild(t *testing.T) {
	// if len(Version) <= 0 {
	// 	t.Errorf("Version string length was empty, zero or less; got: %s", Version)
	// }
	// if len(Build) <= 0 {
	// 	t.Errorf("Build string length was empty, zero or less; got: %s", Build)
	// }
}

var _ = notTestVersionBuild

type customResolver struct {
}

func (res customResolver) ResolveEndpoint(service, region string) (awsv2.Endpoint, error) {
	if service == s3.ServiceID {
		return awsv2.Endpoint{
			PartitionID:       "aws",
			URL:               "http://localhost:4566",
			SigningRegion:     "us-east-1",
			HostnameImmutable: true,
		}, nil
	}
	return awsv2.Endpoint{}, &awsv2.EndpointNotFoundError{}
}

type myCredentialsProvider struct {
	// Retrieve returns nil if it successfully retrieved the value.
	// Error is returned if the value were not obtainable, or empty.
}

func (p myCredentialsProvider) Retrieve(ctx context.Context) (awsv2.Credentials, error) {
	return awsv2.Credentials{
		AccessKeyID:     "test",
		SecretAccessKey: "test-localstack-don-t-care",
	}, nil
}

func TestLocalS3(t *testing.T) {

	teardownSuite := setupSuite(*t)
	defer teardownSuite(*t)

	// session, err := session.NewSession()
	// if err != nil {
	// 	t.Errorf("Error creating session: %s", err)
	// }
	// session.Config.WithEndpoint("http://localhost:4566")
	// session.Config.WithS3ForcePathStyle(true) // very important

	resolver := customResolver{}
	credProvider := myCredentialsProvider{}

	cfg := awsv2.Config{
		Region: "us-east-1",
		//Endpoint: "http://localhost:4566",
		EndpointResolver: resolver,
		Credentials:      credProvider,
	}

	S3svc := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	bin := &s3.ListBucketsInput{}
	buckets, err := S3svc.ListBuckets(context.TODO(), bin)
	if err != nil {
		t.Errorf("Error listing buckets: %s", err)
	}
	_ = buckets
	fmt.Printf("Buckets: %v", buckets)

	bucketout, err := S3svc.CreateBucket(context.TODO(), &s3.CreateBucketInput{Bucket: aws.String("quanta-test-data")})
	if err != nil {
		t.Errorf("Error creating bucket: %s", err)
		fmt.Println("Error creating bucket:", err.Error())
	}
	_ = bucketout
	// fmt.Printf("Bucketout: %v", bucketout)

	buckets, err = S3svc.ListBuckets(context.TODO(), bin)
	if err != nil {
		t.Errorf("Error listing buckets: %s", err)
	}
	_ = buckets
	for _, bucket := range buckets.Buckets {
		fmt.Printf("Bucket: %s", *bucket.Name)
	}

	putObjectInput := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("../test/testdata/us_cityzip")),
		Bucket: aws.String("quanta-test-data"),
		Key:    aws.String("us_cityzip"),
	}

	putout, err := S3svc.PutObject(context.TODO(), putObjectInput)
	if err != nil {
		t.Errorf("Error putting object: %s", err)
	}
	fmt.Println("\nPutObjectOutput: ", putout)
}

func TestInitLoaderMain(t *testing.T) {

	TestLocalS3(t) // populate s3

	teardownSuite := setupSuite(*t)
	defer teardownSuite(*t)

	weStartedTheCluster := false
	// check to see if the cluster is running already
	// if not, start it

	result := "[]"
	res, err := http.Get("http://localhost:8500/v1/health/service/quanta-node")
	if err == nil {
		resBody, err := io.ReadAll(res.Body)
		if err == nil {
			result = string(resBody)
		}
	} else {
		fmt.Println("is consul not running?")
	}
	fmt.Println("result:", result)

	isNotRunning := strings.HasPrefix(result, "[]") || strings.Contains(result, "critical")

	var m0, m1, m2 *server.Node
	if isNotRunning {
		weStartedTheCluster = true
		m0, _ = test.StartNodes(0, 1) // from localCluster/local-cluster-main.go
		m1, _ = test.StartNodes(1, 1)
		m2, _ = test.StartNodes(2, 1)
		// this is too slow
		//fmt.Println("Waiting for nodes to start...", m2.State)
		for m0.State != server.Active || m1.State != server.Active || m2.State != server.Active {
			time.Sleep(100 * time.Millisecond)
			//fmt.Println("Waiting for nodes...", m2.State)
		}
		//fmt.Println("done Waiting for nodes to start...", m2.State
	}
	defer func() {
		if weStartedTheCluster {
			m0.Stop <- true
			m1.Stop <- true
			m2.Stop <- true
		}
	}()

	// start up proxy.
	test.StartProxy(1)

	main := NewMain()
	main.AWSRegion = "us-east-1"
	main.Port = 4000
	main.ConsulAddr = "localhost:8500"

	resolver := customResolver{}
	credProvider := myCredentialsProvider{}

	cfg := awsv2.Config{
		Region: main.AWSRegion,
		//Endpoint: "http://localhost:4566",
		EndpointResolver: resolver,
		Credentials:      credProvider,
	}

	// session.Config.WithEndpoint("http://localhost:4566")
	// session.Config.WithS3ForcePathStyle(true)
	// session.Config.WithRegion(main.AWSRegion)

	err = main.Init("quanta-node", &cfg)
	if err != nil {
		t.Errorf("Error initializing main: %s", err)
	}
	main.BucketPath = "quanta-test-data"
	main.LoadBucketContents()

	if len(main.S3files) != 1 {
		t.Errorf("Error loading bucket , wanted 1: got %v", len(main.S3files))
	}

	log.Printf("S3 bucket %s contains %d files for processing.", main.BucketPath, len(main.S3files))

	// file := main.S3files[0]
	// finish me  main.processRowsForFile(file, conn)

}
