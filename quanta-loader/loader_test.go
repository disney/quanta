package main

import (
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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

	// Return a function to teardown the test
	return func(tb testing.T) {
		fmt.Println("teardown suite")
	}
}

func notTestVersionBuild(t *testing.T) {
	if len(Version) <= 0 {
		t.Errorf("Version string length was empty, zero or less; got: %s", Version)
	}
	if len(Build) <= 0 {
		t.Errorf("Build string length was empty, zero or less; got: %s", Build)
	}
}

var _ = notTestVersionBuild

func TestLocalS3(t *testing.T) {

	teardownSuite := setupSuite(*t)
	defer teardownSuite(*t)

	session, err := session.NewSession()
	if err != nil {
		t.Errorf("Error creating session: %s", err)
	}
	session.Config.WithEndpoint("http://localhost:4566")
	session.Config.WithS3ForcePathStyle(true) // very important

	S3svc := s3.New(session, &aws.Config{Region: aws.String("us-east-1")})

	buckets, err := S3svc.ListBuckets(nil)
	if err != nil {
		t.Errorf("Error listing buckets: %s", err)
	}
	fmt.Printf("Buckets: %v", buckets)

	bucketout, err := S3svc.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String("quanta-test-data")})
	if err != nil {
		t.Errorf("Error creating bucket: %s", err)
		fmt.Println("Error creating bucket:", err.Error())
	}
	fmt.Printf("Bucketout: %v", bucketout)

	buckets, err = S3svc.ListBuckets(nil)
	if err != nil {
		t.Errorf("Error listing buckets: %s", err)
	}
	for _, bucket := range buckets.Buckets {
		fmt.Printf("Bucket: %s", *bucket.Name)
	}

	putObjectInput := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader("../test/testdata/us_cityzip")),
		Bucket: aws.String("quanta-test-data"),
		Key:    aws.String("us_cityzip"),
		Metadata: map[string]*string{
			"metadata1": aws.String("value1"),
			"metadata2": aws.String("value2"),
		},
	}

	putout, err := S3svc.PutObject(putObjectInput)
	if err != nil {
		t.Errorf("Error putting object: %s", err)
	}
	fmt.Println("PutObjectOutput: ", putout)
}

func TestInitLoaderMain(t *testing.T) {

	teardownSuite := setupSuite(*t)
	defer teardownSuite(*t)

	m0, _ := test.StartNodes(0, 1) // from localCluster/local-cluster-main.go
	m1, _ := test.StartNodes(1, 1)
	m2, _ := test.StartNodes(2, 1)

	defer func() {
		m0.Stop <- true
		m1.Stop <- true
		m2.Stop <- true
	}()

	// this is too slow
	//fmt.Println("Waiting for nodes to start...", m2.State)
	for m0.State != server.Active || m1.State != server.Active || m2.State != server.Active {
		time.Sleep(100 * time.Millisecond)
		//fmt.Println("Waiting for nodes...", m2.State)
	}
	//fmt.Println("done Waiting for nodes to start...", m2.State)

	main := NewMain()
	main.AWSRegion = "us-east-1"
	main.Port = 4000
	// main.BucketPath = "quanta-test-data/2017/01/01/00/00/00"
	main.ConsulAddr = "localhost:8500"

	session, err := session.NewSession()
	if err != nil {
		t.Errorf("Error creating session: %s", err)
	}
	session.Config.WithEndpoint("http://localhost:4566")
	session.Config.WithS3ForcePathStyle(true)
	session.Config.WithRegion(main.AWSRegion)

	err = main.Init("quanta-node", session)
	if err != nil {
		t.Errorf("Error initializing main: %s", err)
	}
	main.BucketPath = "quanta-test-data"
	main.LoadBucketContents()

	log.Printf("S3 bucket %s contains %d files for processing.", main.BucketPath, len(main.S3files))

}
