package q_efo_kinesis_lib

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	types "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/disney/quanta/qlbridge/expr"
	"github.com/disney/quanta/test"
	consumer "github.com/harlow/kinesis-consumer"
	"github.com/stretchr/testify/suite"
)

// Consul in a terminal window must be ON.
// Localstack must be running. See the README.md in this directory.

const stream = "test-stream"
const region = "us-east-1"
const shardCount = 4
const consumerName = "test-efo-consumer"

// THIS is the entry point for the suite
func TestEntrypoint(t *testing.T) {
	ourSuite := new(Kinesis_test_struct)
	suite.Run(t, ourSuite)
}

// TestOne Send some rows to a kinesis stream. NOT the entry point.
func (suite *Kinesis_test_struct) TestOne() {

	test.AnalyzeRow(*suite.state.ProxyConnect, []string{"quanta-admin drop customers_qa"}, true)
	test.AnalyzeRow(*suite.state.ProxyConnect, []string{"quanta-admin create  --schema-dir=../sqlrunner/config customers_qa"}, true)

	is_localstack := os.Getenv("LOCALSTACK_ENV") == "true"
	fmt.Println("is_localstack", is_localstack)

	partionKey := "what_is_this" // an AWS feature?

	cfg, errx := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL: "http://localhost:4566",
				}, nil
			})),
	)
	check(errx)

	client := kinesis.NewFromConfig(cfg)

	streamName := aws.String(stream)

	streamAlreadyExists := true
	_, err := client.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: streamName})
	tmpstr := fmt.Sprintf("%s", err)
	if strings.HasPrefix(tmpstr, "ResourceNotFoundException:") {
		streamAlreadyExists = false
	}
	if !streamAlreadyExists {
		out, err := client.CreateStream(context.Background(), &kinesis.CreateStreamInput{
			ShardCount: aws.Int32(shardCount),
			StreamName: streamName,
		})
		check(err)
		fmt.Printf("CreateStream %v\n", out)
		waiter := kinesis.NewStreamExistsWaiter(client)
		err = waiter.Wait(context.Background(), &kinesis.DescribeStreamInput{StreamName: streamName}, 300*time.Second)
		check(err)
		fmt.Printf("Stream is now active\n")

		// Register EFO consumer
		_, err = client.RegisterStreamConsumer(context.Background(), &kinesis.RegisterStreamConsumerInput{
			ConsumerName: aws.String(consumerName),
			StreamARN:    aws.String(fmt.Sprintf("arn:aws:kinesis:%s:000000000000:stream/%s", region, stream)),
		})
		check(err)
	}
	fmt.Println("streamAlreadyExists", streamAlreadyExists)

	defer func() {
		// Deregister EFO consumer
		_, err := client.DeregisterStreamConsumer(context.Background(), &kinesis.DeregisterStreamConsumerInput{
			ConsumerName: aws.String(consumerName),
			StreamARN:    aws.String(fmt.Sprintf("arn:aws:kinesis:%s:000000000000:stream/%s", region, stream)),
		})
		check(err)

		// Delete stream
		_, err = client.DeleteStream(context.Background(), &kinesis.DeleteStreamInput{StreamName: streamName})
		check(err)
		waiter := kinesis.NewStreamNotExistsWaiter(client)
		err = waiter.Wait(context.Background(), &kinesis.DescribeStreamInput{StreamName: streamName}, 300*time.Second)
		check(err)
	}()

	streams, err := client.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: streamName})
	check(err)
	_ = streams

	// start the consumer
	main := NewMain()
	main.Stream = stream
	main.Region = region
	main.Schema = "quanta"
	main.ScanInterval = 1000 // ms
	main.Port = int(4000)
	main.ConsulAddr = "127.0.0.1:8500"
	main.ShardKey = "cust_id"
	main.CheckpointDB = false // Disable checkpointing for test

	// Initialize EFO consumer
	main.Consumer, err = consumer.NewEFO(
		*streamName,
		consumerName,
		consumer.WithEFOClient(client),
	)
	check(err)

	main.ScanInterval = DefaultScanInterval

	// push some records to k while the consumer starts
	preloadDone := false
	go func() {
		for i := 0; i < 100; i++ {
			sqlstr := fmt.Sprintf(`{"first_name": "%s", "last_name": "Doe700","cust_id":"%v"}`, test.GetPseudoRandomWord(i+1880), i+700)
			putOutput, err := client.PutRecord(context.Background(), &kinesis.PutRecordInput{
				Data:         []byte(sqlstr),
				StreamName:   streamName,
				PartitionKey: aws.String(partionKey),
			})
			check(err)
			_ = putOutput
			// fmt.Printf("PutRecord 700 %v\n", putOutput)
			time.Sleep(100 * time.Millisecond)
		}
		preloadDone = true
	}()

	time.Sleep(2000 * time.Millisecond)

	_, cancel := context.WithCancel(context.Background())
	main.CancelFunc = cancel

	// Initialize and start consumer
	main.ShardCount, err = main.Init("http://localhost:4566")
	check(err)

	fmt.Println("main ShardCount", main.ShardCount)
	fmt.Println("main tables", main.tableCache.TableCache)

	table := main.tableCache.TableCache["customers_qa"]
	table.SelectorNode, _ = expr.ParseExpression("true")

	// Start consumer processing in background
	go func() {
		fmt.Println("main.MainProcessingLoop START")
		err := main.MainProcessingLoop()
		if err != nil {
			fmt.Printf("MainProcessingLoop error: %v\n", err)
		}
		fmt.Println("main.MainProcessingLoop EXIT")
	}()

	// Wait for records to be processed
	time.Sleep(5 * time.Second)

	// Check records in database
	test.AnalyzeRow(*suite.state.ProxyConnect, []string{"commit"}, true)

	// preload 3 records using PutRecords API
	entries := make([]types.PutRecordsRequestEntry, 3)
	for i := 0; i < len(entries); i++ {
		sqlstr := fmt.Sprintf(`{"first_name": "%s", "last_name": "Doe200","cust_id":"%v"}`, test.GetPseudoRandomWord(i+1900), i+200)
		entries[i] = types.PutRecordsRequestEntry{
			Data:         []byte(sqlstr),
			PartitionKey: aws.String(partionKey),
		}
	}
	// fmt.Printf("entries to put: %v\n", entries)
	putsOutput, err := client.PutRecords(context.Background(), &kinesis.PutRecordsInput{
		Records:    entries,
		StreamName: streamName,
	})
	check(err)
	// putsOutput has Records, and its shard id and sequence enumber.
	fmt.Printf("preload PutRecords by test %v\n", len(putsOutput.Records))

	// start adding records
	fmt.Println("adding mo records")
	// time.Sleep(1 * time.Second) // let main get ready

	if preloadDone { // we want for the background record puts to still be going.
		suite.False(preloadDone)
	}

	jsonrec := fmt.Sprintf(`{"first_name": "%s", "last_name": "Doe1","cust_id":"%v"}`, test.GetPseudoRandomWord(1), 1)
	putOutput, err := client.PutRecord(context.Background(), &kinesis.PutRecordInput{
		Data:         []byte(jsonrec),
		StreamName:   streamName,
		PartitionKey: aws.String(partionKey),
	})
	check(err)
	fmt.Printf("PutRecord 1 %v\n", putOutput)

	jsonrec = fmt.Sprintf(`{"first_name": "%s", "last_name": "Doe2","cust_id":"%v"}`, test.GetPseudoRandomWord(2), 2)
	putOutput, err = client.PutRecord(context.Background(), &kinesis.PutRecordInput{
		Data:         []byte(jsonrec),
		StreamName:   streamName,
		PartitionKey: aws.String(partionKey),
	})
	check(err)
	fmt.Printf("PutRecord 2 %v\n", putOutput)

	// put 10 records using PutRecords API
	entries = make([]types.PutRecordsRequestEntry, 10)
	for i := 0; i < len(entries); i++ {
		sqlstr := fmt.Sprintf(`{"first_name": "%s", "last_name": "Doe100","cust_id":"%v"}`, test.GetPseudoRandomWord(i+100), i+100)
		entries[i] = types.PutRecordsRequestEntry{
			Data:         []byte(sqlstr),
			PartitionKey: aws.String(partionKey),
		}
	}
	// fmt.Printf("entries to put: %v\n", entries)
	putsOutput, err = client.PutRecords(context.Background(), &kinesis.PutRecordsInput{
		Records:    entries,
		StreamName: streamName,
	})
	check(err)

	for !preloadDone {
		time.Sleep(200 * time.Millisecond)
	}

	// kinesis.IncomingRecords ?? how do we check that all the k records were consumed??
	time.Sleep(5 * time.Second) // let threads consume channels
	test.AnalyzeRow(*suite.state.ProxyConnect, []string{"commit"}, true)
	// putsOutput has Records, and its shard id and sequence enumber.
	fmt.Printf("records put by test %v\n", len(putsOutput.Records))
	time.Sleep(5 * time.Second) // let threads consume channels

	// check that the records happened.

	got := test.AnalyzeRow(*suite.state.ProxyConnect, []string{"select cust_id,first_name,last_name from customers_qa;@17"}, true)
	check(got.Err)
	for i := 0; i < len(got.RowDataArray); i++ {
		json, err := json.Marshal(got.RowDataArray[i])
		check(err)
		fmt.Printf("select found %v\n", string(json))
	}
	fmt.Printf("select found count %v\n", len(got.RowDataArray))
	fmt.Printf("select found count %v\n", got.ActualRowCount)
	suite.EqualValues(115, got.ActualRowCount)

	// time.Sleep(1 * time.Second) // jus a sec
	if main.CancelFunc != nil {
		main.CancelFunc() // stop the consumer
	}

	suite.EqualValues(suite.Total.ExpectedRowcount, suite.Total.ActualRowCount)
	suite.EqualValues(0, len(suite.Total.FailedChildren))
}

type Kinesis_test_struct struct {
	// test.BaseDockerSuite

	suite.Suite
	Total test.SqlInfo
	state *test.ClusterLocalState
}

func (suite *Kinesis_test_struct) SetupSuite() {

	// TODO: don't stop the localstack docker container.

	// stop and remove all containers
	// test.Shell("docker stop $(docker ps -aq)", "")
	// test.Shell("docker rm $(docker ps -aq)", "")

	// suite.SetupDockerCluster(3, 1)

	// the local cluster method
	if !test.IsLocalRunning() { // if no cluster is up
		err := os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	suite.state = test.Ensure_cluster(3)

}

func (suite *Kinesis_test_struct) TearDownSuite() {
	// leave the cluster running
	// or:
	// stop and remove all containers
	// test.Shell("docker stop $(docker ps -aq)", "")
	// test.Shell("docker rm $(docker ps -aq)", "")

	// just let it crash suite.state.Release()

}

func check(err error) {
	if err != nil {
		fmt.Println("ERROR ERROR check err", err)
		// panic(err.Error())
	}
}
