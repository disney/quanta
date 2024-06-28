package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	u "github.com/araddon/gou"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/custom/functions"
	"github.com/disney/quanta/qlbridge/expr/builtins"
	q_kinesis_lib "github.com/disney/quanta/quanta-kinesis-consumer-lib"
	"github.com/disney/quanta/shared"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/alecthomas/kingpin.v2"
)

// Variables to identify the build
var (
	Version string
	Build   string
	// ShardChannelSize = 10000
	EPOCH, _ = time.ParseInLocation(time.RFC3339, "2000-01-01T00:00:00+00:00", time.UTC)
)

func main() {

	app := kingpin.New(os.Args[0], "Quanta Kinesis data consumer").DefaultEnvars()
	app.Version("Version: " + Version + "\nBuild: " + Build)

	stream := app.Arg("stream", "Kinesis stream name.").Required().String()
	schema := app.Arg("schema", "Schema name.").Required().String()
	shardKey := app.Arg("shard-key", "Shard Key").Required().String()
	region := app.Arg("region", "AWS region").Default("us-east-1").String()
	port := app.Flag("port", "Port number for service").Default("4000").Int32()
	withAssumeRoleArn := app.Flag("assume-role-arn", "Assume role ARN.").String()
	withAssumeRoleArnRegion := app.Flag("assume-role-arn-region", "Assume role ARN region.").String()
	environment := app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()
	trimHorizon := app.Flag("trim-horizon", "Set initial position to TRIM_HORIZON").Bool()
	noCheckpointer := app.Flag("no-checkpoint-db", "Disable DynamoDB checkpointer.").Bool()
	checkpointTable := app.Flag("checkpoint-table", "DynamoDB checkpoint table name.").String()
	avroPayload := app.Flag("avro-payload", "Payload is Avro.").Bool()
	deaggregate := app.Flag("deaggregate", "Incoming payload records are aggregated.").Bool()
	scanInterval := app.Flag("scan-interval", "Scan interval (milliseconds)").Default("1000").Int()
	logLevel := app.Flag("log-level", "Log Level [ERROR, WARN, INFO, DEBUG]").Default("WARN").String()

	kingpin.MustParse(app.Parse(os.Args[1:]))

	shared.InitLogging(*logLevel, *environment, q_kinesis_lib.AppName, Version, "Quanta")

	builtins.LoadAllBuiltins()
	functions.LoadAll()

	main := q_kinesis_lib.NewMain()
	main.Stream = *stream
	main.Region = *region
	main.Schema = *schema
	main.ScanInterval = *scanInterval
	main.Port = int(*port)
	main.ConsulAddr = *consul

	log.Printf("Set Logging level to %v.", *logLevel)
	log.Printf("Kinesis stream %v.", main.Stream)
	log.Printf("Kinesis region %v.", main.Region)
	log.Printf("Schema name %v.", main.Schema)
	log.Printf("Scan interval (milliseconds) %d.", main.ScanInterval)
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
			u.Errorf("DynamoDB checkpoint enabled but 'checkpoint-table' not specified")
			os.Exit(1)
		}
		log.Printf("DynamoDB checkpoint table name [%s]", main.CheckpointTable)
		if main.InitialPos == "LATEST" {
			u.Errorf("Checkpoint enabled.  Shard iterator is 'LATEST' setting it to 'AFTER_SEQUENCE_NUMBER'.")
			main.InitialPos = "AFTER_SEQUENCE_NUMBER"
		}
	}

	if shardKey != nil {
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

	if main.ShardCount, err = main.Init(""); err != nil {
		u.Error(err)
		os.Exit(1)
	}

	go func() {
		// Start Prometheus endpoint
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()

	// var ticker *time.Ticker TODO: implement or delete
	// ticker = main.PrintStats()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for range c {
			u.Warnf("Interrupted,  Bytes processed: %s, Records: %v", core.Bytes(main.TotalBytes.Get()),
				main.TotalRecs.Get())
			u.Errorf("Interrupt received, calling cancel function")
			if main.CancelFunc != nil {
				main.CancelFunc()
			}
		}
	}()

	err = main.MainProcessingLoop() // normally never returns.
	if err != nil {
		exitErrorf("MainProcessingLoop error exit: %v", err)
	}
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
