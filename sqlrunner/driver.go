package main

import (
	csv "encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"
	"unicode/utf8"

	runtime "github.com/banzaicloud/logrus-runtime-formatter"
	"github.com/disney/quanta/rbac"
	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/test"
	"github.com/hashicorp/consul/api"
	logger "github.com/sirupsen/logrus"
)

var consulAddress = "127.0.0.1:8500"

// var buf bytes.Buffer
var log = logger.New()

func main() {
	shared.SetUTCdefault()

	scriptFile := flag.String("script_file", "", "Path to the sql file to execute.")
	scriptDelimiter := flag.String("script_delimiter", "@", "The delimiter to use in the script file.  The default is a colon (:).")
	validate := flag.Bool("validate", false, "If not set, the Sql statement will be executed but not validated.")
	host := flag.String("host", "", "Quanta host to connect to.")
	user := flag.String("user", "", "The username that will connect to the database.")
	password := flag.String("password", "", "The password to use to connect.")
	database := flag.String("db", "quanta", "The database to connect to.")
	port := flag.String("port", "4000", "Port to connect to.")
	consul := flag.String("consul", "127.0.0.1:8500", "Address of consul.")
	log_level := flag.String("log_level", "", "Set the logging level to DEBUG for additional logging.")
	flag.Parse()

	if *scriptFile == "" || *host == "" || *user == "" {
		log.Println()
		log.Println("The arguments script_file, host, user and password are required.")
		log.Println()
		log.Println("Example: ./sqlrunner -script_file test.sql -validate -host 1.1.1.1 -user username -password whatever -db quanta -port 4000 -log_level DEBUG")
		os.Exit(0)
	}

	if *log_level == "DEBUG" {
		formatter := runtime.Formatter{ChildFormatter: &logger.TextFormatter{
			FullTimestamp: true,
		}}
		formatter.Line = true
		log.SetFormatter(&formatter)
		log.SetOutput(os.Stdout)
		log.SetLevel(logger.DebugLevel)
		log.WithFields(logger.Fields{
			"file": "driver.go",
		}).Info("SqlRunner is running...")
	}

	log.Debugf("script_file : %s", *scriptFile)
	log.Debugf("delimiter : %s", *scriptDelimiter)
	log.Debugf("validate : %t", *validate)
	log.Debugf("host : %s", *host)
	log.Debugf("user : %s", *user)
	log.Debugf("database : %s", *database)
	log.Debugf("port : %s", *port)
	log.Debugf("log_level : %s", *log_level)

	var proxyConnect test.ProxyConnect
	proxyConnect.Host = *host
	proxyConnect.User = *user
	proxyConnect.Password = *password
	proxyConnect.Port = *port
	proxyConnect.Database = *database

	consulAddress = *consul

	directory := "./"                       // The current directory
	files, err := ioutil.ReadDir(directory) //read the files from the directory
	fmt.Println("current files", len(files))
	if err != nil {
		fmt.Println("error reading directory:", err) //print error if directory is not read properly
		return
	}
	for _, file := range files {
		fmt.Println(file.Name()) //print the files from the directory
	}

	f, err := os.Open(*scriptFile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	csvReader.FieldsPerRecord = -1

	log.Debugf("Setting the delimiter : %s", *scriptDelimiter)
	csvReader.Comma, _ = utf8.DecodeRuneInString(*scriptDelimiter)

	consulAddress := *consul
	consulClient, err := api.NewClient(&api.Config{Address: consulAddress})
	check(err)

	conn := shared.NewDefaultConnection()
	// conn.ServicePort = main.Port
	conn.Quorum = 3
	if err := conn.Connect(consulClient); err != nil {
		log.Fatal(err)
	}

	sharedKV := shared.NewKVStore(conn)

	time.Sleep(5 * time.Second)

	ctx, err := rbac.NewAuthContext(sharedKV, "USER001", true)
	check(err)
	err = ctx.GrantRole(rbac.DomainUser, "USER001", "quanta", true)
	check(err)

	ctx, err = rbac.NewAuthContext(sharedKV, "MOLIG004", true)
	check(err)
	err = ctx.GrantRole(rbac.DomainUser, "MOLIG004", "quanta", true)
	check(err)

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Print("If you are using the validate option, be sure and add the expected rowcount after the sql statement.")
			log.Print("Statement example:  select * from table;@100")
			log.Fatal(err)
		}

		test.AnalyzeRow(proxyConnect, row, *validate)
	}
	logFinalResult()
}

func logAdminOutput(command string, output []byte) {
	log.Print("-------------------------------------------------------------------------------")
	log.Printf("%s", command)
	log.Print("Output:")
	log.Printf("%v", string(output[:]))
}

var _ = logAdminOutput // fixme: (atw) unused

func logFinalResult() {
	colorYellow := "\033[33m"
	// colorRed := "\033[31m"
	colorReset := "\033[0m"
	log.Print("\n-------- Final Result --------")
	log.Printf("Passed: %d", test.PassCount)
	log.Printf("Failed: %d", test.FailCount)

	if test.FailCount > 0 {
		log.Print("\n-------- Failed Statements --------")
		for _, statement := range test.FailedStatements {
			log.Printf(colorYellow+"%s"+colorReset, statement)
		}
	}
}

func check(err error) {
	if err != nil {
		fmt.Println("check err", err)
		panic(err.Error())
	}
}
