package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	runtime "github.com/banzaicloud/logrus-runtime-formatter"
	"github.com/disney/quanta/rbac"
	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/test"
	"github.com/hashicorp/consul/api"
	logger "github.com/sirupsen/logrus"
)

// var buf bytes.Buffer
var log = logger.New()

func main() {
	shared.SetUTCdefault()

	scriptFile := flag.String("script_file", "", "Path to the sql file to execute.")
	// scriptDelimiter := flag.String("script_delimiter", "@", "The delimiter to use in the script file.  The default is a colon (:).")
	validate := flag.Bool("validate", false, "If not set, the Sql statement will be executed but not validated.")
	host := flag.String("host", "", "Quanta host to connect to.")
	user := flag.String("user", "", "The username that will connect to the database.")
	password := flag.String("password", "", "The password to use to connect.")
	database := flag.String("db", "quanta", "The database to connect to.")
	port := flag.String("port", "4000", "Port to connect to.")
	consul := flag.String("consul", "127.0.0.1:8500", "Address of consul.")
	log_level := flag.String("log_level", "", "Set the logging level to DEBUG for additional logging.")
	repeats := flag.Int("repeats", 1, "to execute the scriptFile more that once. Default is 1")
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
	// log.Debugf("delimiter : %s", *scriptDelimiter)
	log.Debugf("validate : %t", *validate)
	log.Debugf("host : %s", *host)
	log.Debugf("user : %s", *user)
	log.Debugf("database : %s", *database)
	log.Debugf("port : %s", *port)
	log.Debugf("log_level : %s", *log_level)
	fmt.Printf("repeats : %d\n", *repeats)

	var proxyConnect test.ProxyConnectStrings
	proxyConnect.Host = *host
	proxyConnect.User = *user
	proxyConnect.Password = *password
	proxyConnect.Port = *port
	proxyConnect.Database = *database

	test.ConsulAddress = *consul
	log.Debugf("ConsulAddress : %s", test.ConsulAddress)

	consulClient, err := api.NewClient(&api.Config{Address: test.ConsulAddress})
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

	for rep := 0; rep < *repeats; rep++ {
		executeFile(rep, *scriptFile, proxyConnect, *validate)
	}
	logFinalResult()
}

func executeFile(rep int, scriptFile string, proxyConnect test.ProxyConnectStrings, validate bool) {

	fmt.Println("rep", rep)

	// We can afford to read the whole file into memory
	// If not then use a line reader. Let's not use a csv reader since the file is not csv.
	bytes, err := os.ReadFile(scriptFile)
	if err != nil {
		fmt.Println("scriptFile Open Failed", err)
		log.Fatal("scriptFile Open Failed : ", err)
	}
	sql := string(bytes)
	lines := strings.Split(sql, "\n")

	for _, row := range lines {
		lineLines := strings.Split(row, "\\") // a '\' is a line continuation
		test.AnalyzeRow(proxyConnect, lineLines, validate)
	}
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