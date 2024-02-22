package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	u "github.com/araddon/gou"

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
		u.Warn()
		u.Warn("The arguments script_file, host, user and password are required.")
		u.Warn()
		u.Warn("Example: ./sqlrunner -script_file test.sql -validate -host 1.1.1.1 -user username -password whatever -db quanta -port 4000 -log_level DEBUG")
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

	u.Debugf("script_file : %s", *scriptFile)
	u.Debugf("validate : %t", *validate)
	u.Debugf("host : %s", *host)
	u.Debugf("user : %s", *user)
	u.Debugf("database : %s", *database)
	u.Debugf("port : %s", *port)
	u.Debugf("log_level : %s", *log_level)
	u.Debugf("repeats : %d\n", *repeats)

	var proxyConnect test.ProxyConnectStrings
	proxyConnect.Host = *host
	proxyConnect.User = *user
	proxyConnect.Password = *password
	proxyConnect.Port = *port
	proxyConnect.Database = *database

	test.ConsulAddress = *consul
	u.Debugf("ConsulAddress : %s", test.ConsulAddress)

	consulClient, err := api.NewClient(&api.Config{Address: test.ConsulAddress})
	check(err)

	conn := shared.NewDefaultConnection("sqlrunner")
	// conn.ServicePort = main.Port
	conn.Quorum = 3
	if err := conn.Connect(consulClient); err != nil {
		u.Log(u.FATAL, err)
	}

	sharedKV := shared.NewKVStore(conn)

	// we're in cluster so we can just call admin status directly

	now := time.Now()
	for {
		status, active, size := sharedKV.GetClusterState()
		u.Debugf("consul status sqlrunner clusterState %v count %v size %v\n", status, active, size)

		if status.String() == "GREEN" && active >= 3 && size >= 3 {
			break
		}
		if time.Since(now) > time.Second*30 {
			u.Log(u.FATAL, "consul timeout driver after NewKVStore")
		}
		time.Sleep(500 * time.Millisecond)
	}

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

	// We can afford to read the whole file into memory
	// If not then use a line reader. Let's not use a csv reader since the file is not csv.
	bytes, err := os.ReadFile(scriptFile)
	if err != nil {
		u.Log(u.FATAL, "scriptFile Open Failed : ", err)
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
