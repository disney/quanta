package main

import (
	"bytes"
	sql "database/sql"
	csv "encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/alecthomas/kong"
	runtime "github.com/banzaicloud/logrus-runtime-formatter"
	admin "github.com/disney/quanta/quanta-admin-lib"
	"github.com/disney/quanta/rbac"
	"github.com/disney/quanta/shared"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/hashicorp/consul/api"
	logger "github.com/sirupsen/logrus"
)

type ProxyConnect struct {
	Env           string
	Host          string
	Port          string
	Database      string
	User          string
	Password      string
	AssumeRoleArn string
	Acl           string
	SseKmsKeyId   string
}

type SqlInfo struct {
	Statement        string
	ExpectedRowcount int64
	ActualRowCount   int64
	ExpectError      bool
	ErrorText        string
	Validate         bool
	Err              error
}

var passCount int64
var failCount int64
var failedStatements []string

type StatementType int64

var consulAddress = "127.0.0.1:8500"

const (
	Insert StatementType = 0
	Update StatementType = 1
	Select StatementType = 2
	Count  StatementType = 3
	Admin  StatementType = 4
)

// var buf bytes.Buffer
var log = logger.New()

func main() {
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

	var proxyConnect ProxyConnect
	proxyConnect.Host = *host
	proxyConnect.User = *user
	proxyConnect.Password = *password
	proxyConnect.Port = *port
	proxyConnect.Database = *database

	consulAddress = *consul

	path, err := os.Getwd()
	if err != nil {
		log.Println(err)
	}
	fmt.Println("current path", path)

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

		analyzeRow(proxyConnect, row, *validate)
	}
	logFinalResult()
}

func (ci *ProxyConnect) proxyConnect() (*sql.DB, error) {

	var db *sql.DB
	var err error
	if ci.Host != "debug" {
		cfg := ci.getQuantaHost()
		db, err = sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			return nil, err
		}
	}
	return db, nil
}

func (ci *ProxyConnect) getQuantaHost() mysql.Config {

	cfg := mysql.Config{
		User:                 ci.User,
		Passwd:               ci.Password,
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("%s:%s", ci.Host, ci.Port),
		DBName:               ci.Database,
		AllowNativePasswords: true,
	}

	return cfg
}

func analyzeRow(proxyConfig ProxyConnect, row []string, validate bool) {

	var err error
	var sqlInfo SqlInfo

	sqlInfo.Statement = strings.TrimLeft(strings.TrimRight(row[0], " "), " ")

	sqlInfo.ExpectedRowcount = 0
	sqlInfo.ActualRowCount = 0
	sqlInfo.Validate = validate

	if sqlInfo.Statement == "" {
		return
	}

	if strings.HasPrefix(sqlInfo.Statement, "#") || strings.HasPrefix(sqlInfo.Statement, "--") {
		log.Debugf("Skipping row - commented out: %s", sqlInfo.Statement)
		return
	}

	var statementType StatementType
	if strings.HasPrefix(strings.ToLower(sqlInfo.Statement), "insert") {
		statementType = Insert
	} else if strings.HasPrefix(strings.ToLower(sqlInfo.Statement), "update") {
		statementType = Update
	} else if strings.HasPrefix(strings.ToLower(sqlInfo.Statement), "select") {
		statementType = Select
		if strings.Contains(sqlInfo.Statement, "count(*)") {
			statementType = Count
		}
	} else if strings.Contains(strings.ToLower(sqlInfo.Statement), "quanta-admin") {
		statementType = Admin
	}

	err = nil

	if statementType == Admin {
		sqlInfo.executeAdmin()
		time.Sleep(1 * time.Second)
		return
	}

	db, err := proxyConfig.proxyConnect()
	if err != nil {
		log.Fatal("Proxy Connection Failed : ", err)
	}
	defer db.Close()

	if len(row) > 1 {
		if strings.HasPrefix(strings.ToUpper(row[1]), "ERR") {
			sqlInfo.ErrorText = strings.Split(row[1], ":")[1]
			sqlInfo.ExpectError = true
		} else {
			if sqlInfo.Validate && ((statementType != Admin) && (statementType != Insert)) {
				log.Debug("Row validation turned on.")
				sqlInfo.ExpectedRowcount, err = strconv.ParseInt(row[1], 10, 64)
				if err != nil {
					log.Debug("Validate")
					log.Fatal(err)
				}
			}
		}
	}

	if statementType == Insert {
		sqlInfo.executeInsert(db)
	} else if statementType == Update {
		sqlInfo.executeUpdate(db)
	} else if statementType == Select {
		time.Sleep(500 * time.Millisecond)
		sqlInfo.executeQuery(db)
	} else if statementType == Count {
		time.Sleep(500 * time.Millisecond)
		sqlInfo.executeScalar(db)
	} else {
		log.Fatalf("Unsupported Statement : %v", sqlInfo.Statement)
	}
}

func (s *SqlInfo) executeInsert(db *sql.DB) {

	var res sql.Result
	log.Debugf("Insert Statement : %s", s.Statement)
	res, s.Err = db.Exec(s.Statement)
	if res != nil {
		s.ActualRowCount, _ = res.RowsAffected()
	}
	s.logResult()
}

func (s *SqlInfo) executeUpdate(db *sql.DB) {

	var res sql.Result
	res, s.Err = db.Exec(s.Statement)
	if res != nil {
		s.ActualRowCount, _ = res.RowsAffected()
	}
	s.logResult()
}

func (s *SqlInfo) executeQuery(db *sql.DB) {

	var rows *sql.Rows
	rows, s.Err = db.Query(s.Statement)
	if s.Err == nil {
		s.ActualRowCount = getRowCount(rows)
	}
	s.logResult()
}

func (s *SqlInfo) executeScalar(db *sql.DB) {

	var rows *sql.Rows
	rows, s.Err = db.Query(s.Statement)
	if s.Err == nil {
		s.ActualRowCount, s.Err = getScalarCount(rows)
	}
	s.logResult()
}

var useAdminDirectly = true

func (s *SqlInfo) executeAdmin() {

	var err error
	var cmd string

	statement := strings.Split(strings.TrimRight(strings.TrimLeft(s.Statement, " "), " "), " ")
	log.Debugf("Statement : %s", s.Statement)

	if useAdminDirectly {

		command := statement[1:]
		parser, err := kong.New(&admin.Cli)
		if err != nil {
			fmt.Println("executeAdmin kong new ", err)
			return
		}
		ctx, err := parser.Parse(command) // os.Args[1:])
		parser.FatalIfErrorf(err)

		err = ctx.Run(&admin.Context{ConsulAddr: consulAddress,
			Port:  admin.Cli.Port,
			Debug: admin.Cli.Debug})
		if err != nil {
			fmt.Println("executeAdmin ctx.Run ", err)
			return
		}

	} else {
		// deprecated - delete this
		cmd = statement[0]
		args := make([]string, len(statement)-1)

		for idx := 0; idx < (len(statement) - 1); idx++ {
			args[idx] = statement[idx+1]
			log.Debugf("Arg : %s", args[idx])
		}
		execCmd := exec.Command(cmd, args...)
		log.Debugf("Command : %v", execCmd)

		var out bytes.Buffer
		var stderr bytes.Buffer
		execCmd.Stdout = &out
		execCmd.Stderr = &stderr
		err = execCmd.Run()
		if err != nil {
			fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
			return
		}
		fmt.Println("Result: " + out.String())

		// output, err := execCmd.Output()
		// if err != nil {
		// 	log.Fatal(err)
		// }
		// fmt.Printf("Output : %v", output)
		// logAdminOutput(s.Statement, output)
	}
}

func getRowCount(rows *sql.Rows) int64 {

	var count = 0
	for rows.Next() {
		count += 1
	}

	return int64(count)
}

func getScalarCount(rows *sql.Rows) (int64, error) {

	var count int64
	for rows.Next() {
		err := rows.Scan(&count)
		if err != nil {
			return count, err
		}
	}

	return count, nil
}

func (s *SqlInfo) logResult() {
	// colorYellow := "\033[33m"
	colorReset := "\033[0m"
	colorGreen := "\033[32m"
	colorRed := "\033[31m"

	log.Print("-------------------------------------------------------------------------------")
	log.Printf("%s", s.Statement)
	if (s.Validate) && (!s.ExpectError) {
		result := getPassFail(s.Statement, s.ExpectedRowcount == s.ActualRowCount, s.Err)
		if result == "PASSED" {
			log.Printf(colorGreen+"%s"+colorReset, result)
		} else {
			log.Printf(colorRed+"%s"+colorReset, result)
		}
		log.Printf("Expected Rowcount: %d  Actual Rowcount: %d", s.ExpectedRowcount, s.ActualRowCount)
	} else if (s.Validate) && (s.ExpectError) {
		result := getPassFailError(s.Statement, s.ErrorText, s.Err.Error())
		if result == "PASSED" {
			log.Printf(colorGreen+"%s"+colorReset, result)
		} else {
			log.Printf(colorRed+"%s"+colorReset, result)
		}
		log.Printf("Expected Error Number: %s  Actual Error Text: %s", s.ErrorText, s.Err.Error())
	}
	if s.Err != nil {
		log.Printf("Error : %v", s.Err)
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
	log.Printf("Passed: %d", passCount)
	log.Printf("Failed: %d", failCount)

	if failCount > 0 {
		log.Print("\n-------- Failed Statements --------")
		for _, statement := range failedStatements {
			log.Printf(colorYellow+"%s"+colorReset, statement)
		}
	}
}

func getPassFail(statement string, passfail bool, err error) string {

	if (!passfail) || (err != nil) {
		failCount += 1
		failedStatements = append(failedStatements, statement)
		return "FAILED"
	} else {
		passCount += 1
		return "PASSED"
	}
}

func getPassFailError(statement string, expectedError string, actualError string) string {

	if strings.Contains(strings.ToLower(actualError), strings.ToLower(expectedError)) {
		passCount += 1
		return "PASSED"
	} else {
		failCount += 1
		failedStatements = append(failedStatements, statement)
		return "FAILED"
	}
}

func check(err error) {
	if err != nil {
		fmt.Println("check err", err)
		panic(err.Error())
	}
}
