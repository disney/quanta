package main

import (
	"bytes"
	sql "database/sql"
	csv "encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"unicode/utf8"

	runtime "github.com/banzaicloud/logrus-runtime-formatter"
	mysql "github.com/go-sql-driver/mysql"
	logger "github.com/sirupsen/logrus"
)

type ProxyConnect struct {
	Env				string
	Host 			string
	Port 			string
	Database 		string
	User 			string
	Password		string
	AssumeRoleArn 	string
	Acl 			string
	SseKmsKeyId 	string
}

type SqlInfo struct {
	Statement			string
	ExpectedRowcount	int64
	ActualRowCount		int64
	ExpectError			bool
	ErrorText			string
	Validate			bool
	Err					error
}

var passCount			int64
var failCount			int64
var failedStatements 	[]string
type StatementType 		int64

const (
	Insert 	StatementType = 0
	Update 	StatementType = 1
	Select 	StatementType = 2
	Count	StatementType = 3
	Admin 	StatementType = 4
)

var buf    bytes.Buffer
var log = logger.New()

func main() {
	scriptFile := flag.String("script_file", "", "Path to the sql file to execute.")
	scriptDelimiter := flag.String("script_delimiter","@","The delimiter to use in the script file.  The default is a colon (:).")
	validate := flag.Bool("validate", false, "If not set, the Sql statement will be executed but not validated.")	
	env := flag.String("env", "", "The environment to run the test in (sb, dev, qa, prod).")
	log_level := flag.String("log_level","","Set the logging level to DEBUG for additional logging.")
	flag.Parse()

	if *scriptFile == "" || *env == ""{
		log.Println()
		log.Println("The args script_file and env are required.")
		log.Println("Environments include : SANDBOX, DEV, QA, PROD, DEBUG, LOCAL, and GUY-SANDBOX")
		log.Println()
		log.Println("Example: ./sqlrunner -script_file test.sql -script_delimiter : -validate -env dev -log_level DEBUG")
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
	log.Debugf("environment : %s", *env)
	log.Debugf("log_level : %s", *log_level)

	proxyConfig := getQuantaConnectInfo(*env)

	f, err := os.Open(*scriptFile)
    if err != nil {
        log.Fatal(err)
    }
    defer f.Close()
	
	csvReader := csv.NewReader(f)
	csvReader.FieldsPerRecord = -1

	log.Debugf("Setting the delimiter : %s", *scriptDelimiter)
	csvReader.Comma, _ = utf8.DecodeRuneInString(*scriptDelimiter)

    for {
        row, err := csvReader.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
			log.Print("If you are using the validate option, be sure and add the expected rowcount after the sql statement.")
			log.Print("Statement example:  select * from table; @ 100")
            log.Fatal(err)
        }

        analyzeRow(proxyConfig, row, *validate)
    }
	logFinalResult()
}

func (ci * ProxyConnect) proxyConnect() (*sql.DB, error){

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
        Passwd:               "",
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

	sqlInfo.Statement = strings.TrimLeft(strings.TrimRight(row[0]," "), " ")
	
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
			if sqlInfo.Validate && ((statementType != Admin) && (statementType != Insert)){
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
		sqlInfo.executeQuery(db)
	} else if statementType == Count {
			sqlInfo.executeScalar(db)		
	} else {
		log.Fatal("Unsupported Statement : %v", sqlInfo.Statement)
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

func (s * SqlInfo) executeQuery(db *sql.DB) {

	var rows *sql.Rows
	rows, s.Err = db.Query(s.Statement)
	if s.Err == nil {
		s.ActualRowCount = getRowCount(rows)
	}
	s.logResult()
}

func (s * SqlInfo) executeScalar(db *sql.DB) {

	var rows *sql.Rows
	rows, s.Err = db.Query(s.Statement)
	if s.Err == nil {
		s.ActualRowCount, s.Err = getScalarCount(rows)
	}
	s.logResult()
}

func (s * SqlInfo) executeAdmin() {

	var err error	
	var cmd string

	statement := strings.Split(strings.TrimRight(strings.TrimLeft(s.Statement," ")," "), " ")
	log.Debugf("Statement : %s", s.Statement)

	cmd = statement[0]
	args :=  make([]string, len(statement)-1)

	for idx := 0; idx < (len(statement)-1); idx++ {
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

func getRowCount(rows *sql.Rows) int64 {

	var count = 0
	for rows.Next() {
		count += 1
    }

	return int64(count)
}

func getScalarCount(rows *sql.Rows) (int64, error) {

	var count int64
	for rows.Next(){
		err := rows.Scan(&count); if err != nil {
			return count, err
		}
	}

	return count, nil
}

func (s *SqlInfo) logResult () {
	// colorYellow := "\033[33m"
	colorReset := "\033[0m"
	colorGreen := "\033[32m"
	colorRed := "\033[31m"

	log.Print("-------------------------------------------------------------------------------")
	log.Printf("%s", s.Statement)
	if (s.Validate) && (!s.ExpectError) {
		result := getPassFail(s.Statement, s.ExpectedRowcount == s.ActualRowCount, s.Err)
		if result == "PASSED" {
			log.Printf(colorGreen + "%s" + colorReset, result)
		} else {
			log.Printf(colorRed + "%s" + colorReset, result)
		}
		log.Printf("Expected Rowcount: %d  Actual Rowcount: %d", s.ExpectedRowcount, s.ActualRowCount)
	} else if (s.Validate) && (s.ExpectError) {
		result := getPassFailError(s.Statement, s.ErrorText, s.Err.Error())
		if result == "PASSED" {
			log.Printf(colorGreen + "%s" + colorReset, result)
		} else {
			log.Printf(colorRed + "%s" + colorReset, result)
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

func logFinalResult(){
	colorYellow := "\033[33m"
	// colorRed := "\033[31m"
	colorReset := "\033[0m"
	log.Print("\n-------- Final Result --------")
	log.Printf("Passed: %d", passCount)
	log.Printf("Failed: %d", failCount)

	if failCount > 0 {
		log.Print("\n-------- Failed Statements --------")
		for _, statement := range failedStatements {
			log.Printf(colorYellow + "%s" + colorReset, statement)
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

func getQuantaConnectInfo(env string) ProxyConnect {

	var connect_info ProxyConnect

	if strings.ToUpper(env) == "SANDBOX" || strings.ToUpper(env) == "SB"{
		connect_info.Env = "SB"
		connect_info.Host = "quanta-mparticle-proxy.us-east-1.dev03.dp.dtcisb.technology"
		connect_info.Port = "4000"
		connect_info.Database = "quanta"
		connect_info.User = "SVCACCT"
		connect_info.Password = ""
		connect_info.AssumeRoleArn = ""
		connect_info.Acl = "bucket-owner-full-control"
		connect_info.SseKmsKeyId = ""
	} else if strings.ToUpper(env) == "DEV" {
		connect_info.Env = "DEV"
		connect_info.Host = "quanta-mparticle-proxy.us-east-1.egmt.dp.dtcidev.technology"
		connect_info.Port = "4000"
		connect_info.Database = "quanta"
		connect_info.User = "SVCACCT"
		connect_info.Password = ""
		connect_info.AssumeRoleArn = "arn:aws:iam::702162754193:role/dp-role-fed-gen-qt-mp-analyst-dev"
		connect_info.Acl = "bucket-owner-full-control"
		connect_info.SseKmsKeyId = "arn:aws:kms:us-east-1:123432527613:key/f2108dee-016b-4fb2-abc7-9b416e796aaf";
	} else if strings.ToUpper(env) == "QA" {
		connect_info.Env = "QA"
		connect_info.Host = "quanta-mparticle-proxy.us-east-1.egmt.dp.dtciqa.technology"
		connect_info.Port = "4000"
		connect_info.Database = "quanta"
		connect_info.User = "SVCACCT"
		connect_info.Password = ""
		connect_info.AssumeRoleArn = "arn:aws:iam::702162754193:role/dp-role-fed-gen-qt-mp-analyst-qa"
		connect_info.Acl = "bucket-owner-full-control"
		connect_info.SseKmsKeyId = "arn:aws:kms:us-east-1:472683241043:key/ec887270-aa4e-4686-b4ab-64e452e7c77a"
	} else if strings.ToUpper(env) == "PROD" {
		connect_info.Env = "PROD"
		connect_info.Host = "quanta-mparticle-proxy.us-east-1.egmt.dp.dtci.technology"
		connect_info.Port = "4000"
		connect_info.Database = "quanta"
		connect_info.User = "SVCACCT"
		connect_info.Password = ""
		connect_info.AssumeRoleArn = ""
		connect_info.Acl = "bucket-owner-full-control"
		connect_info.SseKmsKeyId = ""
	} else if strings.ToUpper(env) == "DEBUG" {
		connect_info.Env = "DEBUG"
		connect_info.Host = "debug"
		connect_info.Port = "4000"
		connect_info.Database = "quanta"
		connect_info.User = "SVCACCT"
		connect_info.Password = ""
		connect_info.AssumeRoleArn = ""
		connect_info.Acl = "bucket-owner-full-control"
		connect_info.SseKmsKeyId = ""
	} else if strings.ToUpper(env) == "LOCAL" {
		connect_info.Env = "LOCAL"
		connect_info.Host = "127.0.0.1"
		connect_info.Port = "4000"
		connect_info.Database = "quanta"
		connect_info.User = "SVCACCT"
		connect_info.Password = ""
		connect_info.AssumeRoleArn = ""
		connect_info.Acl = "bucket-owner-full-control"
		connect_info.SseKmsKeyId = ""
	} else if strings.ToUpper(env) == "GUY-SANDBOX" {
		connect_info.Env = "LOCAL"
		connect_info.Host = "10.180.97.104"
		connect_info.Port = "4000"
		connect_info.Database = "quanta"
		connect_info.User = "DEEMC004"
		connect_info.Password = ""
		connect_info.AssumeRoleArn = ""
		connect_info.Acl = "bucket-owner-full-control"
		connect_info.SseKmsKeyId = ""
	} else {
		panic(fmt.Sprintf("Connection Type Not Supported: %s", env))
	}

	return connect_info
}
