package test

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	admin "github.com/disney/quanta/quanta-admin-lib"
	"github.com/go-sql-driver/mysql"
)

// These types and routines are in support of sqlrunner

type ProxyConnectStrings struct {
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
	Rows             *sql.Rows
}

type StatementType int64

var consulAddress = "127.0.0.1:8500"

const (
	Insert StatementType = 0
	Update StatementType = 1
	Select StatementType = 2
	Count  StatementType = 3
	Admin  StatementType = 4
	Create StatementType = 5
)

var PassCount int64
var FailCount int64
var FailedStatements []string

func (ci *ProxyConnectStrings) ProxyConnectConnect() (*sql.DB, error) {

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

func (ci *ProxyConnectStrings) getQuantaHost() mysql.Config {

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

func AnalyzeRow(proxyConfig ProxyConnectStrings, row []string, validate bool) SqlInfo {

	var err error
	var sqlInfo SqlInfo

	sqlInfo.Statement = strings.TrimLeft(strings.TrimRight(row[0], " "), " ")

	sqlInfo.ExpectedRowcount = 0
	sqlInfo.ActualRowCount = 0
	sqlInfo.Validate = validate

	if sqlInfo.Statement == "" {
		return sqlInfo
	}

	if strings.HasPrefix(sqlInfo.Statement, "#") || strings.HasPrefix(sqlInfo.Statement, "--") {
		log.Printf("Skipping row - commented out: %s", sqlInfo.Statement)
		return sqlInfo
	}

	var statementType StatementType
	lowerStmt := strings.ToLower(sqlInfo.Statement)
	if strings.HasPrefix(lowerStmt, "insert") {
		statementType = Insert
	} else if strings.HasPrefix(lowerStmt, "update") {
		statementType = Update
	} else if strings.HasPrefix(lowerStmt, "select") {
		statementType = Select
		if strings.Contains(sqlInfo.Statement, "count(*)") {
			statementType = Count
		}
	} else if strings.Contains(lowerStmt, "quanta-admin") {
		statementType = Admin
	} else if strings.Contains(lowerStmt, "create") {
		statementType = Create
	}

	err = nil

	if statementType == Admin {
		sqlInfo.ExecuteAdmin()
		time.Sleep(1 * time.Second)
		return sqlInfo
	}

	db, err := proxyConfig.ProxyConnectConnect()
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
				log.Println("Row validation turned on.")
				sqlInfo.ExpectedRowcount, err = strconv.ParseInt(row[1], 10, 64)
				if err != nil {
					log.Println("Validate")
					log.Fatal(err)
				}
			}
		}
	}

	switch statementType {
	case Insert:
		sqlInfo.ExecuteInsert(db)
	case Update:
		sqlInfo.ExecuteUpdate(db)
	case Select:
		//time.Sleep(500 * time.Millisecond)
		sqlInfo.ExecuteQuery(db)
	case Count:
		//time.Sleep(500 * time.Millisecond)
		sqlInfo.ExecuteScalar(db)
	case Create:
		sqlInfo.ExecuteCreate(db)
	default:
		log.Fatalf("Unsupported Statement : %v", sqlInfo.Statement)

	}
	return sqlInfo

	// if statementType == Insert {
	// 	sqlInfo.ExecuteInsert(db)
	// } else if statementType == Update {
	// 	sqlInfo.ExecuteUpdate(db)
	// } else if statementType == Select {
	// 	//time.Sleep(500 * time.Millisecond)
	// 	sqlInfo.ExecuteQuery(db)
	// } else if statementType == Count {
	// 	//time.Sleep(500 * time.Millisecond)
	// 	sqlInfo.ExecuteScalar(db)
	// } else if statementType == Create {
	// 	//time.Sleep(500 * time.Millisecond)
	// 	sqlInfo.ExecuteCreate(db)
	// } else {
	// 	log.Fatalf("Unsupported Statement : %v", sqlInfo.Statement)
	// }
}

func (s *SqlInfo) ExecuteAdmin() {

	var err error
	// var cmd string

	statement := strings.Split(strings.TrimRight(strings.TrimLeft(s.Statement, " "), " "), " ")
	log.Printf("Statement : %s", s.Statement)

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
}

func (s *SqlInfo) ExecuteInsert(db *sql.DB) {

	var res sql.Result
	log.Printf("Insert Statement : %s", s.Statement)
	res, s.Err = db.Exec(s.Statement)
	if res != nil {
		s.ActualRowCount, _ = res.RowsAffected()
	}
	s.logResult()
}

func (s *SqlInfo) ExecuteUpdate(db *sql.DB) {

	var res sql.Result
	res, s.Err = db.Exec(s.Statement)
	if res != nil {
		s.ActualRowCount, _ = res.RowsAffected()
	}
	s.logResult()
}

func (s *SqlInfo) ExecuteQuery(db *sql.DB) {

	var rows *sql.Rows
	rows, s.Err = db.Query(s.Statement)
	if s.Err == nil {
		s.ActualRowCount = GetRowCount(rows)
	}
	s.Rows = rows
	s.logResult()
}

func (s *SqlInfo) ExecuteScalar(db *sql.DB) {

	var rows *sql.Rows
	rows, s.Err = db.Query(s.Statement)
	if s.Err == nil {
		s.ActualRowCount, s.Err = GetScalarCount(rows)
	}
	s.logResult()
}

func (s *SqlInfo) ExecuteCreate(db *sql.DB) {

	var rows *sql.Rows
	rows, s.Err = db.Query(s.Statement)
	if s.Err == nil {
		s.ActualRowCount = 1 // atw FIXME ? GetRowCount(rows)
		_ = rows
	}
	s.logResult()
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

func getPassFail(statement string, passfail bool, err error) string {

	if (!passfail) || (err != nil) {
		FailCount += 1
		FailedStatements = append(FailedStatements, statement)
		return "FAILED"
	} else {
		PassCount += 1
		return "PASSED"
	}
}

func getPassFailError(statement string, expectedError string, actualError string) string {

	if strings.Contains(strings.ToLower(actualError), strings.ToLower(expectedError)) {
		PassCount += 1
		return "PASSED"
	} else {
		FailCount += 1
		FailedStatements = append(FailedStatements, statement)
		return "FAILED"
	}
}

func GetRowCount(rows *sql.Rows) int64 {

	var count = 0
	for rows.Next() {
		count += 1
	}

	return int64(count)
}

func GetScalarCount(rows *sql.Rows) (int64, error) {

	var count int64
	for rows.Next() {
		err := rows.Scan(&count)
		if err != nil {
			return count, err
		}
	}

	return count, nil
}
