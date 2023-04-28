package test

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr/builtins"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/araddon/qlbridge/schema"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/custom/functions"
	"github.com/disney/quanta/rbac"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type QuantaTestSuite2 struct {
	suite.Suite
	node  *server.Node
	store *shared.KVStore
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func BenchmarkQuantaTestSuite(t *testing.B) {
	suite := new(QuantaTestSuite2)
	_ = suite
	// atw fixme:
	// suite.SetupSuite()
}

func (suite *QuantaTestSuite2) SetupSuite() {
	var err error
	suite.node, err = Setup() // harness setup
	assert.NoError(suite.T(), err)

	core.ClearTableCache()
	RemoveContents("./testdata/index")
	RemoveContents("./testdata/bitmap")

	// Server side components already started and available in package level variables in harness.go
	conn := shared.NewDefaultConnection()
	conn.ServicePort = 0
	err = conn.Connect(nil) // no consul
	assert.NoError(suite.T(), err)

	u.SetupLogging("debug")

	// load all of our built-in functions
	builtins.LoadAllBuiltins()
	functions.LoadAll() // Custom functions

	// Simulate the mySQL proxy endpoint for golang dbdriver connection clients.
	src, err2 := source.NewQuantaSource("./testdata/config", "", 0, 1)
	assert.NoError(suite.T(), err2)
	schema.RegisterSourceAsSchema("quanta", src)

	suite.store = shared.NewKVStore(conn)
	assert.NotNil(suite.T(), suite.store)

	ctx, err := rbac.NewAuthContext(suite.store, "USER001", true)
	assert.NoError(suite.T(), err)
	err = ctx.GrantRole(rbac.DomainUser, "USER001", "quanta", true)
	assert.NoError(suite.T(), err)

	// load up vision test data (nested schema containing 3 separate tables)
	start := time.Now()
	fmt.Println("QuantaTestSuite2 SetupSuite before cities")
	suite.loadData("cities", "./testdata/us_cities.parquet", conn, false)
	end := time.Now()

	fmt.Println("QuantaTestSuite2 SetupSuite after cities", end.Sub(start).String())
	start = end

	fmt.Println("QuantaTestSuite2 SetupSuite before cityzip")
	suite.loadData("cityzip", "./testdata/us_cityzip.parquet", conn, false)
	end = time.Now()
	fmt.Println("QuantaTestSuite2 SetupSuite after cityzip", end.Sub(start).String())
	//suite.insertData("cityzip", "./testdata/us_cityzip.parquet")
	// suite.loadData("nba", "./testdata/nba.parquet")
	fmt.Println("QuantaTestSuite2 SetupSuite done")

}

func (suite *QuantaTestSuite2) loadData(table, filePath string, conn *shared.Conn, ignoreSourcePath bool) error {

	fr, err := local.NewLocalFileReader(filePath)
	assert.Nil(suite.T(), err)
	if err != nil {
		log.Println("Can't open file", err)
		return err
	}
	pr, err := reader.NewParquetColumnReader(fr, 4)
	assert.Nil(suite.T(), err)
	if err != nil {
		log.Println("Can't create column reader", err)
		return err
	}
	num := int(pr.GetNumRows())
	c, err := core.OpenSession("./testdata/config", table, false, conn)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c)

	for i := 1; i <= num; i++ {
		err := c.PutRow(table, pr, 0, ignoreSourcePath, false)
		assert.Nil(suite.T(), err)
	}
	c.Flush()
	c.CloseSession()
	return nil
}

func (suite *QuantaTestSuite2) BeforeTest(suiteName, testName string) {
	fmt.Println("BeforeTest2", suiteName, testName)
}

func (suite *QuantaTestSuite2) AfterTest(suiteName, testName string) {
	fmt.Println("AfterTest2", suiteName, testName)
}

func (suite *QuantaTestSuite2) runQuery(q string, args []interface{}) ([]string, []string, error) {

	// Connect using GoLang database/sql driver.
	db, err := sql.Open("qlbridge", "quanta")
	defer db.Close()
	if err != nil {
		return []string{""}, []string{""}, err
	}

	// Set user id in session
	setter := "set @userid = 'USER001'"
	_, err = db.Exec(setter)
	assert.NoError(suite.T(), err)

	log.Printf("EXECUTING SQL: %v", q)
	var rows *sql.Rows
	var stmt *sql.Stmt
	if args != nil {
		stmt, err = db.Prepare(q)
		assert.NoError(suite.T(), err)
		rows, err = stmt.Query(args...)
	} else {
		rows, err = db.Query(q)
	}
	if err != nil {
		return []string{""}, []string{""}, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()

	// this is just stupid hijinx for getting pointers for unknown len columns
	readCols := make([]interface{}, len(cols))
	writeCols := make([]string, len(cols))
	for i := range writeCols {
		readCols[i] = &writeCols[i]
	}
	results := make([]string, 0)
	for rows.Next() {
		rows.Scan(readCols...)
		result := strings.Join(writeCols, ",")
		results = append(results, result)
		log.Println(result)
	}
	log.Println("")
	return results, cols, nil
}
