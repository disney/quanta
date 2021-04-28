package test

import (
	"database/sql"
	"log"
	"strings"
	"testing"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr/builtins"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/araddon/qlbridge/schema"
	quanta "github.com/disney/quanta/client"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/custom/functions"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/source"
	"github.com/disney/quanta/rbac"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type QuantaTestSuite struct {
	suite.Suite
	endpoint *server.EndPoint
    store    *quanta.KVStore
}

func (suite *QuantaTestSuite) SetupSuite() {
	var err error
	suite.endpoint, err = Setup() // harness setup
	assert.NoError(suite.T(), err)

	core.ClearTableCache()
	RemoveContents("./testdata/metadata")
	RemoveContents("./testdata/metadata/cities")
	RemoveContents("./testdata/metadata/cityzip")
	RemoveContents("./testdata/search.dat")

	// Server side components already started and available in package level variables in harness.go

	// load up vision test data (nested schema containing 3 separate tables)
	suite.loadData("cities", "./testdata/us_cities.parquet")
	suite.loadData("cityzip", "./testdata/cityzip.parquet")
	// suite.loadData("nba", "./testdata/nba.parquet")

	// load all of our built-in functions
	u.SetupLogging("debug")
	builtins.LoadAllBuiltins()
	functions.LoadAll() // Custom functions

	// Simulate the mySQL proxy endpoint for golang dbdriver connection clients.
	src, err2 := source.NewQuantaSource("./testdata/config", "./testdata/metadata", "", 0)
	assert.NoError(suite.T(), err2)
	schema.RegisterSourceAsSchema("quanta", src)

    conn := quanta.NewDefaultConnection()
    conn.ServicePort = 0
    err = conn.Connect()
    assert.NoError(suite.T(), err)

    suite.store = quanta.NewKVStore(conn)
    assert.NotNil(suite.T(), suite.store)

    ctx, err := rbac.NewAuthContext(suite.store, "USER001", true)
    assert.NoError(suite.T(), err)
    err = ctx.GrantRole(rbac.DomainUser, "USER001", "quanta", true)
    assert.NoError(suite.T(), err)
}

func (suite *QuantaTestSuite) loadData(table, filePath string) error {

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

	c, err := core.OpenConnection("./testdata/config", "./testdata/metadata", table, false, 0, 0, nil)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c)

	for i := 1; i <= num; i++ {
		err := c.PutRow(table, pr)
		//if i % 10 == 0 {
		//    log.Printf("Processing Row %d", i)
		//}
		assert.Nil(suite.T(), err)
	}
	c.Flush()
	c.CloseConnection()
	return nil
}

func (suite *QuantaTestSuite) runQuery(q string) ([]string, error) {

	// Connect using GoLang database/sql driver.
	db, err := sql.Open("qlbridge", "quanta")
	defer db.Close()
	if err != nil {
		return []string{""}, err
	}

    // Set user id in session
    setter := "set @userid = 'USER001'"
	_, err = db.Exec(setter)
	assert.NoError(suite.T(), err)

	log.Printf("EXECUTING SQL: %v", q)
	rows, err := db.Query(q)
	if err != nil {
		return []string{""}, err
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
	return results, nil
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestQuantaTestSuite(t *testing.T) {
	suite.Run(t, new(QuantaTestSuite))
}

// Test count query with nested data source
func (suite *QuantaTestSuite) TestSimpleQuery() {
	results, err := suite.runQuery("select count(*) from cities")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("29488", results[0])
}

// Test projection with nested data source
func (suite *QuantaTestSuite) TestSimpleProjection() {
	results, err := suite.runQuery("select id, name, state_name, state from cities limit 100000")
	assert.NoError(suite.T(), err)
	suite.Equal(29488, len(results))
}

// Test join with nested data source
func (suite *QuantaTestSuite) TestSimpleJoin() {
	results, err := suite.runQuery("select count(*) from cities as c inner join cityzip as z on c.id = z.city_id")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("46280", results[0])
}

func (suite *QuantaTestSuite) TestSQLSyntaxUnknownKeyword() {
	_, err := suite.runQuery("selectX count(*) from cities")
	assert.EqualError(suite.T(), err, "Unrecognized request type: selectX")
}

func (suite *QuantaTestSuite) TestSQLSyntaxUnknownTable() {
	_, err := suite.runQuery("select count(*) from citiesx")
	assert.EqualError(suite.T(), err, "QLBridge.plan: No datasource found")
}

func (suite *QuantaTestSuite) TestSQLSyntaxUnknownField() {
	_, err := suite.runQuery("select count(*) from cities where nonsensefield = null")
	assert.Error(suite.T(), err)
}

func (suite *QuantaTestSuite) TestNotBetween() {
	results, err := suite.runQuery("select count(*) from cities where population NOT BETWEEN 100000 and 150000")
	assert.NoError(suite.T(), err)
	suite.Equal("29321", results[0])
}

func (suite *QuantaTestSuite) TestInvalidTableOnJoin() {
	_, err := suite.runQuery("select count(*) from cities as c inner join faketable as f on c.id = f.fake_id")
	assert.EqualError(suite.T(), err, "invalid table faketable in join criteria [INNER JOIN faketable AS f ON c.id = f.fake_id]")
}

func (suite *QuantaTestSuite) TestInvalidFieldOnJoin() {
	_, err := suite.runQuery("select count(*) from cities as c inner join cityzip as z on c.id = z.fake_field")
	assert.EqualError(suite.T(), err, "invalid field fake_field in join criteria [INNER JOIN cityzip AS z ON c.id = z.fake_field]")
}

func (suite *QuantaTestSuite) TestSelectStar() {
	results, err := suite.runQuery("select * from cities where timezone != NULL limit 100000")
	assert.NoError(suite.T(), err)
	suite.Equal(29488, len(results))
}

func (suite *QuantaTestSuite) TestSelectStarWithAlias() {
	results, err := suite.runQuery("select count(*) from cities as c")
	assert.NoError(suite.T(), err)
	suite.Equal("29488", results[0])
}

func (suite *QuantaTestSuite) TestJoinWithoutOnClause() {
	_, err := suite.runQuery("select count(*) from cities as c inner join cityzip as z")
	assert.EqualError(suite.T(), err, "join criteria missing (ON clause)")
}

func (suite *QuantaTestSuite) TestJoinWithNonkeyFields() {
	_, err := suite.runQuery("select count(*) from cities as c inner join cityzip as z on c.state_name = z.state")
	assert.EqualError(suite.T(), err, "join field state_name is not a relation")
}

func (suite *QuantaTestSuite) TestSumInvalidFieldName() {
	_, err := suite.runQuery("select sum(foobar) from cities WHERE timezone != NULL")
	assert.EqualError(suite.T(), err, "attribute 'foobar' not found")
}

func (suite *QuantaTestSuite) TestSumInvalidFieldType() {
	_, err := suite.runQuery("select sum(state_name) from cities")
	assert.EqualError(suite.T(), err, "can't sum a non-bsi field state_name")
}

func (suite *QuantaTestSuite) TestSimpleSum() {
	results, err := suite.runQuery("select sum(population) from cities")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("406795495", results[0])
}

func (suite *QuantaTestSuite) TestSimpleAvg() {
	results, err := suite.runQuery("select avg(population) from cities")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("13795", results[0])
}

func (suite *QuantaTestSuite) TestCityzipCount() {
	results, err := suite.runQuery("select count(*) from cityzip")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("46280", results[0])
}

func (suite *QuantaTestSuite) TestCitiesRegionList() {
	results, err := suite.runQuery("select count(*) from cities where region_list != null")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("29488", results[0])
	results, err = suite.runQuery("select count(*) from cities where region_list = 'NY'")
	assert.NoError(suite.T(), err)
	suite.Equal("1186", results[0])
}

func (suite *QuantaTestSuite) TestCitiesTimestamp() {
	results, err := suite.runQuery("select created_timestamp from cities")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(5000, len(results))
	// for _, v := range results {
	// 	assert.True(suite.T(), strings.HasPrefix(v, "1970-01-16"))
	// }
}

func (suite *QuantaTestSuite) TestCitiesIntDirect() {
	results, err := suite.runQuery("select count(*) from cities where ranking = 1")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("50", results[0])
}

func (suite *QuantaTestSuite) TestCitiesBoolDirect() {
	results, err := suite.runQuery("select count(*) from cities where military = true")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("84", results[0])
}

