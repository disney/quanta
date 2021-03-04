package test

import (
	"database/sql"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr/builtins"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/araddon/qlbridge/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/disney/quanta/client"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/custom/functions"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/source"
	"log"
	"strings"
	"testing"
)

type QuantaTestSuite struct {
	suite.Suite
	endpoint *server.EndPoint
	client   *quanta.BitmapIndex
}

func (suite *QuantaTestSuite) SetupSuite() {
	var err error
	suite.endpoint, suite.client, err = Setup() // harness setup
	assert.NoError(suite.T(), err)

	core.ClearTableCache()
	RemoveContents("./testdata/metadata")
	RemoveContents("./testdata/events.*")
	RemoveContents("./testdata/user.*")
	RemoveContents("./testdata/media.*")
	RemoveContents("./testdata/adobe_conformed.*")
	RemoveContents("./testdata/search.dat")

	// Server side components already started and available in package level variables in harness.go

	// load up vision test data (nested schema containing 3 separate tables)
	suite.loadData("user", "./testdata/small-user.snappy.parquet")

	// load up adobe conformed data
	suite.loadData("adobe_conformed", "./testdata/adobe-conformed-small.snappy.parquet")

	// load all of our built-in functions
	u.SetupLogging("debug")
	builtins.LoadAllBuiltins()
	functions.LoadAll() // Custom functions

	// Simulate the mySQL proxy endpoint for golang dbdriver connection clients.
	src, err2 := source.NewQuantaSource("./testdata/config", "./testdata/metadata", "", 0)
	assert.NoError(suite.T(), err2)
	schema.RegisterSourceAsSchema("quanta", src)
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

	c, err := core.OpenConnection("./testdata/config", "./testdata/metadata", table, true, 0, 0, nil)
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
	results, err := suite.runQuery("select count(*) from events")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("60", results[0])
}

// Test projection with nested data source
func (suite *QuantaTestSuite) TestSimpleProjection() {
	results, err := suite.runQuery("select app_bundle_id, app_name, browser_local_storage_flag from events")
	assert.NoError(suite.T(), err)
	suite.Equal(60, len(results))
}

// Test join with nested data source
func (suite *QuantaTestSuite) TestSimpleJoin() {
	results, err := suite.runQuery("select count(*) from user as u inner join events as e on u.anonymous_id = e.event_anonymous_id")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("60", results[0])
}

func (suite *QuantaTestSuite) TestSQLSyntaxUnknownKeyword() {
	_, err := suite.runQuery("selectX count(*) from events")
	assert.EqualError(suite.T(), err, "Unrecognized request type: selectX")
}

func (suite *QuantaTestSuite) TestSQLSyntaxUnknownTable() {
	_, err := suite.runQuery("select count(*) from eventsX")
	assert.EqualError(suite.T(), err, "QLBridge.plan: No datasource found")
}

func (suite *QuantaTestSuite) TestSQLSyntaxUnknownField() {
	_, err := suite.runQuery("select count(*) from events where nonsensefield = null")
	assert.Error(suite.T(), err)
}

func (suite *QuantaTestSuite) TestNotBetween() {
	results, err := suite.runQuery("select count(*) from events where event_properties_app_sequence_num NOT BETWEEN 10 and 15")
	assert.NoError(suite.T(), err)
	suite.Equal(1, len(results))
}

func (suite *QuantaTestSuite) TestInvalidTableOnJoin() {
	_, err := suite.runQuery("select count(*) from events as e inner join faketable as f on e.event_anonymous_id = f.fake_id")
	assert.EqualError(suite.T(), err, "invalid table faketable in join criteria [INNER JOIN faketable AS f ON e.event_anonymous_id = f.fake_id]")
}

func (suite *QuantaTestSuite) TestInvalidFieldOnJoin() {
	_, err := suite.runQuery("select count(*) from events as e inner join user as u on e.event_anonymous_id = u.fake_field")
	assert.EqualError(suite.T(), err, "invalid field fake_field in join criteria [INNER JOIN user AS u ON e.event_anonymous_id = u.fake_field]")
}

func (suite *QuantaTestSuite) TestSelectStar() {
	results, err := suite.runQuery("select * from events where event_properties_timestamp != NULL")
	assert.NoError(suite.T(), err)
	suite.Equal(60, len(results))
}

func (suite *QuantaTestSuite) TestSelectStarWithAlias() {
	results, err := suite.runQuery("select e.* from events as e")
	assert.NoError(suite.T(), err)
	suite.Equal(60, len(results))
}

func (suite *QuantaTestSuite) TestJoinWithoutOnClause() {
	_, err := suite.runQuery("select count(*) from events as e inner join user as u")
	assert.EqualError(suite.T(), err, "join criteria missing (ON clause)")
}

func (suite *QuantaTestSuite) TestJoinWithNonkeyFields() {
	_, err := suite.runQuery("select count(*) from events as e inner join user as u on e.event_properties_instance_id = u.anonymous_id")
	assert.EqualError(suite.T(), err, "join field event_properties_instance_id is not a relation")
}

func (suite *QuantaTestSuite) TestSumInvalidFieldName() {
	_, err := suite.runQuery("select sum(foobar) from events WHERE event_properties_timestamp != NULL")
	assert.EqualError(suite.T(), err, "attribute 'foobar' not found")
}

func (suite *QuantaTestSuite) TestSumInvalidFieldType() {
	_, err := suite.runQuery("select sum(standard_daily_visitor) from adobe_conformed")
	assert.EqualError(suite.T(), err, "can't sum a non-bsi field standard_daily_visitor")
}

func (suite *QuantaTestSuite) TestSimpleSum() {
	results, err := suite.runQuery("select sum(computed_ua_screen_width) from adobe_conformed")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("284156", results[0])
}

func (suite *QuantaTestSuite) TestSimpleAvg() {
	results, err := suite.runQuery("select avg(computed_ua_screen_width) from adobe_conformed")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("1056", results[0])
}

func (suite *QuantaTestSuite) TestAdobeCount() {
	results, err := suite.runQuery("select count(*) from adobe_conformed")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("400", results[0])
}

func (suite *QuantaTestSuite) TestAdobeEventList() {
	results, err := suite.runQuery("select standard_event_list from adobe_conformed where standard_event_list != null")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(395, len(results))
	for _, v := range results {
		assert.Contains(suite.T(), v, ",")
	}
	results, err = suite.runQuery("select count(*) from adobe_conformed where standard_event_list = '169'")
	assert.NoError(suite.T(), err)
	suite.Equal("212", results[0])
}

func (suite *QuantaTestSuite) TestAdobeDates() {
	results, err := suite.runQuery("select standard_date_time from adobe_conformed")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(400, len(results))
	for _, v := range results {
		assert.True(suite.T(), strings.HasPrefix(v, "2020-12-17") || strings.HasPrefix(v, "2020-12-18"))
	}
}

func (suite *QuantaTestSuite) TestAdobeIntDirect() {
	results, err := suite.runQuery("select count(*) from adobe_conformed where standard_daily_visitor = 1")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("62", results[0])
}

func (suite *QuantaTestSuite) TestAdobeBoolDirect() {
	results, err := suite.runQuery("select count(*) from adobe_conformed where computed_ua_is_flash_supported = true")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("210", results[0])
}
