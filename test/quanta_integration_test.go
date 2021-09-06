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
	"github.com/disney/quanta/rbac"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/source"
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
	RemoveContents("./testdata/index")

	// Server side components already started and available in package level variables in harness.go

	// load up vision test data (nested schema containing 3 separate tables)
	suite.loadData("cities", "./testdata/us_cities.parquet")
	suite.loadData("cityzip", "./testdata/us_cityzip.parquet")
	// suite.loadData("nba", "./testdata/nba.parquet")

	// load all of our built-in functions
	u.SetupLogging("debug")
	builtins.LoadAllBuiltins()
	functions.LoadAll() // Custom functions

	// Simulate the mySQL proxy endpoint for golang dbdriver connection clients.
	src, err2 := source.NewQuantaSource("./testdata/config", "", 0)
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

	c, err := core.OpenConnection("./testdata/config", table, false, 0, 0, nil)
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

// Test inner join with nested data source
func (suite *QuantaTestSuite) TestInnerJoin() {
	results, err := suite.runQuery("select count(*) from cityzip as z inner join cities as c on c.id = z.city_id")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("46280", results[0])
}

// Test outer join
func (suite *QuantaTestSuite) TestOuterJoinWithPredicate() {
	results, err := suite.runQuery("select count(*) from cityzip as z outer join cities as c on c.id = z.city_id where z.city = 'Oceanside'")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("10", results[0])
}

// Test outer join no predicate
func (suite *QuantaTestSuite) TestOuterJoinNoPredicate() {
	results, err := suite.runQuery("select count(*) from cities as c outer join cityzip as z on c.id = z.city_id")
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

func (suite *QuantaTestSuite) TestBetweenWithNegative() {
	results, err := suite.runQuery("select count(*) from cities where latitude BETWEEN 41.0056 AND 44.9733 AND longitude BETWEEN '-111.0344' AND '-104.0692'")

	assert.NoError(suite.T(), err)
	suite.Equal("202", results[0]) // Count of all cities in WY
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

// AGGREGATES
// SUM, MIN, MAX, AVERAGE
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

func (suite *QuantaTestSuite) TestAvgInvalidFieldName() {
	_, err := suite.runQuery("select avg(foobar) from cities WHERE timezone != NULL")
	assert.EqualError(suite.T(), err, "attribute 'foobar' not found")
}

func (suite *QuantaTestSuite) TestAvgInvalidFieldType() {
	_, err := suite.runQuery("select avg(state_name) from cities")
	assert.EqualError(suite.T(), err, "can't average a non-bsi field state_name")
}

func (suite *QuantaTestSuite) TestSimpleMin() {
	results, err := suite.runQuery("select min(population) from cities where name = 'Oceanside'")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	//suite.Equal("2530", results[0])
	suite.Equal("       352", results[0])
}

func (suite *QuantaTestSuite) TestNegativeMin() {
	results, err := suite.runQuery("select min(longitude) from cities where state = 'WY'")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(" -111.0344", results[0])
}

func (suite *QuantaTestSuite) TestMinInvalidFieldName() {
	_, err := suite.runQuery("select min(foobar) from cities WHERE timezone != NULL")
	assert.EqualError(suite.T(), err, "attribute 'foobar' not found")
}

func (suite *QuantaTestSuite) TestMinInvalidFieldType() {
	_, err := suite.runQuery("select min(state_name) from cities")
	assert.EqualError(suite.T(), err, "can't find the minimum of a non-bsi field state_name")
}

func (suite *QuantaTestSuite) TestSimpleMax() {
	results, err := suite.runQuery("select max(population) from cities")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("  18713220", results[0])
}

func (suite *QuantaTestSuite) TestMaxInvalidFieldName() {
	_, err := suite.runQuery("select max(foobar) from cities WHERE timezone != NULL")
	assert.EqualError(suite.T(), err, "attribute 'foobar' not found")
}

func (suite *QuantaTestSuite) TestMaxInvalidFieldType() {
	_, err := suite.runQuery("select max(state_name) from cities")
	assert.EqualError(suite.T(), err, "can't find the maximum of a non-bsi field state_name")
}

// END AGGREGATES

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

// Like statements
// LIKE will index based off a whole word (separated by whitespace)
// Wildcard operators are not necessary
func (suite *QuantaTestSuite) TestCitiesLikeStatement() {
	results, err := suite.runQuery("select county, name from cities where name like 'woods' and name like 'HAWTHORN'")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(1, len(results))
}

func (suite *QuantaTestSuite) TestCitiesLikeListStatement() {
	_, err := suite.runQuery("select count(*) from cities where region_list like 'NY'")
	assert.EqualError(suite.T(), err, "LIKE operator not supported for non-range field 'region_list'")
}

// DISTINCT statement
func (suite *QuantaTestSuite) TestCitiesDistinctStatement() {
	results, err := suite.runQuery("select distinct state_name from cities")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(52, len(results))
}

// AND conditional statement
func (suite *QuantaTestSuite) TestCitiesAndWhereStatement() {
	results, err := suite.runQuery("select id, state_name, state from cities where state_name = 'New York State' and state = 'NY'")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(1186, len(results))
}

// OR conditional statement
func (suite *QuantaTestSuite) TestCitiesOrWhereStatement() {
	results, err := suite.runQuery("select id, state_name, state from cities where state_name = 'New York State' or state = 'NY'")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(1186, len(results))
}

// AND / OR conditional statement
func (suite *QuantaTestSuite) TestCitiesAndOrWhereStatement() {
	results, err := suite.runQuery("select id, state_name, state from cities where state_name = 'New York State' or state = 'NY' and ranking = 3")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal(1051, len(results))
}

// IN statements
func (suite *QuantaTestSuite) TestCitiesINStatement() {
	results, err := suite.runQuery("select count(*) from cities where state IN ('NY','AK','WA')")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("2154", results[0])
}

// IN with NOT statement
func (suite *QuantaTestSuite) TestCitiesNotINStatement() {
	results, err := suite.runQuery("select count(*) from cities where state not IN ('NY','AK','WA')")
	assert.NoError(suite.T(), err)
	assert.Greater(suite.T(), len(results), 0)
	suite.Equal("27334", results[0])
}

// SELECT INTO AWS-S3
// Need to find a way to test S3 Integrations
//func (suite *QuantaTestSuite) TestCitiesSelectInfoS3tatement() {
//	results, err := suite.runQuery("select distinct prop_swid as swid, standard_ip as ip_address into 's3://guys-test/output.csv' from adobe_cricinfo where prop_swid != null and (standard_geo_country = 'ind' or evar_geo_country_code = 'ind' or prop_geo_country_code = 'ind') with delimiter = ',';")
//	assert.NoError(suite.T(), err)
//	assert.Greater(suite.T(), len(results), 0)
//	suite.Equal(1, len(results))
//}

// VIEWS - NOT YET IMPLEMENTED

// Create View
// func (suite *QuantaTestSuite) TestCreateView() {
// 	results, err := suite.runQuery("create view quanta_test_view as select * from cities")
// 	assert.NoError(suite.T(), err)
// 	assert.Greater(suite.T(), len(results), 0)
// 	suite.Equal(1, len(results))

// 	results, err = suite.runQuery("select * from quanta_test_view limit 10")
// 	assert.NoError(suite.T(), err)
// 	assert.Greater(suite.T(), len(results), 0)
// 	suite.Equal(10, len(results))
// }

// // Alter View
// func (suite *QuantaTestSuite) TestAlterView() {
// 	results, err := suite.runQuery("create view quanta_test_view as select state from cities")
// 	assert.NoError(suite.T(), err)
// 	assert.Greater(suite.T(), len(results), 0)
// 	suite.Equal(1, len(results))

// 	results, err = suite.runQuery("select * from quanta_test_view limit 10")
// 	assert.NoError(suite.T(), err)
// 	assert.Greater(suite.T(), len(results), 0)
// 	suite.Equal(10, len(results))
// }

// func (suite *QuantaTestSuite) TestDropView() {
// 	results, err := suite.runQuery("drop view quanta_test_view")
// 	assert.NoError(suite.T(), err)
// 	assert.Greater(suite.T(), len(results), 0)
// 	suite.Equal(1, len(results))

// 	results, err = suite.runQuery("select * from quanta_test_view limit 10")
// 	assert.NoError(suite.T(), err)
// 	assert.Equal(suite.T(), len(results), 0)
// 	suite.Equal(1, len(results))
// 	assert.EqualError(suite.T(), err, "View doesn't exist")
// }
