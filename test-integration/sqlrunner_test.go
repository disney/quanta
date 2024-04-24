package test_integration

import (
	"fmt"
	"os"
	"testing"

	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/test"
	"github.com/stretchr/testify/assert"
)

// requirements: Consul must be running on localhost:8500 (it can't be the one in the docker container)
// optional: a local cluster running on localhost:4000
// if there is no local cluster, one will be started. That is slower.

// to run the whole thing see ./test/run-go-integration-tests.sh
// or
// "go test -timeout 3m -v -run TestSQLRunnerSuite ./test-integration/..."

// type SQLRunnerSuite struct {
// 	suite.Suite
// 	isLocalRunning bool
// 	state          *test.ClusterLocalState
// 	total          test.SqlInfo
// 	localLock      sync.Mutex
// }

// func (suite *SQLRunnerSuite) SetupSuite() {

// 	test.AcquirePort4000.Lock()

// 	var err error
// 	shared.SetUTCdefault()

// 	suite.isLocalRunning = test.IsLocalRunning()
// 	// erase the storage
// 	if !suite.isLocalRunning { // if no cluster is up
// 		err = os.RemoveAll("../test/localClusterData/") // start fresh
// 		check(err)
// 	}
// 	// ensure we have a cluster on localhost, start one if necessary
// 	suite.state = test.Ensure_cluster(3)
// }

// func (suite *SQLRunnerSuite) TearDownSuite() {
// 	suite.state.Release()
// 	test.AcquirePort4000.Unlock()
// }

// // In order for 'go test' to run this suite, we need to create
// // a normal test function and pass our suite to suite.Run
// func TestSQLRunnerSuite(t *testing.T) {

// 	ourSuite := new(SQLRunnerSuite)
// 	suite.Run(t, ourSuite)

// 	ourSuite.EqualValues(ourSuite.total.ExpectedRowcount, ourSuite.total.ActualRowCount)
// 	ourSuite.EqualValues(0, len(ourSuite.total.FailedChildren))
// }

// ExecuteSqlFile executes a sql file and returns the result

func TestBasic(t *testing.T) {
	shared.SetUTCdefault()

	isLocalRunning := test.IsLocalRunning()
	// erase the storage
	if !isLocalRunning { // if no cluster is up
		err := os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := test.Ensure_cluster(3)

	// fmt.Println("TestBasic")

	// got := test.ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries.sql")
	got := test.ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_load.sql")

	for _, child := range got.FailedChildren {
		fmt.Println("child failed", child.Statement)
	}

	assert.Equal(t, got.ExpectedRowcount, got.ActualRowCount)
	assert.Equal(t, 0, len(got.FailedChildren))
	// {
	// 	statement := "select * from customers_qa where city = 'Seattle'"
	// 	rows, err := state.Db.Query(statement)
	// 	check(err)

	// 	rowsArr, err := shared.GetAllRows(rows)
	// 	check(err)
	// 	fmt.Println("count", len(rowsArr), rowsArr)
	// 	assert.EqualValues(t, 3, len(rowsArr))

	// 	// count := 0
	// 	// for rows.Next() {
	// 	// 	count += 1
	// 	// }
	// 	// fmt.Println("count", count)
	// 	// assert.EqualValues(t, 3, count)
	// }
	// {
	// 	got = test.AnalyzeRow(*state.ProxyConnect, []string{"select * from customers_qa where city = 'Seattle'"}, true)
	// 	fmt.Println("count", len(got.RowDataArray))
	// 	assert.EqualValues(t, 3, len(got.RowDataArray))
	// }
	// {
	// 	statement := "select * from customers_qa where city = 'Seattle'"
	// 	rows, err := state.Db.Query(statement)
	// 	check(err)

	// 	count := 0
	// 	for rows.Next() {
	// 		count += 1
	// 	}
	// 	fmt.Println("count", count)
	// 	assert.EqualValues(t, 3, count)
	// }
	state.Release()
	// FIXME: see: select avg(age) as avg_age from customers_qa where age > 55 and avg_age = 70 limit 1; in the file
}

func TestFirstName2(t *testing.T) {

	var err error
	shared.SetUTCdefault()

	isLocalRunning := test.IsLocalRunning()
	// erase the storage
	if !isLocalRunning { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := test.Ensure_cluster(3)

	if !isLocalRunning { // if no cluster was up, load some data
		// got := test.ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_load.sql")
		got := test.ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries.sql")

		for _, child := range got.FailedChildren {
			fmt.Println("child failed", child.Statement)
		}

	} // else assume it's already loaded

	// query
	{
		statement := "select * from customers_qa where city = 'Seattle'"
		rows, err := state.Db.Query(statement)
		check(err)

		rowsArr, err := shared.GetAllRows(rows)
		check(err)
		fmt.Println("count", len(rowsArr), rowsArr)
		assert.EqualValues(t, 3, len(rowsArr))
	}
	{
		statement := "select first_name from customers_qa;"
		rows, err := state.Db.Query(statement)
		check(err)

		count := 0
		for rows.Next() {
			columns, err := rows.Columns()
			check(err)
			_ = columns
			//fmt.Println(columns)
			id, firstName := "", ""
			err = rows.Scan(&firstName)
			check(err)
			fmt.Println(id, firstName)
			count += 1
		}
		fmt.Println("count", count)
		assert.EqualValues(t, 30, count)
	}

	// release as necessary
	state.Release()
}

func TestInsert(t *testing.T) {
	shared.SetUTCdefault()
	isLocalRunning := test.IsLocalRunning()
	// erase the storage
	if !isLocalRunning { // if no cluster is up
		err := os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := test.Ensure_cluster(3)

	fmt.Println("Test insert_tests")

	got := test.ExecuteSqlFile(state, "../sqlrunner/sqlscripts/insert_tests.sql")

	for _, child := range got.FailedChildren {
		fmt.Println("child failed", child.Statement)
	}

	assert.Equal(t, got.ExpectedRowcount, got.ActualRowCount)
	assert.Equal(t, 0, len(got.FailedChildren))
	state.Release()
}

func TestJoins(t *testing.T) {
	shared.SetUTCdefault()
	isLocalRunning := test.IsLocalRunning()
	// erase the storage
	if !isLocalRunning { // if no cluster is up
		err := os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := test.Ensure_cluster(3)

	fmt.Println("Test joins_sql")

	got := test.ExecuteSqlFile(state, "../sqlrunner/sqlscripts/joins_sql.sql")

	for _, child := range got.FailedChildren {
		fmt.Println("child failed", child.Statement)
	}

	assert.Equal(t, got.ExpectedRowcount, got.ActualRowCount)
	assert.Equal(t, 0, len(got.FailedChildren))
	state.Release()
}

func check(err error) {
	if err != nil {
		fmt.Println("check err", err)
		panic(err.Error())
	}
}
