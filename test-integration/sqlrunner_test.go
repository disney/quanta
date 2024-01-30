package test_integration

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/test"
	"github.com/stretchr/testify/suite"
)

// requirements: Consul must be running on localhost:8500 (it can't be the one in the docker container)
// optional: a local cluster running on localhost:4000
// if there is no local cluster, one will be started. That is slower.

// to run the whole thing run TestSQLRunnerSuite below
// or
// "go test -timeout 3m -v -run TestSQLRunnerSuite ./test-integration/..."

type SQLRunnerSuite struct {
	suite.Suite
	isLocalRunning bool
	state          *test.ClusterLocalState
	total          test.SqlInfo
	localLock      sync.Mutex
}

func (suite *SQLRunnerSuite) SetupSuite() {

	test.AcquirePort4000.Lock()

	var err error
	shared.SetUTCdefault()

	suite.isLocalRunning = test.IsLocalRunning()
	// erase the storage
	if !suite.isLocalRunning { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	suite.state = test.Ensure_cluster(3)
}

func (suite *SQLRunnerSuite) TearDownSuite() {
	suite.state.Release()
	test.AcquirePort4000.Unlock()
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestSQLRunnerSuite(t *testing.T) {

	ourSuite := new(SQLRunnerSuite)
	suite.Run(t, ourSuite)

	ourSuite.EqualValues(ourSuite.total.ExpectedRowcount, ourSuite.total.ActualRowCount)
	ourSuite.EqualValues(0, len(ourSuite.total.FailedChildren))
}

// ExecuteSqlFile executes a sql file and returns the result

func (suite *SQLRunnerSuite) TestBasic() {

	suite.localLock.Lock()
	defer suite.localLock.Unlock()

	fmt.Println("TestBasic")

	got := test.ExecuteSqlFile(suite.state, "../sqlrunner/sqlscripts/basic_queries.sql")

	test.MergeSqlInfo(&suite.total, got)

	for _, child := range got.FailedChildren {
		fmt.Println("child failed", child.Statement)
	}

	suite.EqualValues(got.ExpectedRowcount, got.ActualRowCount)
	suite.EqualValues(0, len(got.FailedChildren))

	// FIXME: see: select avg(age) as avg_age from customers_qa where age > 55 and avg_age = 70 limit 1; in the file
}

func (suite *SQLRunnerSuite) TestInsert() {
	suite.localLock.Lock()
	defer suite.localLock.Unlock()

	fmt.Println("Test insert_tests")

	got := test.ExecuteSqlFile(suite.state, "../sqlrunner/sqlscripts/insert_tests.sql")

	test.MergeSqlInfo(&suite.total, got)

	for _, child := range got.FailedChildren {
		fmt.Println("child failed", child.Statement)
	}

	suite.EqualValues(got.ExpectedRowcount, got.ActualRowCount)
	suite.EqualValues(0, len(got.FailedChildren))
}

func (suite *SQLRunnerSuite) TestJoins() {
	suite.localLock.Lock()
	defer suite.localLock.Unlock()

	fmt.Println("Test joins_sql")

	got := test.ExecuteSqlFile(suite.state, "../sqlrunner/sqlscripts/joins_sql.sql")

	test.MergeSqlInfo(&suite.total, got)

	for _, child := range got.FailedChildren {
		fmt.Println("child failed", child.Statement)
	}

	suite.EqualValues(got.ExpectedRowcount, got.ActualRowCount)
	suite.EqualValues(0, len(got.FailedChildren))
}

func check(err error) {
	if err != nil {
		fmt.Println("check err", err)
		panic(err.Error())
	}
}
