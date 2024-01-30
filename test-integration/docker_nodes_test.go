package test_integration

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/disney/quanta/test"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/errgroup"
)

// Requirements: Consul must NOT be running on localhost:8500 we will start our own

// just one test:
// go test -v -run TestBasic ./test-integration/...

// We're going to leverage the fact that docker build is very fast if nothing has changed.
// The nodes are left up. To reset them go to docker desktop (or whatever) and delete the containers.

// Hint: run TestBasic first to make sure stuff is working.

type DockerNodesRunnerSuite struct {
	test.BaseDockerSuite

	// suite.Suite
	// state *test.ClusterLocalState
	// total test.SqlInfo

	// consulAddress string
	// proxyAddress  []string
}

func (suite *DockerNodesRunnerSuite) TestOne() { // just do the setup and teardown
	suite.EqualValues(suite.Total.ExpectedRowcount, suite.Total.ActualRowCount)
	suite.EqualValues(0, len(suite.Total.FailedChildren))
}

func (suite *DockerNodesRunnerSuite) SetupSuite() {

	suite.SetupDockerCluster(3, 2)

}

func (suite *DockerNodesRunnerSuite) TearDownSuite() {
	// leave the cluster running
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestDockerNodesRunnerSuite(t *testing.T) {
	ourSuite := new(DockerNodesRunnerSuite)
	suite.Run(t, ourSuite)

	ourSuite.EqualValues(ourSuite.Total.ExpectedRowcount, ourSuite.Total.ActualRowCount)
	ourSuite.EqualValues(0, len(ourSuite.Total.FailedChildren))
}

// Run two sqlrunners hitting both proxies, forever. Turn your computer into a heater.
func (suite *DockerNodesRunnerSuite) TestBasicTorture() {

	fmt.Println("TestBasicTorture")

	test.StopAndRemoveContainer("basic_queries0")
	test.StopAndRemoveContainer("basic_queries1")

	cmd := "docker run --name basic_queries0 -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/basic_queries_load.sql"
	cmd += " -validate"
	cmd += " -host " + suite.ProxyAddress[0] // this is the proxy
	cmd += " -consul " + suite.ConsulAddress + ":8500"
	cmd += " -user MOLIG004"
	cmd += " db quanta"
	cmd += " -log_level DEBUG"

	out, err := test.Shell(cmd, "")
	// fmt.Println("sqlrunner run", out, err)
	_ = out
	_ = err

	var errGroup errgroup.Group
	for i := 0; i < 2; i++ {

		index := i
		istr := fmt.Sprintf("%d", index)
		errGroup.Go(func() error {
			test.StopAndRemoveContainer("basic_queries" + istr)

			time.Sleep(5 * time.Second)

			cmd = "docker run --name basic_queries" + istr + " -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/basic_queries_body.sql"
			cmd += " -validate"
			cmd += " -repeats 1000"
			cmd += " -host " + suite.ProxyAddress[index] // this is the proxy
			cmd += " -consul " + suite.ConsulAddress + ":8500"
			cmd += " -user MOLIG004"
			cmd += " db quanta"
			cmd += " -log_level DEBUG"

			out, err = test.Shell(cmd, "")
			//fmt.Println("sqlrunner run", out, err)
			_ = out
			_ = err
			hasFailed := strings.Contains(out, "FAILED")
			suite.Assert().EqualValues(false, hasFailed)

			return err
		})
	}
	errGroup.Wait()
}

func (suite *DockerNodesRunnerSuite) TestJoinsTorture() {

	fmt.Println("TestJoinsTorture")

	test.StopAndRemoveContainer("join_queries0")
	test.StopAndRemoveContainer("join_queries1")

	cmd := "docker run --name join_queries0 -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/joins_sql_load.sql"
	cmd += " -validate"
	cmd += " -host " + suite.ProxyAddress[0] // this is the proxy
	cmd += " -consul " + suite.ConsulAddress + ":8500"
	cmd += " -user MOLIG004"
	cmd += " db quanta"
	cmd += " -log_level DEBUG"

	out, err := test.Shell(cmd, "")
	// fmt.Println("sqlrunner run", out, err)
	_ = out
	_ = err

	var errGroup errgroup.Group
	for i := 0; i < 2; i++ {

		index := i
		istr := fmt.Sprintf("%d", index)
		errGroup.Go(func() error {
			test.StopAndRemoveContainer("join_queries" + istr)

			time.Sleep(5 * time.Second)

			cmd = "docker run --name join_queries" + istr + " -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/joins_sql_body.sql"
			cmd += " -validate"
			cmd += " -repeats 1000"
			cmd += " -host " + suite.ProxyAddress[index] // this is the proxy
			cmd += " -consul " + suite.ConsulAddress + ":8500"
			cmd += " -user MOLIG004"
			cmd += " db quanta"
			cmd += " -log_level DEBUG"

			out, err = test.Shell(cmd, "")
			//fmt.Println("sqlrunner run", out, err)
			_ = out
			_ = err
			hasFailed := strings.Contains(out, "FAILED")
			suite.Assert().EqualValues(false, hasFailed)

			return err
		})
	}
	errGroup.Wait()
}

// TestJoinsOneTwo runs the load and then runs the queries once.
func (suite *DockerNodesRunnerSuite) TestJoinsOneTwo() {

	time.Sleep(5 * time.Second)

	fmt.Println("TestJoinOneTwo")

	test.StopAndRemoveContainer("join_queries0")

	cmd := "docker run --name join_queries0 -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/joins_sql_load.sql"
	cmd += " -validate"
	cmd += " -host " + suite.ProxyAddress[0] // this is the proxy
	cmd += " -consul " + suite.ConsulAddress + ":8500"
	cmd += " -user MOLIG004"
	cmd += " db quanta"
	cmd += " -log_level DEBUG"

	out, err := test.Shell(cmd, "")
	// fmt.Println("sqlrunner run", out, err)
	_ = out
	_ = err

	test.StopAndRemoveContainer("join_queries0")

	time.Sleep(5 * time.Second)

	cmd = "docker run --name join_queries0 -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/joins_sql_body.sql"
	cmd += " -validate"
	cmd += " -repeats 1"
	cmd += " -host " + suite.ProxyAddress[0] // this is the proxy
	cmd += " -consul " + suite.ConsulAddress + ":8500"
	cmd += " -user MOLIG004"
	cmd += " db quanta"
	cmd += " -log_level DEBUG"

	out, err = test.Shell(cmd, "")
	//fmt.Println("sqlrunner run", out, err)
	_ = out
	_ = err
	hasFailed := strings.Contains(out, "FAILED")
	suite.Assert().EqualValues(false, hasFailed)

}

// TestBasicOneTwo is same as TestBasic does the load first and then the queries - 10 times
func (suite *DockerNodesRunnerSuite) TestBasicOneTwo() {

	time.Sleep(5 * time.Second)

	fmt.Println("TestBasicOneTwo")

	test.StopAndRemoveContainer("basic_queries0")

	cmd := "docker run --name basic_queries0 -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/basic_queries_load.sql"
	cmd += " -validate"
	cmd += " -host " + suite.ProxyAddress[0] // this is the proxy
	cmd += " -consul " + suite.ConsulAddress + ":8500"
	cmd += " -user MOLIG004"
	cmd += " db quanta"
	cmd += " -log_level DEBUG"

	out, err := test.Shell(cmd, "")
	// fmt.Println("sqlrunner run", out, err)
	_ = out
	_ = err

	test.StopAndRemoveContainer("basic_queries0")

	time.Sleep(5 * time.Second)

	cmd = "docker run --name basic_queries0 -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/basic_queries_body.sql"
	cmd += " -validate"
	cmd += " -repeats 1000"
	cmd += " -host " + suite.ProxyAddress[0] // this is the proxy
	cmd += " -consul " + suite.ConsulAddress + ":8500"
	cmd += " -user MOLIG004"
	cmd += " db quanta"
	cmd += " -log_level DEBUG"

	out, err = test.Shell(cmd, "")
	//fmt.Println("sqlrunner run", out, err)
	_ = out
	_ = err
	hasFailed := strings.Contains(out, "FAILED")
	suite.Assert().EqualValues(false, hasFailed)

}

func (suite *DockerNodesRunnerSuite) TestBasic() {

	fmt.Println("TestBasic")

	test.StopAndRemoveContainer("basic_queries0")

	cmd := "docker run --name basic_queries0 -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/basic_queries.sql"
	cmd += " -validate"
	cmd += " -host " + suite.ProxyAddress[0] // this is the proxy
	cmd += " -consul " + suite.ConsulAddress + ":8500"
	cmd += " -user MOLIG004"
	cmd += " db quanta"
	cmd += " -log_level DEBUG"

	out, err := test.Shell(cmd, "")
	// fmt.Println("sqlrunner run", out, err)
	hasFailed := strings.Contains(out, "FAILED")
	suite.Assert().EqualValues(false, hasFailed)
	_ = err
}

// TestBasicProxy1 is the same as TestBasic but uses proxy 1
func (suite *DockerNodesRunnerSuite) TestBasicProxy1() {

	fmt.Println("TestBasic p1")
	index := 1

	test.StopAndRemoveContainer("basic_queries1")

	cmd := "docker run --name basic_queries1 -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/basic_queries.sql"
	cmd += " -validate"
	cmd += " -host " + suite.ProxyAddress[index] // this is the proxy #1
	cmd += " -consul " + suite.ConsulAddress + ":8500"
	cmd += " -user MOLIG004"
	cmd += " db quanta"
	cmd += " -log_level DEBUG"

	out, err := test.Shell(cmd, "")
	fmt.Println("sqlrunner run", out, err)
	hasFailed := strings.Contains(out, "FAILED")
	suite.Assert().EqualValues(false, hasFailed)

}

// XTestBasic doesn't work because ExecuteSqlFile calls AnalyzeRow which might invoke create table
// and CreateTable wants to contact the nodes directly.
func (suite *DockerNodesRunnerSuite) XTestBasic() { // this would be better if it worked

	fmt.Println("TestBasic")

	got := test.ExecuteSqlFile(suite.State, "../sqlrunner/sqlscripts/basic_queries.sql")

	test.MergeSqlInfo(&suite.Total, got)

	for _, child := range got.FailedChildren {
		fmt.Println("child failed", child.Statement)
	}

	suite.EqualValues(got.ExpectedRowcount, got.ActualRowCount)
	suite.EqualValues(0, len(got.FailedChildren))

	// FIXME: see: select avg(age) as avg_age from customers_qa where age > 55 and avg_age = 70 limit 1; in the file
}
