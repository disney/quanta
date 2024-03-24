package test_integration_docker

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/disney/quanta/test"
	"github.com/stretchr/testify/suite"
)

// Start a cluster, restart a node.
// check if a query works.

// This is the test for DFS-456-data-node-restart.
// The bug is that when a node was restarted, it hung and never restarted.
// It was CNR but here is a test to be sure.

// Requirements: Consul must NOT be running on localhost:8500 we will start our own

// run it
// go test -timeout 90s -tags=integration -run ^TestDockerSuiteRestart$ github.com/disney/quanta/test-integration-docker

// run one test ? TODO: doesn't work
// go test -timeout 90s -tags=integration -run ^TestDockerSuiteRestart$ github.com/disney/quanta/test-integration-docker -testify.m TestDummy

type DockerSuiteRestart struct {
	test.BaseDockerSuite
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
// Start this one and not TestRestart
func TestDockerSuiteRestart(t *testing.T) {
	ourSuite := new(DockerSuiteRestart)
	suite.Run(t, ourSuite)

	ourSuite.EqualValues(ourSuite.Total.ExpectedRowcount, ourSuite.Total.ActualRowCount)
	ourSuite.EqualValues(0, len(ourSuite.Total.FailedChildren))
}

// TestRestart is the test function here.
// it's going to try to restart a node.
func (suite *DockerSuiteRestart) TestRestart() { // just do the setup and teardown
	fmt.Println("TestRestart cluster started")
	suite.EqualValues(suite.Total.ExpectedRowcount, suite.Total.ActualRowCount)
	suite.EqualValues(0, len(suite.Total.FailedChildren))
	fmt.Println("TestRestart checking for status ")
	test.WaitForStatusGreen(suite.ConsulAddress+":8500", "q-node-0")
	fmt.Println("TestRestart status check done ")

	// add some data
	test.StopAndRemoveContainer("basic_queries0")
	cmd := "docker run --name basic_queries0 -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/basic_queries_load.sql"
	cmd += " -validate"
	cmd += " -host " + suite.ProxyAddress[0] // this is the proxy
	cmd += " -consul " + suite.ConsulAddress + ":8500"
	cmd += " -user MOLIG004"
	cmd += " db quanta"
	cmd += " -log_level DEBUG"

	_, err := test.Shell(cmd, "")
	check(err)

	test.StopAndRemoveContainer("basic_queries0")
	cmd = "docker run --name basic_queries0 -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/basic_queries_body_bug_zip2.sql"
	cmd += " -validate"
	cmd += " -repeats 1"
	cmd += " -host " + suite.ProxyAddress[0] // this is the proxy
	cmd += " -consul " + suite.ConsulAddress + ":8500"
	cmd += " -user MOLIG004"
	cmd += " db quanta"
	cmd += " -log_level DEBUG"

	out, err := test.Shell(cmd, "")
	check(err)
	hasFailed := strings.Contains(out, "FAILED")
	suite.Assert().EqualValues(false, hasFailed)

	// restart a node
	test.Shell("docker restart q-node-1", "")

	time.Sleep(5 * time.Second)
	fmt.Println("TestRestart post restart status check styarting")
	test.WaitForStatusGreen(suite.ConsulAddress+":8500", "q-node-0")
	fmt.Println("TestRestart post restart status check done ")

	// test again
	test.StopAndRemoveContainer("basic_queries0")
	out, err = test.Shell(cmd, "")
	check(err)

	hasFailed = strings.Contains(out, "FAILED")
	suite.Assert().EqualValues(false, hasFailed)

}

func (suite *DockerSuiteRestart) SetupSuite() {
	// stop and remove all containers
	test.Shell("docker stop $(docker ps -aq)", "")
	test.Shell("docker rm $(docker ps -aq)", "")

	suite.SetupDockerCluster(3, 1)
	fmt.Println("DockerSuiteRestart SetupSuite done")
}

func (suite *DockerSuiteRestart) TearDownSuite() {
	// leave the cluster running
	// or,
	test.Shell("docker stop $(docker ps -aq)", "")
	test.Shell("docker rm $(docker ps -aq)", "")
}

// TestDummy is used to try to get Go to run one test in the suite.
// FIXME: it doesn't work.
func (suite *DockerSuiteRestart) TestDummy() {
	fmt.Println("TestDummy")
	fmt.Println("TestDummy")
	fmt.Println("TestDummy")
	fmt.Println("TestDummy")
}
