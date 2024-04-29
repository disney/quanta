package test_integration_docker

import (
	"testing"

	"github.com/disney/quanta/test"
	"github.com/stretchr/testify/suite"
)

// This is a simple test suite that just sets up and tears down a docker cluster.

// Requirements: Consul must NOT be running on localhost:8500 we will tart our own

// run them all
// go test -v -tags=integration -run TestDockerSuiteSimplest

type DockerSuiteSimplest struct {
	test.BaseDockerSuite
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
// this is the entry point for the suite
func TestDockerSuiteSimplest(t *testing.T) {
	ourSuite := new(DockerSuiteSimplest)
	suite.Run(t, ourSuite)

	ourSuite.EqualValues(ourSuite.Total.ExpectedRowcount, ourSuite.Total.ActualRowCount)
	ourSuite.EqualValues(0, len(ourSuite.Total.FailedChildren))
}

func (suite *DockerSuiteSimplest) TestOne() { // just do the setup and teardown
	suite.EqualValues(suite.Total.ExpectedRowcount, suite.Total.ActualRowCount)
	suite.EqualValues(0, len(suite.Total.FailedChildren))
}

func (suite *DockerSuiteSimplest) SetupSuite() {

	// stop and remove all containers
	test.Shell("docker stop $(docker ps -aq)", "")
	test.Shell("docker rm $(docker ps -aq)", "")

	suite.SetupDockerCluster(3, 1)

}

func (suite *DockerSuiteSimplest) TearDownSuite() {
	// leave the cluster running
	// or:
	// stop and remove all containers
	test.Shell("docker stop $(docker ps -aq)", "")
	test.Shell("docker rm $(docker ps -aq)", "")

}
