package test_integration

import (
	"fmt"
	"strings"
	"testing"

	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/test"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/suite"
)

// Requirements: Consul must NOT be running on localhost:8500 we will start our own

type NodeStabilitySuite31 struct {
	test.BaseDockerSuite
}

func (suite *NodeStabilitySuite31) TestOne() { // just do the setup and teardown
	suite.EqualValues(suite.Total.ExpectedRowcount, suite.Total.ActualRowCount)
	suite.EqualValues(0, len(suite.Total.FailedChildren))
}

func (suite *NodeStabilitySuite31) SetupSuite() {

	suite.SetupDockerCluster(3, 2)
}

func (suite *NodeStabilitySuite31) TearDownSuite() {
	// leave the cluster running
	// if you wish to stop the cluster, do that in docker
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestNodeStability31Suite(t *testing.T) {
	ourSuite := new(NodeStabilitySuite31)
	ourSuite.ProxyAddress = make([]string, 2)
	suite.Run(t, ourSuite)

	ourSuite.EqualValues(ourSuite.Total.ExpectedRowcount, ourSuite.Total.ActualRowCount)
	ourSuite.EqualValues(0, len(ourSuite.Total.FailedChildren))
}

func addANode(suite *test.BaseDockerSuite, index int) {
	i := fmt.Sprintf("%d", index)
	// start the node as necessary
	// quanta-node is the entrypoint, node is the image
	// q-node-0 ./data-dir 0.0.0.0 4000 are the args

	pprofPortMap := ""

	options := "-d --network mynet" + pprofPortMap + " --name q-node-" + i + " -t node"
	cmd := "docker run " + options + " quanta-node --consul-endpoint " + suite.ConsulAddress + ":8500  q-node-" + i + " ./data-dir 0.0.0.0 4000"

	out, err := test.Shell(cmd, "")
	// check(err)
	fmt.Println("docker node command", cmd)
	fmt.Println("docker run node", out, err)
}

// TestBasic3Plus1 runs a basic test with 3 nodes, then removes adds 1 node and runs the test again
// btw this test is broken
func (suite *NodeStabilitySuite31) TestBasic3Plus1() {

	fmt.Println("NodeStabilitySuite TestBasic3Plus1")

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

	cmd = "docker run --name basic_queries0 -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/basic_queries_body.sql"
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

	// now add a node
	addANode(&suite.BaseDockerSuite, 3) // add q-node-3

	// time.Sleep(15 * time.Second) // let things  settle down
	localConsulAddress := "127.0.0.1" // we have to use the mapping when we're outside the container
	consulClient, err := api.NewClient(&api.Config{Address: localConsulAddress + ":8500"})
	check(err)
	err = shared.SetClusterSizeTarget(consulClient, 4)
	check(err)

	// Wait for the cluster to ge GREEN
	test.WaitForStatusGreen(suite.ConsulAddress+":8500", "q-node-0")
	// time.Sleep(10 * time.Second)

	test.StopAndRemoveContainer("basic_queries0")

	cmd = "docker run --name basic_queries0 -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/basic_queries_body.sql"
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

	test.StopAndRemoveContainer("q-node-3")
}

// TestBasicOneTwo is a baseline test that should always pass
func (suite *NodeStabilitySuite31) TestBasicOneTwo() {

	fmt.Println("NodeStabilitySuite TestBasicOneTwo")

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

	cmd = "docker run --name basic_queries0 -w /quanta/sqlrunner --network mynet -t node sqlrunner -script_file ./sqlscripts/basic_queries_body.sql"
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
}
