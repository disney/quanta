package test_integration

import (
	"fmt"
	"testing"

	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/test"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/suite"
)

// Requirements: Consul must NOT be running on localhost:8500 we will start our own

type NodeStabilitySuite struct {
	test.BaseDockerSuite
}

func (suite *NodeStabilitySuite) TestOne() { // just do the setup and teardown
	suite.EqualValues(suite.Total.ExpectedRowcount, suite.Total.ActualRowCount)
	suite.EqualValues(0, len(suite.Total.FailedChildren))
}

func (suite *NodeStabilitySuite) SetupSuite() {

	suite.SetupDockerCluster(4, 2)
}

func (suite *NodeStabilitySuite) TearDownSuite() {
	// leave the cluster running
	// if you wish to stop the cluster, do that in docker
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestNodeStabilitySuite(t *testing.T) {
	ourSuite := new(NodeStabilitySuite)
	ourSuite.ProxyAddress = make([]string, 2)
	suite.Run(t, ourSuite)

	ourSuite.EqualValues(ourSuite.Total.ExpectedRowcount, ourSuite.Total.ActualRowCount)
	ourSuite.EqualValues(0, len(ourSuite.Total.FailedChildren))
}

// TestBasic4minus1 runs a basic test with 4 nodes, then removes one node and runs the test again
func (suite *NodeStabilitySuite) TestBasic4minus1() {

	fmt.Println("NodeStabilitySuite TestBasic4minus1")

	// time.Sleep(10 * time.Second)

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

	// time.Sleep(5 * time.Second)

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

	test.StopAndRemoveContainer("q-node-1")

	test.StopAndRemoveContainer("basic_queries0")

	// time.Sleep(15 * time.Second) // let things  settle down
	localConsulAddress := "127.0.0.1" // we have to use the mapping when we're outside the container
	consulClient, err := api.NewClient(&api.Config{Address: localConsulAddress + ":8500"})
	check(err)
	err = shared.SetClusterSizeTarget(consulClient, 3)
	check(err)

	// Wait for the nodes to come up
	test.WaitForStatusGreen(suite.ConsulAddress+":8500", "q-node-0") // does this even work? Why not?
	// time.Sleep(10 * time.Second)

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

// TestBasic4minus1AndBack will run a basic test with 4 nodes,
// then remove one node and run the test again, then add the node back and run the test again
func (suite *NodeStabilitySuite) TestBasic4minus1AndBack() {

	fmt.Println("NodeStabilitySuite TestBasic4minus1AndBack")

	// time.Sleep(10 * time.Second)

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

	// time.Sleep(5 * time.Second)

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

	test.StopAndRemoveContainer("q-node-1")

	test.StopAndRemoveContainer("basic_queries0")

	// time.Sleep(15 * time.Second) // let things  settle down
	localConsulAddress := "127.0.0.1" // we have to use the mapping when we're outside the container
	consulClient, err := api.NewClient(&api.Config{Address: localConsulAddress + ":8500"})
	check(err)
	err = shared.SetClusterSizeTarget(consulClient, 3)
	check(err)

	// Wait for the nodes to come up
	test.WaitForStatusGreen(suite.ConsulAddress+":8500", "q-node-0") // does this even work? Why not?
	// time.Sleep(10 * time.Second)

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

	addANode(&suite.BaseDockerSuite, 1) // add q-node-1
	err = shared.SetClusterSizeTarget(consulClient, 4)
	check(err)

	test.StopAndRemoveContainer("basic_queries0")

	// Wait for the nodes to come up
	test.WaitForStatusGreen(suite.ConsulAddress+":8500", "q-node-0") // does this even work? Why not?
	// time.Sleep(10 * time.Second)

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

// TestBasicOneTwo is a baseline test that should always pass
func (suite *NodeStabilitySuite) TestBasicOneTwo() {

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
