package test

import (
	"fmt"
	"strings"

	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/suite"
)

type BaseDockerSuite struct {
	suite.Suite
	State *ClusterLocalState
	Total SqlInfo

	ConsulAddress string // the address in the docker network
	ProxyAddress  []string
}

// SetupDockerCluster is the
func (suite *BaseDockerSuite) SetupDockerCluster(nodeCount int, proxyCount int) {

	var err error
	var out string

	suite.ProxyAddress = make([]string, proxyCount)

	somethingRestarted := false

	proxyConnect := ProxyConnectStrings{}
	proxyConnect.Host = "127.0.0.1"
	proxyConnect.User = "MOLIG004"
	proxyConnect.Password = ""
	proxyConnect.Port = "4000"
	proxyConnect.Database = "quanta"

	suite.State = &ClusterLocalState{}
	suite.State.ProxyConnect = &proxyConnect
	suite.State.Db, err = suite.State.ProxyConnect.ProxyConnectConnect()
	check(err)

	// check if consul is running
	if !IsConsuleRunning() {
		Sh("docker network rm mynet")
		Sh("docker network create -d bridge --subnet=172.20.0.0/16 mynet")
		Sh("docker run -d -p 8500:8500 -p 8600:8600/udp --network mynet	--name=myConsul consul:1.10 agent -dev -ui -client=0.0.0.0")
	}
	// get the IP address of the consul container --format {{.NetworkSettings.Networks.mynet.IPAddress}}
	out, err = Shell("docker inspect --format {{.NetworkSettings.Networks.mynet.IPAddress}} myConsul", "")
	fmt.Println("docker inspect myConsul", out, err)
	suite.ConsulAddress = strings.TrimSpace(out)

	// check if there's a new build of the node image
	out, err = Shell("docker inspect --format {{.Id}} node", "")
	if err != nil {
		fmt.Println("docker inspect node", err, out)
		// check(err)
		out = ""
	} else {
		fmt.Println("docker inspect node", out)
	}

	// build the image, as necessary
	beforeSha := out
	out, err = Shell("docker build -t node -f ../test/docker-nodes/Dockerfile ../", "")
	_ = out
	check(err)
	out, err = Shell("docker inspect --format {{.Id}} node", "")
	check(err)
	imageChanged := out != beforeSha // if the sha changed, we need to restart the nodes
	fmt.Println("imageChanged", imageChanged)

	// check the nodes and see if we need to start/restart them
	nodeToPprof := -1 // to set pprof on a node, set this to the index
	for index := 0; index < nodeCount; index++ {
		i := fmt.Sprintf("%d", index)
		// check node is running
		out, err = Shell("docker exec q-node-"+i+" pwd", "")
		itsUp := false
		if err == nil {
			itsUp = out == "/quanta\n"
		}
		if itsUp && imageChanged {
			StopAndRemoveContainer("q-node-" + i)
		}
		if !itsUp || imageChanged { // start the node as necessary
			// quanta-node is the entrypoint, node is the image
			// q-node-0 ./data-dir 0.0.0.0 4000 are the args

			somethingRestarted = true

			pprofPortMap := ""
			if index == nodeToPprof {
				pprofPortMap = " -p 6060:6060"
			}
			options := "-d --network mynet" + pprofPortMap + " --name q-node-" + i + " -t node"
			cmd := "docker run " + options + " quanta-node --consul-endpoint " + suite.ConsulAddress + ":8500  q-node-" + i + " ./data-dir 0.0.0.0 4000"
			if index == nodeToPprof {
				cmd += " --pprof true"
			}
			out, err := Shell(cmd, "")
			// check(err)
			fmt.Println("docker node command", cmd)
			fmt.Println("docker run node", out, err)
		}
	}

	// check if the nodes are up
	localConsulAddress := "127.0.0.1" // we have to use the mapping when we're outside the container
	consulClient, err := api.NewClient(&api.Config{Address: localConsulAddress + ":8500"})
	check(err)
	err = shared.SetClusterSizeTarget(consulClient, nodeCount)
	check(err)

	// Wait for the nodes to come up
	// if somethingRestarted {
	fmt.Println("WaitForStatusGreen")
	WaitForStatusGreen(suite.ConsulAddress+":8500", "q-node-0") // does this even work? Why not?
	// time.Sleep(10 * time.Second)
	// }

	// check the PROXIES and see if we need to start/restart them
	proxyToPprof := -1 // to set pprof on a node, set this to the index
	for index := 0; index < len(suite.ProxyAddress); index++ {
		i := fmt.Sprintf("%d", index)
		// check node is running, quanta-proxy
		out, err = Shell("docker exec quanta-proxy-"+i+" pwd", "")
		itsUp := false
		if err == nil {
			itsUp = out == "/quanta\n"
		}
		if itsUp && imageChanged {
			StopAndRemoveContainer("quanta-proxy-" + i)
		}
		if !itsUp || imageChanged { // start the proxy as necessary
			somethingRestarted = true
			// quanta-proxy is the entrypoint, node is the image
			// --consul-endpoint 172.20.0.2:8500 are the args
			pprofPortMap := ""
			if index == proxyToPprof {
				pprofPortMap = " -p 6060:6060"
			}
			port := fmt.Sprintf("%d", 4000+index)
			options := "-d -p " + port + ":4000" + pprofPortMap + " --network mynet --name quanta-proxy-" + i + " -t node"
			cmd := "docker run " + options + " quanta-proxy --consul-endpoint " + suite.ConsulAddress + ":8500"
			if index == proxyToPprof {
				cmd += " --pprof true"
			}
			out, err := Shell(cmd, "")
			// check(err)
			fmt.Println("docker proxy command", cmd)
			fmt.Println("docker run", out, err)
		}
	}

	if somethingRestarted {
		//time.Sleep(10 * time.Second)
		WaitForStatusGreen(suite.ConsulAddress+":8500", "q-node-0") // does this even work? Why not?
	}
	// tode check if the proxies are up

	for index := 0; index < len(suite.ProxyAddress); index++ {
		istr := fmt.Sprintf("%d", index)
		out, err = Shell("docker inspect --format {{.NetworkSettings.Networks.mynet.IPAddress}} quanta-proxy-"+istr, "")
		fmt.Println("docker inspect quanta-proxy", out, err)
		suite.ProxyAddress[index] = strings.TrimSpace(out)
		if suite.ProxyAddress[index] == "" {
			suite.Fail("FAIL ProxyAddress is empty")
			suite.Fail("FAIL ProxyAddress is empty")
			suite.Fail("FAIL ProxyAddress is empty")
		}
	}
	// if somethingRestarted {
	// 	// time.Sleep(5 * time.Second)
	// }

}

// StopAndRemoveContainer stops and removes a docker container. Makes annoying error messages sometimes.
func StopAndRemoveContainer(container string) {

	SuperQuiet = true

	list, _ := Shell(`docker ps -a --format "{{.Names}}, {{.Status}}"`, "")
	// eg.
	// basic_queries0, Exited (0) 9 minutes ago
	// or
	// quanta-proxy-1, Up 9 minutes
	// etc
	if strings.Contains(list, container) {
		Sh("docker stop " + container)
		Sh("docker rm " + container)
	}
	SuperQuiet = false
}
