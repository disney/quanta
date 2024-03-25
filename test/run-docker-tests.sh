#!/bin/bash -e


# these are system tests using a cluster of docker containers.

# consul must NOT be running in a terminal. 
# docker must be running.
# the tests will stop and remove ALL containers, create the cluster, run the tests, and remove the containers.

sleep 1
echo "starting TestDockerSuiteSimplest"
go test -timeout 200s -run ^TestDockerSuiteSimplest$  github.com/disney/quanta/test-integration-docker

sleep 1
echo "starting TestDockerNodesRunnerSuite"
# FIXME: has errors go test -timeout 200s -run ^TestDockerNodesRunnerSuite$  github.com/disney/quanta/test-integration-docker

sleep 1
echo "starting TestDockerSuiteRestart"
go test -timeout 200s -run ^TestDockerSuiteRestart$  github.com/disney/quanta/test-integration-docker

sleep 1
echo "starting NodeStabilitySuite31"
# FIXME: has errors go test -timeout 500s -run ^TestNodeStability31Suite$  github.com/disney/quanta/test-integration-docker

sleep 1
echo "starting NodeStabilitySuite41"
# FIXME: has errors go test -timeout 500s -run ^TestNodeStabilitySuite41$  github.com/disney/quanta/test-integration-docker





