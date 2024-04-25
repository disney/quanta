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

	fmt.Println("TestBasic")
	currentDir, err := os.Getwd()
	check(err)
	err = os.Chdir("../sqlrunner") // these run from the sqlrunner/ directory
	check(err)
	defer os.Chdir(currentDir)

	got := test.ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries.sql")

	for _, child := range got.FailedChildren {
		fmt.Println("child failed", child.Statement)
	}

	assert.Equal(t, got.ExpectedRowcount, got.ActualRowCount)
	assert.Equal(t, 0, len(got.FailedChildren))

	state.Release()
	// FIXME: see: select avg(age) as avg_age from customers_qa where age > 55 and avg_age = 70 limit 1; in the file
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
	currentDir, err := os.Getwd()
	check(err)
	err = os.Chdir("../sqlrunner") // these run from the sqlrunner/ directory
	check(err)
	defer os.Chdir(currentDir)

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
	currentDir, err := os.Getwd()
	check(err)
	err = os.Chdir("../sqlrunner") // these run from the sqlrunner/ directory
	check(err)
	defer os.Chdir(currentDir)

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
		fmt.Println("test-integration check err", err)
		// no panic(err.Error())
	}
}
