package test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/disney/quanta/shared"

	"github.com/stretchr/testify/assert"
)

// This requires that consul is running on localhost:8500
// If a cluster is running it will use that cluster.
// If not it will start a cluster.

func TestLocalQuery(t *testing.T) {

	var err error

	shared.SetUTCdefault()

	// erase the storage
	if !IsLocalRunning() { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster()

	// load something

	AnalyzeRow(*state.proxyConnect, []string{"quanta-admin drop orders_qa"}, true)
	AnalyzeRow(*state.proxyConnect, []string{"quanta-admin drop customers_qa"}, true)
	AnalyzeRow(*state.proxyConnect, []string{"quanta-admin create customers_qa"}, true)

	AnalyzeRow(*state.proxyConnect, []string{"insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('101','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');"}, true)

	// query

	AnalyzeRow(*state.proxyConnect, []string{"select * from customers_qa;@1"}, true)

	assert.EqualValues(t, 0, FailCount) // FailCount in sql-types.go

	// release as necessary
	state.Release()
}

// This should work from scratch with nothing running except consul.
// It should also work with a cluster already running and basic_queries.sql loaded which is MUCH faster.
// eg. go run ./driver.go -script_file ./sqlscripts/basic_queries.sql -validate -host 127.0.0.1 -user MOLIG004 -db quanta -port 4000 -log_level DEBUG
func XTestIsNull(t *testing.T) {

	var err error

	shared.SetUTCdefault()

	isLocalRunning := IsLocalRunning()
	// erase the storage
	if !isLocalRunning { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster()

	if !isLocalRunning { // if no cluster was up, load some data
		executeSqlFile(state, "../sqlrunner/sqlscripts/basic_load.sql")
	} // else assume it's already loaded

	// query

	statement := "select cust_id,first_name,last_name from customers_qa where last_name is null;"
	rows, err := state.db.Query(statement)
	check(err)

	count := 0
	for rows.Next() {
		columns, err := rows.Columns()
		check(err)
		_ = columns
		//fmt.Println(columns)
		id, firstName, lastName := "", "", ""
		err = rows.Scan(&id, &firstName, &lastName)
		check(err)
		fmt.Println(id, firstName, lastName)
		count += 1
	}
	fmt.Println("count", count)
	assert.EqualValues(t, 16, count)

	// release as necessary
	state.Release()
}

func XTestIsNotNull(t *testing.T) {

	var err error

	shared.SetUTCdefault()

	isLocalRunning := IsLocalRunning()
	// erase the storage
	if !isLocalRunning { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster()

	if !isLocalRunning { // if no cluster was up, load some data
		executeSqlFile(state, "../sqlrunner/sqlscripts/basic_load.sql")
	} // else assume it's already loaded

	// query

	//statement := "select cust_id,first_name,last_name from customers_qa where last_name != null;"
	statement := "select cust_id,first_name,last_name from customers_qa where last_name is not null;"
	rows, err := state.db.Query(statement)
	check(err)

	count := 0
	for rows.Next() {
		columns, err := rows.Columns()
		check(err)
		_ = columns
		//fmt.Println(columns)
		id, firstName, lastName := "", "", ""
		err = rows.Scan(&id, &firstName, &lastName)
		check(err)
		fmt.Println(id, firstName, lastName)
		count += 1
	}
	fmt.Println("count", count)
	assert.EqualValues(t, 14, count)

	// release as necessary
	state.Release()
}

func executeSqlFile(state *ClusterLocalState, filename string) {
	bytes, err := os.ReadFile(filename)
	check(err)
	sql := string(bytes)
	lines := strings.Split(sql, "\n")
	for _, line := range lines {
		AnalyzeRow(*state.proxyConnect, []string{line}, true)
	}
}
