package test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	admin "github.com/disney/quanta/quanta-admin-lib"
	"github.com/disney/quanta/shared"

	"github.com/stretchr/testify/assert"
)

// This requires that consul is running on localhost:8500
// If a cluster is running it will use that cluster.
// If not it will start a cluster.

func TestLocalQuery(t *testing.T) {

	acquirePort4000.Lock()
	defer acquirePort4000.Unlock()
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
func TestIsNull(t *testing.T) {

	acquirePort4000.Lock()
	defer acquirePort4000.Unlock()
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

func TestIsNotNull(t *testing.T) {

	acquirePort4000.Lock()
	defer acquirePort4000.Unlock()
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

func TestSpellTypeWrong(t *testing.T) {

	acquirePort4000.Lock()
	defer acquirePort4000.Unlock()
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

	ctx := admin.Context{ConsulAddr: "localhost:8500", Port: 4000}

	cmd := admin.TablesCmd{}
	cmd.Run(&ctx)

	verify := admin.VerifyCmd{Table: "customers_qa", Field: "first_name"}
	verify.Run(&ctx)

	enum := admin.VerifyEnumCmd{Table: "customers_qa", Field: "first_name"}
	enum.Run(&ctx)

	vi := admin.VerifyIndexCmd{Table: "customers_qa"}
	vi.Run(&ctx)

	drop := admin.DropCmd{Table: "dummytable"}
	drop.Run(&ctx)

	create := admin.CreateCmd{Table: "dummytable"}
	create.SchemaDir = "../test/testdata/config/"
	err = create.Run(&ctx)
	assert.NotEqual(t, nil, err) // <-  this is the test. It returns an error because "string" is not a valid type.

	cmd = admin.TablesCmd{}
	cmd.Run(&ctx)

	// conn := admin.GetClientConnection(ctx.ConsulAddr, ctx.Port)
	// table, err := shared.LoadSchema("", "dummytable", conn.Consul)
	// check(err)

	// for i := 0; i < len(table.Attributes); i++ {
	// 	fmt.Println(table.Attributes[i].FieldName, table.Attributes[i].Type)
	// }

	// assert.EqualValues(t, "String", table.Attributes[0].Type)

	// find := admin.FindKeyCmd{Table: "dummytable", Field: "f1"}
	// find.Run(&ctx)

	// verify = admin.VerifyCmd{Table: "dummytable", Field: "f1"}
	// verify.Run(&ctx)

	// enum = admin.VerifyEnumCmd{Table: "dummytable", Field: "f1"}
	// enum.Run(&ctx)

	// vi = admin.VerifyIndexCmd{Table: "dummytable"}
	// vi.Run(&ctx)

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
