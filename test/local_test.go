package test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/disney/quanta/qlbridge/expr"
	"github.com/disney/quanta/qlbridge/lex"
	"github.com/disney/quanta/qlbridge/rel"
	admin "github.com/disney/quanta/quanta-admin-lib"
	"github.com/disney/quanta/shared"

	"github.com/stretchr/testify/assert"
)

// This requires that consul is running on localhost:8500
// If a cluster is running it will use that cluster.
// If not it will start a cluster.

func TestCreateView(t *testing.T) {

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

	fmt.Println("tables:", GetTableNames())

	if !isLocalRunning { // if no cluster was up, load some data
		executeSqlFile(state, "../sqlrunner/sqlscripts/basic_load.sql")
	} // else assume it's already loaded

	fmt.Println("tables 2:", GetTableNames())

	// create a view
	q := "CREATE VIEW aView AS SELECT cust_id,first_name,last_name FROM customers_qa WHERE last_name IS not null"
	AnalyzeRow(*state.proxyConnect, []string{q}, true)

	// check the expression
	got := AnalyzeRow(*state.proxyConnect, []string{"SELECT cust_id,first_name,last_name FROM aView WHERE last_name != NULL"}, true)
	fmt.Println("selected from aView count", got.ActualRowCount)
	assert.EqualValues(t, 14, got.ActualRowCount)

	fmt.Println("tables 3:", GetTableNames())

	// pairs, meta, err := state.m0.Conn.Consul.KV().List("schema", nil)
	// check(err)
	// _ = meta
	// for _, p := range pairs {
	// 	fmt.Println("schema", string(p.Key), string(p.Value))
	// }

	// check consul for the view.
	pair, _, err := state.m0.Conn.Consul.KV().Get("quantaviews/views/aView", nil) // delete me. It's in the schema now.
	check(err)
	wanted := "SELECT cust_id,first_name,last_name FROM customers_qa WHERE last_name IS not null"
	assert.EqualValues(t, nil, err)
	assert.EqualValues(t, wanted, string(pair.Value))

	// check that every node knows about the view TODO:
	// it's not in the cache yet. It's in consul.

	// use the view. Selects 13 rows
	got = AnalyzeRow(*state.proxyConnect, []string{"SELECT cust_id,first_name,last_name FROM aView WHERE first_name != NULL"}, true)
	fmt.Println("selected from aView count", got.ActualRowCount)
	assert.EqualValues(t, 13, got.ActualRowCount)

	// for {
	// 	fmt.Println("Sleeping")
	// 	time.Sleep(9 * time.Second)
	// }

	// todo: CREATE OR REPLACE VIEW, [] syntax for view name, DROP VIEW, SELECT INTO

	// release as necessary
	state.Release()
}

// todo: CREATE TABLE ...

func TestParseSqlAndChangeWhere(t *testing.T) {

	l := lex.NewSqlLexer("SELECT x FROM y WHERE (x < 10) AND (x> 4);")
	for l.IsEnd() == false {
		fmt.Println("lexer", l.NextToken())
	}

	stmt, err := rel.ParseSql("SELECT x,i,j from y where (x < 10) AND (x> 4);")
	check(err)
	sql := stmt.(*rel.SqlSelect)
	fmt.Println("parser", sql)
	fmt.Println("parser Columns", sql.Columns)
	fmt.Println("parser where", sql.Where)
	fmt.Println("parser from", sql.From)

	stmt2, err := rel.ParseSql("SELECT x,i from y where (x < 10) ;")
	check(err)
	sql2 := stmt2.(*rel.SqlSelect)
	fmt.Println("parser", sql2)
	fmt.Println("parser Columns", sql2.Columns)
	fmt.Println("parser where", sql2.Where)
	fmt.Println("parser from", sql2.From)
	xlt10 := sql2.Where
	_ = xlt10

	stmt4, err := rel.ParseSql("SELECT x,i,j from y where ((x> 4));")
	check(err)
	sql4 := stmt4.(*rel.SqlSelect)
	prevWhere := sql4.Where
	_ = prevWhere
	atok := lex.Token{}
	atok.T = lex.TokenLogicAnd
	atok.V = "AND"
	and := &expr.BinaryNode{} //Left: xlt10, Right: sql4.Where, Operator: lex.TokenAnd}
	and.Operator = atok
	and.Operator.T = lex.TokenAnd
	and.Args = []expr.Node{xlt10.Expr, sql4.Where.Expr}

	fmt.Println("and expr", and)

	sql4.Where.Expr = and
	fmt.Println("parser", sql4)
	want := "SELECT x, i, j FROM y WHERE (x < 10) AND (x > 4)"
	got := sql4.String()
	assert.EqualValues(t, want, got)
}

// delete or finish:
func TestSelectInto(t *testing.T) {

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

	fmt.Println("tables 1:", GetTableNames())

	// new table
	AnalyzeRow(*state.proxyConnect, []string{"quanta-admin drop newtable"}, true)
	AnalyzeRow(*state.proxyConnect, []string{"quanta-admin create newtable"}, true)

	fmt.Println("tables 2:", GetTableNames())

	q := "INSERT into newtable (cust_id, first_name, last_name) values('101','Al','woo')  "
	AnalyzeRow(*state.proxyConnect, []string{q}, true)

	q = "SELECT * FROM newtable;@123"
	AnalyzeRow(*state.proxyConnect, []string{q}, true)

	fmt.Println("tables 3:", GetTableNames())

	// for {
	// 	fmt.Println("Sleeping")
	// 	time.Sleep(9 * time.Second)
	// }

	// select into it

	// q = "SELECT cust_id,first_name,last_name INTO newtable FROM customers_qa WHERE last_name IS not null"
	// AnalyzeRow(*state.proxyConnect, []string{q}, true)

	// select from newtable
	q = "SELECT * FROM newtable;@1234"
	AnalyzeRow(*state.proxyConnect, []string{q}, true)

	// release as necessary
	state.Release()
}

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
	assert.EqualValues(t, 13, count)

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

func GetTableNames() []string {
	ctx := admin.Context{ConsulAddr: "localhost:8500", Port: 4000}
	tableNames, err := admin.GetTableNames(&ctx)
	check(err)
	return tableNames
}
