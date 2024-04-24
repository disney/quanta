package test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

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

// This should work from scratch with nothing running except consul.
// It should also work with a cluster already running and basic_queries.sql loaded which is MUCH faster.
// eg. go run ./driver.go -script_file ./sqlscripts/basic_queries.sql -validate -host 127.0.0.1 -user MOLIG004 -db quanta -port 4000 -log_level DEBUG

func TestFirstName(t *testing.T) {

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	isLocalRunning := IsLocalRunning()
	// erase the storage
	if !isLocalRunning { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster(3)

	if !isLocalRunning { // if no cluster was up, load some data
		ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_load.sql")
	} // else assume it's already loaded

	// query
	{
		statement := "select * from customers_qa where city = 'Seattle'"
		rows, err := state.Db.Query(statement)
		check(err)

		rowsArr, err := shared.GetAllRows(rows)
		check(err)
		fmt.Println("count", len(rowsArr), rowsArr)
		assert.EqualValues(t, 3, len(rowsArr))
	}
	{
		statement := "select first_name from customers_qa;"
		rows, err := state.Db.Query(statement)
		check(err)

		count := 0
		for rows.Next() {
			columns, err := rows.Columns()
			check(err)
			_ = columns
			//fmt.Println(columns)
			id, firstName := "", ""
			err = rows.Scan(&firstName)
			check(err)
			fmt.Println(id, firstName)
			count += 1
		}
		fmt.Println("count", count)
		assert.EqualValues(t, 30, count)
	}

	// release as necessary
	state.Release()
}

// TestReplication will check that data is written 2 places.
func TestReplication(t *testing.T) {

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	// erase the storage
	if !IsLocalRunning() { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	} else {
		fmt.Println("FAIL cluster already running")
		t.FailNow()
	}
	// ensure_custer
	state := Ensure_cluster(3)
	state.Db, err = state.ProxyConnect.ProxyConnectConnect()
	_ = err

	// load something

	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin drop orders_qa"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin drop customers_qa"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin create customers_qa"}, true)

	AnalyzeRow(*state.ProxyConnect, []string{"insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType,age,rownum) values('101','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home','33','11');"}, true)

	got := AnalyzeRow(*state.ProxyConnect, []string{"select cust_id,first_name,city,age,rownum from customers_qa where cust_id != NULL ;@1"}, true)
	for i := 0; i < len(got.RowDataArray); i++ {
		json, err := json.Marshal(got.RowDataArray[i])
		check(err)
		fmt.Printf("%v\n", string(json))
	}

	vectors := []string{"customers_qa/cust_id/1970-01-01T00"}
	DumpField(t, state, vectors) // see the mapping, check for replication.

	vectors = []string{"customers_qa/first_name/1970-01-01T00"}
	DumpField(t, state, vectors) // see the mapping, check for replication.

	vectors = []string{"customers_qa/age/1970-01-01T00"}
	DumpField(t, state, vectors) // see the mapping, check for replication.

	vectors = []string{"customers_qa/rownum/1970-01-01T00"}
	DumpField(t, state, vectors) // see the mapping, check for replication.

	state.Release()
}

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

func TestLocalQuery(t *testing.T) {

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	// erase the storage
	if !IsLocalRunning() { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster(3)

	// load something

	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin drop orders_qa"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin drop customers_qa"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin create customers_qa"}, true)

	AnalyzeRow(*state.ProxyConnect, []string{"insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('101','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');"}, true)

	time.Sleep(5 * time.Second)
	// query

	got := AnalyzeRow(*state.ProxyConnect, []string{"select * from customers_qa;@1"}, true)

	assert.EqualValues(t, got.ExpectedRowcount, got.ActualRowCount)

	// release as necessary
	state.Release()
}

// This should work from scratch with nothing running except consul.
// It should also work with a cluster already running and basic_queries.sql loaded which is MUCH faster.
// eg. go run ./driver.go -script_file ./sqlscripts/basic_queries.sql -validate -host 127.0.0.1 -user MOLIG004 -db quanta -port 4000 -log_level DEBUG
func TestIsNull(t *testing.T) {

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	isLocalRunning := IsLocalRunning()
	// erase the storage
	if !isLocalRunning { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster(3)

	if !isLocalRunning { // if no cluster was up, load some data
		ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_load.sql")
	} // else assume it's already loaded

	// query

	statement := "select cust_id,first_name,last_name from customers_qa where last_name is null;"
	rows, err := state.Db.Query(statement)
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

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	isLocalRunning := IsLocalRunning()
	// erase the storage
	if !isLocalRunning { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster(3)

	if !isLocalRunning { // if no cluster was up, load some data
		ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_load.sql")
	} // else assume it's already loaded

	// query

	statement := "select cust_id,first_name,last_name from customers_qa where last_name is not null;"
	rows, err := state.Db.Query(statement)
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

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	isLocalRunning := IsLocalRunning()
	// erase the storage
	if !isLocalRunning { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster(3)

	if !isLocalRunning { // if no cluster was up, load some data
		ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_load.sql")
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

func TestAvgAge(t *testing.T) {

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	isLocalRunning := IsLocalRunning()
	// erase the storage
	if !isLocalRunning { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster(3)

	if !isLocalRunning { // if no cluster was up, load some data
		ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_load.sql")
	} // else assume it's already loaded

	// query

	// statement := "select avg(age) as avg_age from customers_qa"
	statement := "select avg(age) as avg_age from customers_qa"
	rows, err := state.Db.Query(statement)
	check(err)

	count := 0
	for rows.Next() {
		columns, err := rows.Columns()
		check(err)
		_ = columns
		//fmt.Println(columns)
		avg_age := 0
		err = rows.Scan(&avg_age)
		check(err)
		fmt.Println(avg_age) // 46
		count += 1
	}
	fmt.Println("rows", count)
	assert.EqualValues(t, 1, count)

	// release as necessary
	state.Release()
}

func GetTableNames() []string {
	ctx := admin.Context{ConsulAddr: "localhost:8500", Port: 4000}
	tableNames, err := admin.GetTableNames(&ctx)
	check(err)
	return tableNames
}
