package test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	admin "github.com/disney/quanta/quanta-admin-lib"
	proxy "github.com/disney/quanta/quanta-proxy-lib"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/shared"

	"github.com/stretchr/testify/assert"
)

// This requires that consul is running on localhost:8500
// A cluster should NOT be already running.

// TestTableMod_reload_table tries to remove the whole table and then load it again.
func TestTableMod_reload_table(t *testing.T) {

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	if IsLocalRunning() {
		assert.Fail(t, "This test should not be run with a cluster running")
	}

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
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin create --schema-dir=../sqlrunner/config customers_qa"}, true)

	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf("insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('%d','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');", i)
		AnalyzeRow(*state.ProxyConnect, []string{sql}, true)
	}
	AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)
	time.Sleep(1 * time.Second)
	// query

	got := AnalyzeRow(*state.ProxyConnect, []string{"select cust_id from customers_qa"}, true)
	for i := 0; i < len(got.RowDataArray); i++ {
		json, err := json.Marshal(got.RowDataArray[i])
		check(err)
		fmt.Printf("%v\n", string(json))
	}
	assert.EqualValues(t, 10, got.ActualRowCount)

	// get the schema from admin
	// from  shared.GetTables(consulClient)
	ctx := admin.Context{ConsulAddr: "localhost:8500", Port: 4000}
	cmd := admin.TablesCmd{}
	out := captureStdout(func() {
		cmd.Run(&ctx) // sent to console
	})
	fmt.Println("initial tables", out)
	assert.Contains(t, out, "customers_qa")

	attrCount := CountAttrInTable(t, state, "customers_qa")
	assert.EqualValues(t, 20, attrCount)

	// delete the table
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin drop customers_qa"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)

	// add it back slightly different.
	createcmd := admin.CreateCmd{Table: "customers_qa-hashed_id", SchemaDir: "../sqlrunner/config", Confirm: false}
	createerr := createcmd.Run(&ctx)
	assert.Nil(t, createerr)
	fmt.Println(createerr)

	attrCount = CountAttrInTable(t, state, "customers_qa")
	assert.EqualValues(t, 19, attrCount) // we lost a column

	// is the data still there? It should be gone.
	got = AnalyzeRow(*state.ProxyConnect, []string{"select cust_id from customers_qa"}, true)
	for i := 0; i < len(got.RowDataArray); i++ {
		json, err := json.Marshal(got.RowDataArray[i])
		check(err)
		fmt.Printf("%v\n", string(json))
	}
	assert.EqualValues(t, 0, got.ActualRowCount)

	time.Sleep(10 * time.Second) // FIXME: every sleep is an admission of failure.

	// add more data
	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf("insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('%d','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');", i+100)
		AnalyzeRow(*state.ProxyConnect, []string{sql}, true)
	}
	AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)

	got = AnalyzeRow(*state.ProxyConnect, []string{"select cust_id from customers_qa"}, true)
	for i := 0; i < len(got.RowDataArray); i++ {
		json, err := json.Marshal(got.RowDataArray[i])
		check(err)
		fmt.Printf("%v\n", string(json))
	}
	assert.EqualValues(t, 10, int(got.ActualRowCount))

	// release as necessary
	state.Release()
}

// TestTableMod_change tries to change the default value of a column in a table.
func TestTableMod_change(t *testing.T) {

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	if IsLocalRunning() {
		assert.Fail(t, "This test should not be run with a cluster running")
	}

	// erase the storage
	if !IsLocalRunning() { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster(3)

	// load something

	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin drop customers_qa"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin create --schema-dir=../sqlrunner/config customers_qa"}, true)

	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf("insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('%d','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');", i)
		AnalyzeRow(*state.ProxyConnect, []string{sql}, true)
	}
	AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)
	// time.Sleep(1 * time.Second)
	// query

	got := AnalyzeRow(*state.ProxyConnect, []string{"select cust_id,hashedCustId from customers_qa;@10"}, true)
	minLen := 9999
	for i := 0; i < len(got.RowDataArray); i++ {
		json, err := json.Marshal(got.RowDataArray[i])
		check(err)
		tmp := got.RowDataArray[i]["hashedCustId"].(string)
		if len(tmp) < minLen {
			minLen = len(tmp)
		}
		fmt.Printf("%v\n", string(json))
	}
	assert.EqualValues(t, got.ExpectedRowcount, got.ActualRowCount)
	assert.EqualValues(t, minLen, 64) // 64 is the length of a sha256 hash

	// get the schema from admin
	// from  shared.GetTables(consulClient)
	ctx := admin.Context{ConsulAddr: "localhost:8500", Port: 4000}
	cmd := admin.TablesCmd{}
	out := captureStdout(func() {
		cmd.Run(&ctx) // sent to console
	})
	fmt.Println("initial tables", out)
	assert.Contains(t, out, "customers_qa")

	attrCount := CountAttrInTable(t, state, "customers_qa")
	assert.EqualValues(t, 20, attrCount)

	// change the table. change the defaultValue of a column.
	createcmd := admin.CreateCmd{Table: "customers_qa-mod-defaultValue", SchemaDir: "../sqlrunner/config", Confirm: true}
	err = createcmd.Run(&ctx)
	check(err)

	attrCount = CountAttrInTable(t, state, "customers_qa")
	assert.EqualValues(t, 20, attrCount) // not 21 and  not 19, still 20

	// is the data still there?
	// the first 10 are still hashed.
	isAll_hashed := true
	got = AnalyzeRow(*state.ProxyConnect, []string{"select cust_id,hashedCustId from customers_qa"}, true)
	for i := 0; i < len(got.RowDataArray); i++ {
		json, err := json.Marshal(got.RowDataArray[i])
		str := got.RowDataArray[i]["hashedCustId"].(string)
		if str == "not_hashed" {
			isAll_hashed = false
		}
		check(err)
		fmt.Printf("%v\n", string(json))
	}
	assert.True(t, isAll_hashed)
	assert.EqualValues(t, 10, got.ActualRowCount)
	AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)

	// add more data
	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf("insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('%d','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');", i+100)
		got := AnalyzeRow(*state.ProxyConnect, []string{sql}, true)
		check(got.Err)
	}
	AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)

	// now 10 rows are not 'not_hashed' and 10 are long hashes
	not_hashed_count := 0
	got = AnalyzeRow(*state.ProxyConnect, []string{"select cust_id,hashedCustId from customers_qa"}, true)
	for i := 0; i < len(got.RowDataArray); i++ {
		json, err := json.Marshal(got.RowDataArray[i])
		check(err)
		v := got.RowDataArray[i]["hashedCustId"]
		if v.(string) == "not_hashed" {
			not_hashed_count += 1
		}
		fmt.Printf("%v\n", string(json))
	}
	assert.EqualValues(t, 20, got.ActualRowCount)
	assert.EqualValues(t, 10, not_hashed_count)

	// release as necessary
	state.Release()
}

// TestTableMod_remove_column tries to remove a column from a table.
// This makes an error.  The column is not removed from the table.
func TestTableMod_remove_column(t *testing.T) {

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	if IsLocalRunning() {
		assert.Fail(t, "This test should not be run with a cluster running")
	}

	// erase the storage
	if !IsLocalRunning() { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster(3)

	// load something

	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin drop customers_qa"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin create --schema-dir=../sqlrunner/config customers_qa"}, true)

	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf("insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('%d','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');", i)
		AnalyzeRow(*state.ProxyConnect, []string{sql}, true)
	}
	AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)
	time.Sleep(1 * time.Second)
	// query

	got := AnalyzeRow(*state.ProxyConnect, []string{"select cust_id from customers_qa"}, true)
	for i := 0; i < len(got.RowDataArray); i++ {
		json, err := json.Marshal(got.RowDataArray[i])
		check(err)
		fmt.Printf("%v\n", string(json))
	}
	assert.EqualValues(t, 10, got.ActualRowCount)

	// get the schema from admin
	// from  shared.GetTables(consulClient)
	ctx := admin.Context{ConsulAddr: "localhost:8500", Port: 4000}
	cmd := admin.TablesCmd{}
	out := captureStdout(func() {
		cmd.Run(&ctx) // sent to console
	})
	fmt.Println("initial tables", out)
	assert.Contains(t, out, "customers_qa")

	attrCount := CountAttrInTable(t, state, "customers_qa")
	assert.EqualValues(t, 20, attrCount)

	// change the table. Add a column  this just calls admin.CreateCmd.Run - except we needs the confirm
	// AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin create customers_qa+middle_name"}, true)
	createcmd := admin.CreateCmd{Table: "customers_qa-hashed_id", SchemaDir: "../sqlrunner/config", Confirm: true}
	createerr := createcmd.Run(&ctx)
	assert.NotNil(t, createerr)
	fmt.Println(createerr)
	assert.Contains(t, createerr.Error(), "table attribute cannot be dropped")

	attrCount = CountAttrInTable(t, state, "customers_qa")
	assert.EqualValues(t, 20, attrCount) // NOT 19

	// is the data still there?
	got = AnalyzeRow(*state.ProxyConnect, []string{"select cust_id from customers_qa"}, true)
	for i := 0; i < len(got.RowDataArray); i++ {
		json, err := json.Marshal(got.RowDataArray[i])
		check(err)
		fmt.Printf("%v\n", string(json))
	}
	assert.EqualValues(t, 10, got.ActualRowCount)

	// add more data
	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf("insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('%d','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');", i+100)
		AnalyzeRow(*state.ProxyConnect, []string{sql}, true)
	}
	AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)

	got = AnalyzeRow(*state.ProxyConnect, []string{"select cust_id from customers_qa"}, true)
	for i := 0; i < len(got.RowDataArray); i++ {
		json, err := json.Marshal(got.RowDataArray[i])
		check(err)
		fmt.Printf("%v\n", string(json))
	}
	assert.EqualValues(t, 20, got.ActualRowCount)

	// release as necessary
	state.Release()
}

func TestTableMod_add(t *testing.T) {

	AcquirePort4000.Lock()
	defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	if IsLocalRunning() {
		assert.Fail(t, "This test should not be run with a cluster running")
	}

	// erase the storage
	if !IsLocalRunning() { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := Ensure_cluster(3)

	// load something

	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin drop customers_qa"}, true)
	AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin create --schema-dir=../sqlrunner/config customers_qa"}, true)

	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf("insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('%d','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');", i)
		AnalyzeRow(*state.ProxyConnect, []string{sql}, true)
	}
	AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)
	time.Sleep(1 * time.Second)
	// query

	got := AnalyzeRow(*state.ProxyConnect, []string{"select cust_id from customers_qa;@10"}, true)
	for i := 0; i < len(got.RowDataArray); i++ {
		fmt.Printf("%v\n", got.RowDataArray[i])
	}
	assert.EqualValues(t, got.ExpectedRowcount, got.ActualRowCount)

	// get the schema from admin
	// from  shared.GetTables(consulClient)
	ctx := admin.Context{ConsulAddr: "localhost:8500", Port: 4000}
	cmd := admin.TablesCmd{}
	out := captureStdout(func() {
		cmd.Run(&ctx) // sent to console
	})
	fmt.Println("initial tables", out)
	assert.Contains(t, out, "customers_qa")

	attrCount := CountAttrInTable(t, state, "customers_qa")
	assert.EqualValues(t, 20, attrCount)

	// change the table. Add a column  this just calls admin.CreateCmd.Run - except we needs the confirm
	// AnalyzeRow(*state.ProxyConnect, []string{"quanta-admin create customers_qa+middle_name"}, true)
	createcmd := admin.CreateCmd{Table: "customers_qa+middle_name", SchemaDir: "../sqlrunner/config", Confirm: true}
	createcmd.Run(&ctx)

	attrCount = CountAttrInTable(t, state, "customers_qa")
	assert.EqualValues(t, 21, attrCount)

	// is the data still there?
	got = AnalyzeRow(*state.ProxyConnect, []string{"select cust_id from customers_qa"}, true)
	for i := 0; i < len(got.RowDataArray); i++ {
		json, err := json.Marshal(got.RowDataArray[i])
		check(err)
		fmt.Printf("%v\n", string(json))
	}
	assert.EqualValues(t, 10, got.ActualRowCount)
	AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)
	time.Sleep(10 * time.Second)

	// add more data
	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf("insert into customers_qa (cust_id, first_name,middle_name, address, city, state, zip, phone, phoneType) values('%d','Abe','Lou','123 Main','Seattle','WA','98072','425-232-4323','cell;home');", i+500)
		AnalyzeRow(*state.ProxyConnect, []string{sql}, true)
	}
	AnalyzeRow(*state.ProxyConnect, []string{"commit"}, true)
	time.Sleep(10 * time.Second)

	got = AnalyzeRow(*state.ProxyConnect, []string{"select cust_id,middle_name from customers_qa"}, true)
	for i := 0; i < len(got.RowDataArray); i++ {
		json, err := json.Marshal(got.RowDataArray[i])
		check(err)
		fmt.Printf("%v\n", string(json))
	}
	assert.EqualValues(t, 20, got.ActualRowCount)

	// release as necessary
	state.Release()
}

func CountAttrInTable(t *testing.T, state *ClusterLocalState, tableName string) int {
	count := 0
	for _, node := range state.nodes {
		bmi := node.GetNodeService("BitmapIndex")
		bitmap := bmi.(*server.BitmapIndex)
		table := bitmap.GetTable(tableName)
		if count == 0 {
			count = len(table.Attributes)
		} else {
			if count != len(table.Attributes) {
				fmt.Printf("Table %s has different number of attributes on different nodes\n", tableName)
			}
		}
	}
	conn := proxy.Src.GetConnection()
	bmi := conn.GetService("BitmapIndex")
	_ = bmi // FIXME: how do I get the tables the proxy uses?
	return count
}
