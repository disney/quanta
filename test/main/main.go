package main

import (
	"fmt"
)

func main() {

	// shared.SetUTCdefault()

	// // erase the storage
	// if !test.IsLocalRunning() { // if no cluster is up
	// 	err := os.RemoveAll("./test/localClusterData/")
	// 	_ = err
	// }
	// // ensure_custer
	// state := test.Ensure_cluster()

	// var err error
	// state.db, err = state.proxyConnect.ProxyConnectConnect()
	// _ = err

	// conn := shared.NewDefaultConnection()
	// err = conn.Connect(nil)
	// check(err)
	// defer conn.Disconnect()

	// sharedKV := shared.NewKVStore(conn)

	// ctx, err := rbac.NewAuthContext(sharedKV, "MOLIG004", true)
	// check(err)
	// err = ctx.GrantRole(rbac.SystemAdmin, "MOLIG004", "quanta", true)
	// check(err)

	// time.Sleep(5 * time.Second)

	// // load something

	// AnalyzeRow(*state.proxyConnect, []string{"quanta-admin drop orders_qa"}, true)
	// AnalyzeRow(*state.proxyConnect, []string{"quanta-admin drop customers_qa"}, true)
	// AnalyzeRow(*state.proxyConnect, []string{"quanta-admin create customers_qa"}, true)

	// AnalyzeRow(*state.proxyConnect, []string{"insert into customers_qa (cust_id, first_name, address, city, state, zip, phone, phoneType) values('101','Abe','123 Main','Seattle','WA','98072','425-232-4323','cell;home');"}, true)

	// // query

	// AnalyzeRow(*state.proxyConnect, []string{"select * from customers_qa;@30"}, true)

	// //	assert.EqualValues(t, FailCount, 0) // FailCount in sql-types.go

	// // release as necessary
	// state.Release()

}

func check(err error) {
	if err != nil {
		fmt.Println("check err", err)
		panic(err.Error())
	}
}
