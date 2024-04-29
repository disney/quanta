package test_integration

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/akrylysov/pogreb"
	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/test"
	"github.com/stretchr/testify/assert"
)

var e_words []string // an english dictionary

func xxxxxTestBasicLoadBig_part2(t *testing.T) {
	// run TestBasicLoadBig and then run this and see if it fails

	state := &test.ClusterLocalState{}

	go func(state *test.ClusterLocalState) {
		for {
			vectors := []string{"customers_qa/isActive/0/1970-01-01T00", "customers_qa/isActive/1/1970-01-01T00"}
			test.DumpField(t, state, vectors)
			time.Sleep(2 * time.Second)
		}
	}(state)

	test.Ensure_this_cluster(3, state)

	// time.Sleep(99999999 * time.Second)
	// got := ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries.sql")

	// for _, child := range got.FailedChildren {
	// 	fmt.Println("child failed", child.Statement)
	// }

	// assert.EqualValues(t, got.ExpectedRowcount, got.ActualRowCount)
	// assert.EqualValues(t, 0, len(got.FailedChildren))

	state.Release()

}

// time to start cluster 12.483301773s  for 1000 records

// TestOpenwStrings starts the cluster and times it.
// then search the log for "KVStore Init elapsed1" and see how long it took. ~ 15 sec for 256k records NOT
// if the index is intact then it takes 3.3ms
func TestOpenwStrings(t *testing.T) {

	state := &test.ClusterLocalState{}

	// go func(state *ClusterLocalState) {
	// 	for {
	// 		vectors := []string{"customers_qa/first_name/1969-12-31T16"}
	// 		dumpField(t, state, vectors)
	// 		time.Sleep(4 * time.Second)
	// 	}
	// }(state)

	start := time.Now()
	test.Ensure_this_cluster(3, state)
	end := time.Since(start)
	fmt.Println("time to start cluster", end) // 11 sec for 100 with index
	fmt.Println("time to start cluster", end) // 53s for 25k records

	assert.True(t, end < 20*time.Second)

	// time.Sleep(99999999 * time.Second)
	// got := ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries.sql")

	// for _, child := range got.FailedChildren {
	// 	fmt.Println("child failed", child.Statement)
	// }

	// assert.EqualValues(t, got.ExpectedRowcount, got.ActualRowCount)
	// assert.EqualValues(t, 0, len(got.FailedChildren))
	state.Release()
}

// time, total 6.90716759s 500046 (250k records)
// time, total 9.647931ms 0 (250k records) with intact index

func TestReadPogreb(t *testing.T) {
	// TestBasicLoadBig (below) will leave pogreb files in the localClusterData directory.
	// open some of them and read them. Time it.

	dir := "../test/localClusterData/"                                 // quanta-node-0/data/index/"
	items := []string{"customers_qa/first_name/strings/1970-01-01T00"} // , "customers_qa/phoneType.StringEnum"}
	nodes := []string{"quanta-node-0", "quanta-node-1", "quanta-node-2"}

	total := 0

	start := time.Now()
	for _, node := range nodes {
		for i, item := range items {

			path1 := dir + node + "/data/index/" + item
			fmt.Println("opening", path1)
			// delete left over lock files
			// lockfile := path1 + "/lock"
			// os.Remove(lockfile)

			po, err := pogreb.Open(path1, nil)
			check(err)
			// count := po.Count()
			// fmt.Println("count", count, "for", path1)
			// total += int(count)

			if false {
				it := po.Items()
				for {
					key, val, err := it.Next()
					if err == pogreb.ErrIterationDone {
						break
					}
					check(err)

					_ = key
					_ = val
					if i%1000 == 0 {
						log.Printf("%s %s", key, val)
					}
				}
			}
			po.Close()
		}
	}
	end := time.Since(start)
	fmt.Println("time, total", end, total)

	_ = dir
}

// aka  go test -timeout 300s -run ^TestBasicLoadBig$ github.com/disney/quanta/test

func TestBasicLoadBig(t *testing.T) {

	if len(e_words) == 0 {
		str := test.English_words
		e_words = strings.Split(str, "\n")
	}

	// AcquirePort4000.Lock()
	// defer AcquirePort4000.Unlock()
	var err error
	shared.SetUTCdefault()

	isLocalRunning := test.IsLocalRunning()

	// erase the storage
	if !isLocalRunning { // if no cluster is up
		err = os.RemoveAll("../test/localClusterData/") // start fresh
		check(err)
	}
	// ensure we have a cluster on localhost, start one if necessary
	state := test.Ensure_cluster(3)

	currentDir, err := os.Getwd()
	check(err)
	err = os.Chdir("../sqlrunner") // these run from the sqlrunner/ directory
	check(err)
	defer os.Chdir(currentDir)

	if !isLocalRunning {
		test.ExecuteSqlFile(state, "../sqlrunner/sqlscripts/basic_queries_load.sql")
	}

	// make a whole lot of records OBSOLETE
	// 100    now 30
	// 4000   now 54
	// 10000  now 55
	// 100,000   now  96 seconds
	// 250,000   now  146  seconds

	amt := 250 * 1000
	fname := "tmp.tmp.sql"
	// open a file to write
	f, err := os.Create(fname)
	check(err)
	conn, err := state.ProxyControl.Src.GetSessionPool().Borrow("customers_qa")
	check(err)
	defer state.ProxyControl.Src.GetSessionPool().Return("customers_qa", conn)

	for i := 0; i < amt; i++ {

		id := strconv.FormatInt(int64(1000+i), 10)
		active := strconv.FormatInt(int64(i&1), 10)
		zip := strconv.FormatInt(int64(99821+i), 10)
		city1 := e_words[((i+100)*11)%len(e_words)]
		city2 := e_words[((i+100)*13)%len(e_words)]
		city3 := e_words[((i+100)*17)%len(e_words)]

		city := city1 + "-" + city2 + "-" + city3

		first_name := city
		city = "dummy"

		stmt := `insert into customers_qa (cust_id, first_name, last_name, isLegalAge, isActive, height, zip,city)`
		stmt += ` values('` + id + `', ` + first_name + `,'Araki',1,` + active + `,68.25,'` + zip + `', '` + city + `'); `
		// AnalyzeRow(*state.ProxyConnect, []string{stmt}, true)
		f.WriteString(stmt + "\n")

		data := make(map[string]interface{}, 0)

		data["cust_id"] = id
		data["first_name"] = first_name // "Theresa"
		data["last_name"] = "Araki"
		data["isLegalAge"] = 1
		data["isActive"] = active
		data["height"] = 68.25
		data["zip"] = zip
		data["city"] = city

		// fmt.Println("putting", data, "into customers_qa")
		err = conn.PutRow("customers_qa", data, 0, false, false)
		check(err)
	}
	f.Close()
	// got := ExecuteSqlFile(state, fname)
	// _ = got
	err = os.Remove(fname)
	check(err)

	vectors := []string{"customers_qa/isActive/0/1970-01-01T00", "customers_qa/isActive/1/1970-01-01T00"}

	test.TestStatesAllMatch(t, state, "initial")
	test.DumpField(t, state, vectors)

	// time.Sleep(99999999 * time.Second) // stay alive forever

	// release as necessary
	state.Release()

}
