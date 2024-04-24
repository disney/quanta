#!/bin/bash -e

# You can also add the -race flag to different go commands like go test, to help detect where race conditions happen: golang.org/doc/articles/race_detector.html 

sleep 1
echo "starting TestBasic"
go test -timeout 300s -run ^TestBasic$  github.com/disney/quanta/test-integration


# TestTableMod_change fails with the following repeating endlessly until timeout:
# 2024/04/21 02:41:59.440477 bitmap.go:490: [DEBUG] batchProcessLoop shard count quanta-node-0 shard 10 bsi 8 bitmap 2
# 2024/04/21 02:41:59.440539 bitmap.go:490: [DEBUG] batchProcessLoop shard count quanta-node-2 shard 10 bsi 6 bitmap 4
# 2024/04/21 02:41:59.440542 bitmap.go:490: [DEBUG] batchProcessLoop shard count quanta-node-1 shard 8 bsi 4 bitmap 4
# 2024/04/21 02:42:08.954685 kvstore.go:164: [DEBUG] quanta-node-0 KVStore cleanup
# 2024/04/21 02:42:08.959808 kvstore.go:164: [DEBUG] quanta-node-1 KVStore cleanup

sleep 1
echo "starting TestTableMod_change"
# FIXME: go test -timeout 300s -run ^TestTableMod_change$  github.com/disney/quanta/test

# from local_test.go
sleep 1
echo "starting TestReplication"
go test -timeout 200s -run ^TestReplication$  github.com/disney/quanta/test

# from table_mod_test.go
sleep 5
echo "starting TestTableMod_remove_column"
go test -timeout 200s -run ^TestTableMod_remove_column$  github.com/disney/quanta/test

sleep 5
echo "starting TestTableMod_add"
go test -timeout 200s -run ^TestTableMod_add$  github.com/disney/quanta/test

sleep 5
echo "starting TestTableMod_reload_table"
go test -timeout 200s -run ^TestTableMod_reload_table$  github.com/disney/quanta/test


# from bigDataRetention.go
sleep 1
echo "starting TestBasicLoadBig"
# this one loads 250k strings into first_name and quits
go test -timeout 500s -run ^TestBasicLoadBig$  github.com/disney/quanta/test
sleep 15
echo "starting TestOpenwStrings"
# this one starts the cluster and if it takes too long (rebuilding indexes) it will fail
go test -timeout 90s -run ^TestOpenwStrings$  github.com/disney/quanta/test

sleep 15
echo "starting TestSQLRunnerSuite"
# fixme: go test -timeout 500s -run ^TestSQLRunnerSuite$  github.com/disney/quanta/test_integration

# from restart_test.go
# these work locally but seem to have a problem retaining data at github actions.
# FIXME: 
sleep 1
echo "starting TestRetainData"
# go test -timeout 90s -run ^TestRetainData$  github.com/disney/quanta/test
sleep 1
echo "starting TestRetainData_Part2"
# go test -timeout 90s -run ^TestRetainData_Part2$  github.com/disney/quanta/test
sleep 1
# echo "starting TestRetainData_Part2 again"
# go test -timeout 90s -run ^TestRetainData_Part2$  github.com/disney/quanta/test

# from sql_test.go
sleep 15
echo "starting TestShowTables"
go test -timeout 200s -run ^TestShowTables$  github.com/disney/quanta/test

# from local_test.go
echo "TestParseSqlAndChangeWhere"
go test -timeout 90s -run ^TestParseSqlAndChangeWhere$  github.com/disney/quanta/test
echo "TestLocalQuery"
go test -timeout 90s -run ^TestLocalQuery$              github.com/disney/quanta/test
echo "TestIsNull"
go test -timeout 90s -run ^TestIsNull$                  github.com/disney/quanta/test
echo "TestIsNotNull"
go test -timeout 90s -run ^TestIsNotNull$               github.com/disney/quanta/test
echo "TestSpellTypeWrong"
go test -timeout 90s -run ^TestSpellTypeWrong$          github.com/disney/quanta/test
echo "TestAvgAge"
go test -timeout 90s -run ^TestAvgAge$                  github.com/disney/quanta/test

# from local3+1_test.go
sleep 30
echo "starting TestLocalBasic3then4"
# fixme: go test -timeout 200s -run ^TestLocalBasic3then4$  github.com/disney/quanta/test

# from local3+1_zip_test.go
sleep 30
echo "starting TestLocalBasic3then4Zip"
go test -timeout 200s -run ^TestLocalBasic3then4Zip$  github.com/disney/quanta/test

# sleep 5
# echo "starting TestLocalBasic4minus1" atw FIXME: fails
# go test -timeout 300s -run ^TestLocalBasic4minus1$  github.com/disney/quanta/test

# TODO: kill the consul, run docker tests see: ./test/run-docker-tests.sh

