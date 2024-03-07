#!/bin/bash -e

# from bigDataREtention

sleep 1
echo "starting TestBasicLoadBig"
# this one loads 250k strings into first_name and quits
go test -timeout 500s -run ^TestBasicLoadBig$  github.com/disney/quanta/test
sleep 15
echo "starting TestOpenwStrings"
# this one starts the cluster and if it takes too long (rebuilding indexes) it will fail
go test -timeout 90s -run ^TestOpenwStrings$  github.com/disney/quanta/test

# from restart_test.go
sleep 1
echo "starting TestRetainData"
go test -timeout 90s -run ^TestRetainData$  github.com/disney/quanta/test
sleep 30
echo "starting TestRetainData_Part2"
go test -timeout 90s -run ^TestRetainData_Part2$  github.com/disney/quanta/test
sleep 15
echo "starting TestRetainData_Part2 again"
go test -timeout 90s -run ^TestRetainData_Part2$  github.com/disney/quanta/test

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
sleep 15
echo "starting TestLocalBasic3then4"
go test -timeout 200s -run ^TestLocalBasic3then4$  github.com/disney/quanta/test

# from local3+1_zip_test.go
sleep 15
echo "starting TestLocalBasic3then4Zip"
go test -timeout 200s -run ^TestLocalBasic3then4Zip$  github.com/disney/quanta/test

# sleep 5
# echo "starting TestLocalBasic4minus1" atw FIXME: fails
# go test -timeout 300s -run ^TestLocalBasic4minus1$  github.com/disney/quanta/test

# TODO: kill the consul, stop the docker containers, run tests in test-integration.sh

# TODO: start the consul again 'consul agent -dev' iff it was stopped at the beginning
