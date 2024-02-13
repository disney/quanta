#!/bin/bash -e

# from sql_test.go
sleep 5
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
sleep 5
echo "starting TestLocalBasic3then4"
go test -timeout 90s -run ^TestLocalBasic3then4$  github.com/disney/quanta/test

sleep 5
# echo "starting TestLocalBasic4minus1" atw FIXME: fails
# go test -timeout 300s -run ^TestLocalBasic4minus1$  github.com/disney/quanta/test

# from local3+1_zip_test.go
sleep 5
echo "starting TestLocalBasic3then4Zip"
go test -timeout 200s -run ^TestLocalBasic3then4Zip$  github.com/disney/quanta/test

# from restart_test.go
# atw FIXME: fails: go test -timeout 90s -run ^TestRetainData$  github.com/disney/quanta/test


# TODO: kill the consul, stop the docker containers, run tests in test-integration.sh

# TODO: start the consul again 'consul agent -dev' iff it was stopped at the beginning
