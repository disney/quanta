#!/bin/bash -e

# from local_test.go
go test -timeout 90s -run ^TestParseSqlAndChangeWhere$  github.com/disney/quanta/test
go test -timeout 90s -run ^TestLocalQuery$              github.com/disney/quanta/test
go test -timeout 90s -run ^TestIsNull$                  github.com/disney/quanta/test
go test -timeout 90s -run ^TestIsNotNull$               github.com/disney/quanta/test
go test -timeout 90s -run ^TestSpellTypeWrong$          github.com/disney/quanta/test
go test -timeout 90s -run ^TestAvgAge$                  github.com/disney/quanta/test

# from local3+1_test.go
go test -timeout 90s -run ^TestLocalBasic3then4$  github.com/disney/quanta/test
go test -timeout 120s -run ^TestLocalBasic4minus1$  github.com/disney/quanta/test

# from local3+1_zip_test.go
go test -timeout 120s -run ^TestLocalBasic3then4Zip$  github.com/disney/quanta/test


# from restart_test.go
# atw FIXME: fails: go test -timeout 90s -run ^TestRetainData$  github.com/disney/quanta/test


# TODO: kill the consul, stop the docker containers, run tests in test-integration.sh

# TODO: start the consul again 'consul agent -dev'
