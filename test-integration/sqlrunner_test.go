package test_integration

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/sqlrunner/roadmap"
	"github.com/disney/quanta/test"
)

func TestBasic(t *testing.T) {
	runRoadmapSuite(t, "sqltests/basic_queries.yaml")
}

func check(err error) {
	if err != nil {
		fmt.Println("test-integration check err", err)
	}
}

func TestInsert(t *testing.T) {
	runRoadmapSuite(t, "sqltests/insert_tests.yaml")
}

func TestMutate(t *testing.T) {
	runRoadmapSuite(t, "sqltests/mutate_tests_body.yaml")
}

func TestJoins(t *testing.T) {
	runRoadmapSuite(t, "sqltests/joins_sql.yaml")
}

func runRoadmapSuite(t *testing.T, suitePath string) {
	t.Helper()
	shared.SetUTCdefault()

	state := test.Ensure_cluster(3)
	defer state.Release()

	currentDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir("../sqlrunner"); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(currentDir)

	data, err := os.ReadFile(suitePath)
	if err != nil {
		t.Fatal(err)
	}
	suite, err := roadmap.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.ProxyConnect.ProxyConnectConnect()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	summary := (roadmap.Runner{
		DB:    db,
		Admin: func(_ context.Context, command string) error { return test.ExecuteAdminCommandAndWait(command) },
	}).Run(context.Background(), suite)

	for _, result := range summary.Results {
		switch result.Status {
		case roadmap.ResultFail, roadmap.ResultXPass:
			t.Errorf("%s %s: %s", result.Status, result.ID, result.Details)
		case roadmap.ResultXFail:
			t.Logf("%s %s: %s", result.Status, result.ID, result.Details)
		}
	}
}
