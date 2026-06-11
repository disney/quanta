package roadmap

import (
	"errors"
	"testing"
)

func TestParseNormalizesDefaults(t *testing.T) {
	suite, err := Parse([]byte(`
version: 1
name: smoke
tests:
  - id: query.one
    sql: select 1
    expect:
      rows:
        - [1]
`))
	if err != nil {
		t.Fatal(err)
	}

	test := suite.Tests[0]
	if test.Status != CaseSupported {
		t.Fatalf("expected supported status, got %s", test.Status)
	}
	if test.Kind != "query" {
		t.Fatalf("expected query kind, got %s", test.Kind)
	}
	if test.Order != "exact" {
		t.Fatalf("expected exact order, got %s", test.Order)
	}
}

func TestEvaluateQueryRowsortAndNull(t *testing.T) {
	test := TestCase{
		ID:     "query.rowsort",
		Status: CaseSupported,
		Order:  "rowsort",
		Expect: Expected{
			Columns: []string{"id", "name"},
			Rows: [][]interface{}{
				{2, nil},
				{1, "Abe"},
			},
		},
	}
	actual := QueryResult{
		Columns: []string{"id", "name"},
		Rows: [][]Cell{
			{{Text: "1"}, {Text: "Abe"}},
			{{Text: "2"}, {Null: true}},
		},
	}

	if details := evaluateQuery(test, actual, nil); details != "" {
		t.Fatal(details)
	}
}

func TestXFailAndXPassClassification(t *testing.T) {
	test := TestCase{ID: "roadmap.group-by", Status: CaseXFail}

	if result := classify(test, "not implemented"); result.Status != ResultXFail {
		t.Fatalf("expected XFAIL, got %s", result.Status)
	}
	if result := classify(test, ""); result.Status != ResultXPass {
		t.Fatalf("expected XPASS, got %s", result.Status)
	}
}

func TestCompareRowsReportsFirstDifference(t *testing.T) {
	expected := [][]Cell{{{Text: "1"}, {Text: "Abe"}}}
	actual := [][]Cell{{{Text: "1"}, {Text: "Abby"}}}

	details := compareRows(expected, actual)
	if details != `row 1 column 2 differs: expected "Abe", actual "Abby"` {
		t.Fatalf("unexpected difference: %s", details)
	}
}

func TestExpectedErrorPassesOnMatchingSubstring(t *testing.T) {
	test := TestCase{
		ID:     "errors.unknown-column",
		Status: CaseSupported,
		Expect: Expected{Error: "unknown column"},
	}

	if details := evaluateQuery(test, QueryResult{}, errors.New("Error 1105: Unknown Column foo")); details != "" {
		t.Fatal(details)
	}
}

func TestEvaluateQueryRowCountOnly(t *testing.T) {
	expectedCount := 2
	test := TestCase{
		ID:     "query.row-count",
		Status: CaseSupported,
		Expect: Expected{RowCount: &expectedCount},
	}
	actual := QueryResult{Rows: [][]Cell{{{Text: "one"}}, {{Text: "two"}}}}

	if details := evaluateQuery(test, actual, nil); details != "" {
		t.Fatal(details)
	}
}
