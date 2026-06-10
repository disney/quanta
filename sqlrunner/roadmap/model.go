package roadmap

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v2"
)

const (
	CaseSupported = "supported"
	CaseXFail     = "xfail"
	CaseSkip      = "skip"

	ResultPass  = "PASS"
	ResultFail  = "FAIL"
	ResultXFail = "XFAIL"
	ResultXPass = "XPASS"
	ResultSkip  = "SKIP"
)

type Suite struct {
	Version int        `yaml:"version"`
	Name    string     `yaml:"name"`
	Tests   []TestCase `yaml:"tests"`
}

type TestCase struct {
	ID           string   `yaml:"id"`
	Status       string   `yaml:"status"`
	Kind         string   `yaml:"kind"`
	Order        string   `yaml:"order"`
	Capabilities []string `yaml:"capabilities"`
	Issue        string   `yaml:"issue"`
	SQL          string   `yaml:"sql"`
	Expect       Expected `yaml:"expect"`
}

type Expected struct {
	Columns      []string        `yaml:"columns"`
	Types        []string        `yaml:"types"`
	Rows         [][]interface{} `yaml:"rows"`
	RowCount     *int            `yaml:"row_count"`
	AffectedRows *int64          `yaml:"affected_rows"`
	Error        string          `yaml:"error_contains"`
}

type Cell struct {
	Null bool
	Text string
}

type QueryResult struct {
	Columns []string
	Types   []string
	Rows    [][]Cell
}

type CaseResult struct {
	ID      string
	Status  string
	Details string
}

type Summary struct {
	Suite   string
	Results []CaseResult
}

func Parse(data []byte) (*Suite, error) {
	var suite Suite
	if err := yaml.UnmarshalStrict(data, &suite); err != nil {
		return nil, err
	}
	if suite.Version != 1 {
		return nil, fmt.Errorf("unsupported suite version %d", suite.Version)
	}
	if strings.TrimSpace(suite.Name) == "" {
		return nil, fmt.Errorf("suite name is required")
	}
	for i := range suite.Tests {
		if err := normalizeTestCase(&suite.Tests[i]); err != nil {
			return nil, fmt.Errorf("test %d: %w", i+1, err)
		}
	}
	return &suite, nil
}

func normalizeTestCase(test *TestCase) error {
	test.ID = strings.TrimSpace(test.ID)
	test.Status = strings.ToLower(strings.TrimSpace(test.Status))
	test.Kind = strings.ToLower(strings.TrimSpace(test.Kind))
	test.Order = strings.ToLower(strings.TrimSpace(test.Order))
	test.SQL = strings.TrimSpace(test.SQL)

	if test.ID == "" {
		return fmt.Errorf("id is required")
	}
	if test.Status == "" {
		test.Status = CaseSupported
	}
	switch test.Status {
	case CaseSupported, CaseXFail, CaseSkip:
	default:
		return fmt.Errorf("%s has invalid status %q", test.ID, test.Status)
	}
	if test.Kind == "" {
		test.Kind = inferKind(test.SQL)
	}
	switch test.Kind {
	case "query", "statement", "admin":
	default:
		return fmt.Errorf("%s has invalid kind %q", test.ID, test.Kind)
	}
	if test.Order == "" {
		test.Order = "exact"
	}
	switch test.Order {
	case "exact", "rowsort":
	default:
		return fmt.Errorf("%s has invalid order %q", test.ID, test.Order)
	}
	if test.Status != CaseSkip && test.SQL == "" {
		return fmt.Errorf("%s requires sql", test.ID)
	}
	if test.Kind == "query" && test.Expect.AffectedRows != nil {
		return fmt.Errorf("%s query cannot expect affected_rows", test.ID)
	}
	if test.Kind != "query" && (len(test.Expect.Rows) > 0 || test.Expect.RowCount != nil) {
		return fmt.Errorf("%s statement cannot expect rows", test.ID)
	}
	return nil
}

func inferKind(sql string) string {
	fields := strings.Fields(strings.ToLower(sql))
	if len(fields) > 0 && (fields[0] == "select" || fields[0] == "show") {
		return "query"
	}
	return "statement"
}

func (s Summary) HasFailures() bool {
	for _, result := range s.Results {
		if result.Status == ResultFail || result.Status == ResultXPass {
			return true
		}
	}
	return false
}
