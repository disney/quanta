package roadmap

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Runner struct {
	DB    *sql.DB
	Admin func(context.Context, string) error
}

func (r Runner) Run(ctx context.Context, suite *Suite) Summary {
	summary := Summary{Suite: suite.Name, Results: make([]CaseResult, 0, len(suite.Tests))}
	for _, test := range suite.Tests {
		if test.Status == CaseSkip {
			summary.Results = append(summary.Results, CaseResult{ID: test.ID, Status: ResultSkip})
			continue
		}

		var details string
		if test.Kind == "query" {
			actual, err := r.query(ctx, test.SQL)
			details = evaluateQuery(test, actual, err)
		} else if test.Kind == "admin" {
			if r.Admin == nil {
				details = "admin executor is not configured"
			} else {
				details = evaluateAdmin(test, r.Admin(ctx, test.SQL))
			}
		} else {
			affected, err := r.exec(ctx, test.SQL)
			details = evaluateStatement(test, affected, err)
		}
		summary.Results = append(summary.Results, classify(test, details))
	}
	return summary
}

func (r Runner) query(ctx context.Context, statement string) (QueryResult, error) {
	rows, err := r.DB.QueryContext(ctx, statement)
	if err != nil {
		return QueryResult{}, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return QueryResult{}, err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return QueryResult{}, err
	}
	types := make([]string, len(columnTypes))
	for i, columnType := range columnTypes {
		types[i] = strings.ToUpper(columnType.DatabaseTypeName())
	}

	result := QueryResult{Columns: columns, Types: types}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		dest := make([]interface{}, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return QueryResult{}, err
		}
		row := make([]Cell, len(values))
		for i, value := range values {
			row[i] = actualCell(value)
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return QueryResult{}, err
	}
	return result, nil
}

func (r Runner) exec(ctx context.Context, statement string) (int64, error) {
	result, err := r.DB.ExecContext(ctx, statement)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func evaluateQuery(test TestCase, actual QueryResult, queryErr error) string {
	if test.Expect.Error != "" {
		return evaluateExpectedError(test.Expect.Error, queryErr)
	}
	if queryErr != nil {
		return "unexpected error: " + queryErr.Error()
	}
	if len(test.Expect.Columns) > 0 && !equalStrings(test.Expect.Columns, actual.Columns) {
		return fmt.Sprintf("columns differ: expected %v, actual %v", test.Expect.Columns, actual.Columns)
	}
	if len(test.Expect.Types) > 0 {
		expectedTypes := upperStrings(test.Expect.Types)
		if !equalStrings(expectedTypes, actual.Types) {
			return fmt.Sprintf("types differ: expected %v, actual %v", expectedTypes, actual.Types)
		}
	}
	if test.Expect.RowCount != nil && len(actual.Rows) != *test.Expect.RowCount {
		return fmt.Sprintf("row count differs: expected %d, actual %d", *test.Expect.RowCount, len(actual.Rows))
	}

	if test.Expect.Rows == nil {
		return ""
	}
	expectedRows := expectedCells(test.Expect.Rows)
	actualRows := cloneRows(actual.Rows)
	if test.Order == "rowsort" {
		sortRows(expectedRows)
		sortRows(actualRows)
	}
	if details := compareRows(expectedRows, actualRows); details != "" {
		return details
	}
	return ""
}

func evaluateAdmin(test TestCase, adminErr error) string {
	if test.Expect.Error != "" {
		return evaluateExpectedError(test.Expect.Error, adminErr)
	}
	if adminErr != nil {
		return "unexpected error: " + adminErr.Error()
	}
	return ""
}

func evaluateStatement(test TestCase, affected int64, execErr error) string {
	if test.Expect.Error != "" {
		return evaluateExpectedError(test.Expect.Error, execErr)
	}
	if execErr != nil {
		return "unexpected error: " + execErr.Error()
	}
	if test.Expect.AffectedRows != nil && affected != *test.Expect.AffectedRows {
		return fmt.Sprintf("affected rows differ: expected %d, actual %d", *test.Expect.AffectedRows, affected)
	}
	return ""
}

func evaluateExpectedError(expected string, actual error) string {
	if actual == nil {
		return "expected an error containing " + strconv.Quote(expected)
	}
	if !strings.Contains(strings.ToLower(actual.Error()), strings.ToLower(expected)) {
		return fmt.Sprintf("error differs: expected substring %q, actual %q", expected, actual.Error())
	}
	return ""
}

func classify(test TestCase, details string) CaseResult {
	passed := details == ""
	if test.Status == CaseXFail {
		if passed {
			return CaseResult{ID: test.ID, Status: ResultXPass, Details: "roadmap goal now passes; promote it to supported"}
		}
		return CaseResult{ID: test.ID, Status: ResultXFail, Details: details}
	}
	if passed {
		return CaseResult{ID: test.ID, Status: ResultPass}
	}
	return CaseResult{ID: test.ID, Status: ResultFail, Details: details}
}

func actualCell(value interface{}) Cell {
	if value == nil {
		return Cell{Null: true}
	}
	switch typed := value.(type) {
	case []byte:
		return Cell{Text: string(typed)}
	case time.Time:
		return Cell{Text: typed.UTC().Format(time.RFC3339Nano)}
	default:
		return Cell{Text: fmt.Sprint(typed)}
	}
}

func expectedCells(rows [][]interface{}) [][]Cell {
	result := make([][]Cell, len(rows))
	for i, row := range rows {
		result[i] = make([]Cell, len(row))
		for j, value := range row {
			if value == nil {
				result[i][j] = Cell{Null: true}
			} else {
				result[i][j] = Cell{Text: fmt.Sprint(value)}
			}
		}
	}
	return result
}

func compareRows(expected, actual [][]Cell) string {
	if len(expected) != len(actual) {
		return fmt.Sprintf("row count differs: expected %d, actual %d", len(expected), len(actual))
	}
	for row := range expected {
		if len(expected[row]) != len(actual[row]) {
			return fmt.Sprintf("row %d column count differs: expected %d, actual %d", row+1, len(expected[row]), len(actual[row]))
		}
		for column := range expected[row] {
			if expected[row][column] != actual[row][column] {
				return fmt.Sprintf("row %d column %d differs: expected %s, actual %s",
					row+1, column+1, formatCell(expected[row][column]), formatCell(actual[row][column]))
			}
		}
	}
	return ""
}

func formatCell(cell Cell) string {
	if cell.Null {
		return "NULL"
	}
	return strconv.Quote(cell.Text)
}

func sortRows(rows [][]Cell) {
	sort.Slice(rows, func(i, j int) bool {
		return rowKey(rows[i]) < rowKey(rows[j])
	})
}

func rowKey(row []Cell) string {
	var builder strings.Builder
	for _, cell := range row {
		if cell.Null {
			builder.WriteString("-1:")
		} else {
			builder.WriteString(strconv.Itoa(len(cell.Text)))
			builder.WriteByte(':')
			builder.WriteString(cell.Text)
		}
		builder.WriteByte('|')
	}
	return builder.String()
}

func cloneRows(rows [][]Cell) [][]Cell {
	result := make([][]Cell, len(rows))
	for i := range rows {
		result[i] = append([]Cell(nil), rows[i]...)
	}
	return result
}

func upperStrings(values []string) []string {
	result := make([]string, len(values))
	for i, value := range values {
		result[i] = strings.ToUpper(value)
	}
	return result
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
