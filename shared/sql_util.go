package shared

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
)

// GenerateSQLInsert - Generate an SQL insert statement from a table.
func GenerateSQLInsert(table string, s interface{}) string {

	colBuilder := strings.Builder{}
	valBuilder := strings.Builder{}

	cols := ColumnList(s)
	for i, v := range cols {
		colBuilder.WriteString(v)
		valBuilder.WriteString("?")
		if i < len(cols)-1 {
			colBuilder.WriteString(", ")
			valBuilder.WriteString(", ")
		}
	}
	return fmt.Sprintf("insert into %s (%s) values (%s)", table, colBuilder.String(), valBuilder.String())
}

// BindParams - look for 'sql' tags on struct elements passed in and return the values
func BindParams(s interface{}) []interface{} {

	v := reflect.ValueOf(s)
	elem := v.Elem()
	//fields := getFieldInfo(v.Type())
	fields := getFieldInfo(elem.Type())

	names := make([]string, 0, len(fields))
	for f := range fields {
		names = append(names, f)
	}
	sort.Strings(names)

	var values []interface{}
	for _, name := range names {
		idx, ok := fields[name]
		var v interface{}
		if !ok {
			// There is no field mapped to this column so we discard it
			v = &sql.RawBytes{}
		} else {
			//v = elem.FieldByIndex(idx).Addr().Interface()
			v = elem.FieldByIndex(idx).Interface()
		}
		values = append(values, v)
	}
	return values
}

// NameMapper is the function used to convert struct fields which do not have sql tags
// into database column names.
//
// The default mapper converts field names to lower case. If instead you would prefer
// field names converted to snake case, simply assign sqlstruct.ToSnakeCase to the variable:
//
//	sqlstruct.NameMapper = sqlstruct.ToSnakeCase
//
// Alternatively for a custom mapping, any func(string) string can be used instead.
var NameMapper func(string) string = strings.ToLower

// A cache of fieldInfos to save reflecting every time. Inspried by encoding/xml
var finfos map[reflect.Type]fieldInfo
var finfoLock sync.RWMutex

// TagName is the name of the tag to use on struct fields
var TagName = "sql"

// fieldInfo is a mapping of field tag values to their indices
type fieldInfo map[string][]int

func init() {
	finfos = make(map[reflect.Type]fieldInfo)
}

// Rows defines the interface of types that are scannable with the Scan function.
// It is implemented by the sql.Rows type from the standard library
type Rows interface {
	Scan(...interface{}) error
	Columns() ([]string, error)
}

// getFieldInfo creates a fieldInfo for the provided type. Fields that are not tagged
// with the "sql" tag and unexported fields are not included.
func getFieldInfo(typ reflect.Type) fieldInfo {
	finfoLock.RLock()
	finfo, ok := finfos[typ]
	finfoLock.RUnlock()
	if ok {
		return finfo
	}

	finfo = make(fieldInfo)

	n := typ.NumField()
	for i := 0; i < n; i++ {
		f := typ.Field(i)
		tag := f.Tag.Get(TagName)

		// Skip unexported fields or fields marked with "-"
		if f.PkgPath != "" || tag == "-" || tag == "" {
			continue
		}

		// Handle embedded structs
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			for k, v := range getFieldInfo(f.Type) {
				finfo[k] = append([]int{i}, v...)
			}
			continue
		}
		tag = NameMapper(tag)

		finfo[tag] = []int{i}
	}

	finfoLock.Lock()
	finfos[typ] = finfo
	finfoLock.Unlock()

	return finfo
}

// Scan scans the next row from rows in to a struct pointed to by dest. The struct type
// should have exported fields tagged with the "sql" tag. Columns from row which are not
// mapped to any struct fields are ignored. Struct fields which have no matching column
// in the result set are left unchanged.
func Scan(dest interface{}, rows Rows) error {
	return doScan(dest, rows, "")
}

// ScanAliased works like scan, except that it expects the results in the query to be
// prefixed by the given alias.
//
// For example, if scanning to a field named "name" with an alias of "user" it will
// expect to find the result in a column named "user_name".
//
// See ColumnAliased for a convenient way to generate these queries.
func ScanAliased(dest interface{}, rows Rows, alias string) error {
	return doScan(dest, rows, alias)
}

// Columns returns a string containing a sorted, comma-separated list of column names as
// defined by the type s. s must be a struct that has exported fields tagged with the "sql" tag.
func Columns(s interface{}) string {
	return strings.Join(cols(s), ", ")
}

// ColumnList returns a string containing a sorted, comma-separated list of column names as
func ColumnList(s interface{}) []string {
	return cols(s)
}

// ColumnsAliased works like Columns except it prefixes the resulting column name with the
// given alias.
//
// For each field in the given struct it will generate a statement like:
//
//	alias.field AS alias_field
//
// It is intended to be used in conjunction with the ScanAliased function.
func ColumnsAliased(s interface{}, alias string) string {

	names := cols(s)
	aliased := make([]string, 0, len(names))
	for _, n := range names {
		aliased = append(aliased, alias+"."+n+" AS "+alias+"_"+n)
	}
	return strings.Join(aliased, ", ")
}

func cols(s interface{}) []string {

	v := reflect.ValueOf(s)
	elem := v.Elem()
	fields := getFieldInfo(elem.Type())

	names := make([]string, 0, len(fields))
	for f := range fields {
		names = append(names, f)
	}

	sort.Strings(names)
	return names
}

func doScan(dest interface{}, rows Rows, alias string) error {

	destv := reflect.ValueOf(dest)
	typ := destv.Type()

	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		panic(fmt.Errorf("dest must be pointer to struct; got %T", destv))
	}
	fieldInfo := getFieldInfo(typ.Elem())

	elem := destv.Elem()
	var values []interface{}

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	for _, name := range cols {
		if len(alias) > 0 {
			name = strings.Replace(name, alias+"_", "", 1)
		}
		idx, ok := fieldInfo[strings.ToLower(name)]
		var v interface{}
		if !ok {
			// There is no field mapped to this column so we discard it
			v = &sql.RawBytes{}
		} else {
			v = elem.FieldByIndex(idx).Addr().Interface()
		}
		values = append(values, v)
	}

	return rows.Scan(values...)
}

// ToSnakeCase converts a string to snake case, words separated with underscores.
// It's intended to be used with NameMapper to map struct field names to snake case database fields.
func ToSnakeCase(src string) string {
	thisUpper := false
	prevUpper := false

	buf := bytes.NewBufferString("")
	for i, v := range src {
		if v >= 'A' && v <= 'Z' {
			thisUpper = true
		} else {
			thisUpper = false
		}
		if i > 0 && thisUpper && !prevUpper {
			buf.WriteRune('_')
		}
		prevUpper = thisUpper
		buf.WriteRune(v)
	}
	return strings.ToLower(buf.String())
}

// GetAllRows - Handles database/sql *Rows and returns and array of row maps
func GetAllRows(rows *sql.Rows) ([]map[string]interface{}, error) {

	ret := make([]map[string]interface{}, 0)
	cols, _ := rows.Columns()
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		err := rows.Scan(columnPointers...)
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{})
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			x, ok := (*val).([]byte)
			if !ok {
				continue
			}
			v := string(x)
			if v != "NULL" {
				row[colName] = v
			} else {
				row[colName] = "NULL" // add it anyway
			}
		}
		// fmt.Println("GetAllRows found row", row)
		if len(row) != 0 {
			ret = append(ret, row)
		} else {
			fmt.Println("WARNING had an empty row")
		}
	}
	return ret, nil
}
