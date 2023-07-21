package core

import (
	"database/sql/driver"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	schema     *Table
	conn       *Session
	tableCache *TableCacheStruct
)

func setup() {
	tableCache = NewTableCacheStruct()
	schema, _ = LoadTable(tableCache, "./testdata", nil, "cities", nil)
	tbuf := make(map[string]*TableBuffer, 0)
	tbuf[schema.Name] = &TableBuffer{Table: schema}
	conn = &Session{TableBuffers: tbuf}
}

func teardown() {
}

func TestMain(m *testing.M) {
	setup()
	ret := m.Run()
	if ret == 0 {
		teardown()
	}
	os.Exit(ret)
}

func TestMapperFactory(t *testing.T) {

	require.NotNil(t, schema)

	attr, err1 := schema.GetAttribute("military")
	assert.Nil(t, err1)
	mapper, err := ResolveMapper(attr)
	assert.Nil(t, err)
	value, err2 := mapper.MapValue(attr, true, nil)
	assert.Nil(t, err2)
	assert.Equal(t, uint64(1), value)
}

func TestBuiltinMappers(t *testing.T) {

	require.NotNil(t, schema)
	require.NotNil(t, conn)

	data := make(map[string]driver.Value)
	data["id"] = driver.Value("1840034016")  // id
	data["name"] = driver.Value("John")      // Name
	data["county"] = driver.Value("King")    // County
	data["latitude"] = driver.Value(123.5)   // Latitude
	data["longitude"] = driver.Value(-365.5) // Longitude
	data["population"] = driver.Value(10000) // Population
	data["density"] = driver.Value(100)      // Density
	data["military"] = driver.Value(true)    // Military
	data["ranking"] = driver.Value(99)       // Ranking

	values := make(map[string]uint64)

	table, err := LoadTable(tableCache, "./testdata", nil, "cities", nil)
	assert.Nil(t, err)
	if assert.NotNil(t, table) {
		for k, v := range data {
			if k == "longitude" { // FIXME: repair error here.
				continue
			}
			a, err := table.GetAttribute(k)
			if assert.Nil(t, err) {
				value, err := a.MapValue(v, nil)
				if assert.Nil(t, err) {
					values[k] = value
				}
			}

		}
		assert.Equal(t, "1840034016", data["id"])
		assert.Equal(t, uint64(1), values["military"])
	}

}
