package core

import (
	"database/sql/driver"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	schema *Table
	conn   *Connection
)

func setup() {
	os.RemoveAll("./testdata/metadata/cities")
	schema, _ = LoadSchema("./testdata", "./testdata/metadata", "cities", nil)
	tbuf := make(map[string]*TableBuffer, 0)
	tbuf[schema.Name] = &TableBuffer{Table: schema}
	conn = &Connection{TableBuffers: tbuf}
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

	data := make([]driver.Value, 15)
	data[0] = driver.Value("1840034016") // id
	data[1] = driver.Value("John")       // Name
	data[2] = driver.Value("Washington") // State_Name
	data[3] = driver.Value("WA")         // State_ID
	data[4] = driver.Value("King")       // County
	data[5] = driver.Value(123.5)        // Lattitude
	data[6] = driver.Value(365.5)        // Longitude
	data[7] = driver.Value(10000)        // Population
	data[8] = driver.Value(100)          // Density
	data[9] = driver.Value(true)         // Military
	data[10] = driver.Value("PST")       // Timezone
	data[11] = driver.Value(99)          // Ranking
	// data[12] = driver.Value("123456789") // registered_dma_id (dma_id)

	values := make([]uint64, 15)

	for _, v := range schema.Attributes {
		if v.Type == "NotExist" || v.Type == "NotDefined" || v.Type == "JSON" {
			continue
		}
		value, err := v.MapValue(data[v.Ordinal-1], nil)
		assert.Nil(t, err)
		values[v.Ordinal-1] = value
		assert.NotEqual(t, 0, data[v.Ordinal-1])
	}

	assert.Equal(t, "1840034016", data[0])
	assert.Equal(t, uint64(1), values[9])

}
