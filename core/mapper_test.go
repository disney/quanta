package core

import (
	"database/sql/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

var (
	schema *Table
	conn   *Connection
)

func setup() {
	os.RemoveAll("./testdata/metadata/user360")
	schema, _ = LoadSchema("./testdata", "./testdata/metadata", "user360", nil)
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

	attr, err1 := schema.GetAttribute("plays_fantasy")
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
	data[0] = driver.Value(99)           // visits
	data[1] = driver.Value(true)         // plays_fantasy
	data[2] = driver.Value(false)        // has_notifications
	data[3] = driver.Value("F")          // gender
	data[4] = driver.Value(false)        // user_type
	data[5] = driver.Value(true)         // has_autostart
	data[6] = driver.Value(36)           // age
	data[7] = driver.Value(false)        // has_favorites
	data[8] = driver.Value(false)        // is_insider
	data[9] = driver.Value("US")         // registered_country (country)
	data[10] = driver.Value(false)       // is_league_manager
	data[11] = driver.Value("auto")      // sort_type
	data[12] = driver.Value("123456789") // registered_dma_id (dma_id)

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

	assert.Equal(t, 99, data[0])
	assert.Equal(t, uint64(840), values[9])

}
