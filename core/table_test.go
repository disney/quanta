package core

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadTable(t *testing.T) {

	os.RemoveAll("./testdata/metadata/cities")
	schema, err := LoadSchema("./testdata", "./testdata/metadata", "cities", nil)
	assert.Nil(t, err)
	if assert.NotNil(t, schema) {
		state_name, err2 := schema.GetAttribute("state_name")
		assert.Nil(t, err2)
		if assert.NotNil(t, state_name) {
			assert.Equal(t, len(state_name.Values), 1, "There should be 1 value")
			assert.Equal(t, MapperTypeFromString(state_name.MappingStrategy), StringEnum)
		}

	}
}

func TestLoadTableWithPK(t *testing.T) {

	os.RemoveAll("./testdata/metadata/cities")
	_, err := LoadSchema("./testdata", "./testdata/metadata", "cities", nil)
	assert.Nil(t, err)
}
