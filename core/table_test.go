package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadTable(t *testing.T) {

	schema, err := LoadSchema("./testdata", nil, "cities", nil)
	assert.Nil(t, err)
	if assert.NotNil(t, schema) {
		stateName, err2 := schema.GetAttribute("state_name")
		assert.Nil(t, err2)
		if assert.NotNil(t, stateName) {
			assert.Equal(t, MapperTypeFromString(stateName.MappingStrategy), StringEnum)
		}

	}
}

func TestLoadTableWithPK(t *testing.T) {

	_, err := LoadSchema("./testdata", nil, "cities", nil)
	assert.Nil(t, err)
}
