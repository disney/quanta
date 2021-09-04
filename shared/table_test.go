package shared

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadTable(t *testing.T) {

	schema, err := LoadSchema("./testdata", "cities", nil)
	assert.Nil(t, err)
	if assert.NotNil(t, schema) {
		gender, err2 := schema.GetAttribute("gender")
		assert.Nil(t, err2)
		if assert.NotNil(t, gender) {
			assert.Equal(t, gender.MappingStrategy, "StringEnum")
		}
		assert.Equal(t, len(gender.Values), 2)

		regionList, err2 := schema.GetAttribute("region_list")
		assert.Nil(t, err2)
		if assert.NotNil(t, regionList) {
			assert.NotNil(t, regionList.MapperConfig)
			assert.Equal(t, regionList.MapperConfig["delim"], ",")
		}

		name, err3 := schema.GetAttribute("name")
		assert.Nil(t, err3)
		if assert.NotNil(t, name) {
			assert.True(t, name.IsBSI())
		}
	}
}

func TestLoadTableWithPK(t *testing.T) {

	schema, err := LoadSchema("./testdata", "cityzip", nil)
	assert.Nil(t, err)
	pki, err2 := schema.GetPrimaryKeyInfo()
	assert.Nil(t, err2)
	assert.NotNil(t, pki)
	assert.Equal(t, len(pki), 2)
}
