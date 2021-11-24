package shared

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadTable(t *testing.T) {

	schema, err := LoadSchema("./testdata/config2", "cities", nil)
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

	schema, err := LoadSchema("./testdata/config", "cityzip", nil)
	assert.Nil(t, err)
	pki, err2 := schema.GetPrimaryKeyInfo()
	assert.Nil(t, err2)
	assert.NotNil(t, pki)
	assert.Equal(t, len(pki), 2)
}

func TestSchemaCompare(t *testing.T) {

	current, err := LoadSchema("./testdata/config", "cities", nil)
	assert.Nil(t, err)
	new, err := LoadSchema("./testdata/config2", "cities", nil)
	assert.Nil(t, err)

	ok, warnings, err := current.Compare(new)
	assert.Nil(t, err)
	assert.False(t, ok)
	if assert.Equal(t, 1, len(warnings)) {
		assert.Equal(t, warnings[0], "new attribute 'gender', addition is allowable")
	}

	new.DisableDedup = true
	ok, warnings, err = current.Compare(new)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(warnings))

	currState, errx := current.GetAttribute("state_name")
	assert.Nil(t, errx)
	assert.NotNil(t, currState)
	newState, errx := new.GetAttribute("state_name")
	assert.Nil(t, errx)
	assert.NotNil(t, newState)
	ok, warnings, err = currState.Compare(newState)
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, 0, len(warnings))

	newState.Desc = "State name."
	ok, warnings, err = currState.Compare(newState)
	assert.Nil(t, err)
	assert.False(t, ok)
	if assert.Equal(t, 1, len(warnings)) {
		assert.Equal(t, warnings[0],
			"attribute 'state_name' description changed existing = '', new = 'State name.'")
	}
}
