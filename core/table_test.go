package core

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestLoadTable(t *testing.T) {

	os.RemoveAll("./testdata/metadata/user360")
	schema, err := LoadSchema("./testdata", "./testdata/metadata", "user360", nil)
	assert.Nil(t, err)
	if assert.NotNil(t, schema) {
		gender, err2 := schema.GetAttribute("gender")
		assert.Nil(t, err2)
		if assert.NotNil(t, gender) {
			assert.Equal(t, len(gender.Values), 3, "There should be 3 values")
			assert.Equal(t, MapperTypeFromString(gender.MappingStrategy), StringEnum)
		}

	}
}

func TestLoadTableWithPK(t *testing.T) {

	os.RemoveAll("./testdata/metadata/guest_id")
	_, err := LoadSchema("./testdata", "./testdata/metadata", "guest_id", nil)
	assert.Nil(t, err)
}
