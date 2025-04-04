package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadTable(t *testing.T) {
	tcs := NewTableCacheStruct()
	table, err := LoadTable(tcs, "./testdata", nil, "cities", nil)
	assert.Nil(t, err)
	if assert.NotNil(t, table) {
		assert.NotNil(t, table.BasicTable)
		assert.Equal(t, 15, len(table.Attributes))
		regionList, err2 := table.GetAttribute("region_list")
		assert.Nil(t, err2)
		if assert.NotNil(t, regionList) {
			assert.NotNil(t, regionList.MapperConfig)
			assert.Equal(t, regionList.MapperConfig["delim"], ",")
			assert.Equal(t, MapperTypeFromString(regionList.MappingStrategy), StringEnum)
			assert.NotNil(t, regionList.mapperInstance)
		}

		name, err3 := table.GetAttribute("name")
		assert.Nil(t, err3)
		if assert.NotNil(t, name) {
			assert.True(t, name.IsBSI())
		}
	}
}

func TestLoadTableWithPK(t *testing.T) {
	tcs := NewTableCacheStruct()
	table, err := LoadTable(tcs, "./testdata", nil, "cityzip", nil)
	assert.Nil(t, err)
	pki, err2 := table.GetPrimaryKeyInfo()
	assert.Nil(t, err2)
	assert.NotNil(t, pki)
	assert.Equal(t, len(pki), 2)
}

func TestLoadTableWithRelation(t *testing.T) {
	tcs := NewTableCacheStruct()
	table, err := LoadTable(tcs, "./testdata", nil, "cityzip", nil)
	assert.Nil(t, err)
	fka, err2 := table.GetAttribute("city_id")
	assert.Nil(t, err2)
	tab, spec, err3 := fka.GetFKSpec()
	assert.Nil(t, err3)
	assert.NotNil(t, tab)
	assert.NotNil(t, spec)
}
