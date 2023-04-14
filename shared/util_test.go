package shared

import (
	"testing"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/sdk/testutil"
	"github.com/stretchr/testify/assert"
)

// Since we have cinsukl running all the time this makes no sense.
func xxTestConsul(t *testing.T) {

	// Create a test Consul server
	srv, err := testutil.NewTestServerConfigT(t, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	conf := api.DefaultConfig()
	conf.Address = srv.HTTPAddr
	consulClient, errx := api.NewClient(conf)
	assert.Nil(t, errx)

	ok, _ := TableExists(consulClient, "cities")
	assert.False(t, ok)

	init, err := LoadSchema("./testdata/config2", "cities", consulClient)
	errx1 := MarshalConsul(init, consulClient)
	assert.Nil(t, errx1)

	// LoadSchema with "" config parameter will unmarshal from Consul
	schema, err := LoadSchema("", "cities", consulClient)
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

		ok, _ := TableExists(consulClient, "cities")
		assert.True(t, ok)
	}
}

// FIXME: This test is broken. the test server won't start because we always have a consul server running.
func xxxTestConstraints(t *testing.T) {

	// Create a test Consul server
	//srv, err := testutil.NewTestServerConfigT(t, nil)
	// srv, err := api.NewClient(api.DefaultConfig())
	// if err != nil {
	// 	t.Fatal(err)
	// }
	//defer srv.Stop()

	conf := api.DefaultConfig()
	conf.Address = "8500" // srv.HTTPAddr
	consulClient, err1 := api.NewClient(conf)
	assert.Nil(t, err1)

	cityzip, err2 := LoadSchema("./testdata/config", "cityzip", consulClient)
	assert.Nil(t, err2)

	cities, err3 := LoadSchema("./testdata/config", "cities", consulClient)
	assert.Nil(t, err3)

	// Simulate create table where parent of FK does not exist
	ok, err := CheckParentRelation(consulClient, cityzip)
	assert.Nil(t, err)
	assert.False(t, ok)

	// Ok, create parent and recheck
	err = MarshalConsul(cities, consulClient)
	ok, _ = TableExists(consulClient, "cities")
	assert.True(t, ok)
	ok, err = CheckParentRelation(consulClient, cityzip)
	assert.Nil(t, err)
	assert.True(t, ok)

	// create child
	err = MarshalConsul(cityzip, consulClient)
	assert.Nil(t, err)
	ok, _ = TableExists(consulClient, "cityzip")
	assert.True(t, ok)

	// Simulate drop parent table where child relation exists.
	dependencies, errx := CheckChildRelation(consulClient, cities.Name)
	assert.Nil(t, errx)
	assert.Equal(t, 1, len(dependencies))
	assert.Equal(t, "cityzip", dependencies[0]) // no go, dependecies

	// Drop parent and re-check
	err = DeleteTable(consulClient, "cityzip")
	assert.Nil(t, err)
	ok, _ = TableExists(consulClient, "cityzip")
	assert.False(t, ok)
	dependencies, errx = CheckChildRelation(consulClient, cities.Name)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(dependencies))
}
