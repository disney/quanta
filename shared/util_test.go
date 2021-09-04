package shared

import (
	"testing"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/sdk/testutil"
	"github.com/stretchr/testify/assert"
)

func testConsul(t *testing.T) {

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

	init, err := LoadSchema("./testdata", "cities", consulClient)
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
	}
}
