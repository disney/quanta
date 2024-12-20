package shared

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	jsonData = `{
		"testname":"mpath_test",
		 "data": {
    		"partner": "espn",
    		"sku": "a3-f3fewa",
    		"more": "fields from completed-v2 event data..."
  		},
		"metadata": [
    		{ 
				"metadata-origin": "dfi", 
				"receivedTimestamp": "12-14-1993", 
				"processedTimestamp": "12-14-1993", 
				"source": "some-source-id-possbily-by-arn"
    		},
    		{
      			"metadata-origin": "sdp",
      			"type": "urn:dss:event:edge:fed:purchase:completed-v2",
      			"source": "some-source-id",
      			"other metadata": "from sdp event will populate here..."
    		}
  		]
	}`
)

func TestPath(t *testing.T) {

	dat := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonData), &dat)
	assert.NoError(t, err)

	var val interface{}
	_, err = GetPath("metadata/1/receivedTimestamp", dat, false, false)
	assert.Error(t, err, "Key not present. [Key:receivedTimestamp]")
	val, err = GetPath("metadata/0/receivedTimestamp", dat, false, false)
	assert.NoError(t, err)
	assert.Equal(t, val, "12-14-1993")
	val, err = GetPath("data/partner", dat, false, false)
	assert.NoError(t, err)
	assert.Equal(t, val, "espn")
	val, err = GetPath("badpath/testname", dat, true, false)
	assert.Error(t, err)
	// val is nil assert.Equal(t, val, "mpath_test")
	val, err = GetPath("/badpath/testname", dat, true, false)
	assert.NoError(t, err)
	assert.Equal(t, val, "mpath_test")
	_, err = GetPath("/badpath/testname2", dat, true, false)
	assert.Error(t, err, "Key not present. [Key:testname2]")
}

func TestCreate(t *testing.T) {
	nm := CreateNestedMapFromPath("data/value", "XXXX")
	assert.NotNil(t, nm)
}
