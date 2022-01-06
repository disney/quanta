package shared

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	jsonData = `{
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
	val, err = GetPath("metadata/1/receivedTimestamp", dat)
	assert.Error(t, err, "Key not present. [Key:receivedTimestamp]")
	val, err = GetPath("metadata/0/receivedTimestamp", dat)
	assert.NoError(t, err)
	assert.Equal(t, val, "12-14-1993")
	val, err = GetPath("data/partner", dat)
	assert.NoError(t, err)
	assert.Equal(t, val, "espn")
}
