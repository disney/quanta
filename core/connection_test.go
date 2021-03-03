package core

import (
	"github.com/stretchr/testify/assert"
	_ "github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestCreateConnection(t *testing.T) {
	os.RemoveAll("./testdata/metadata/events")
	c, err := OpenConnection("./testdata", "./testdata/metadata", "events", false, 0, 0, nil)
	assert.Nil(t, err)
	assert.NotNil(t, c)
	assert.NotNil(t, c.TableBuffers)
	assert.Equal(t, len(c.TableBuffers), 1)
	assert.NotNil(t, c.TableBuffers["events"])
}

func TestCreateRecursiveConnection(t *testing.T) {
	os.RemoveAll("./testdata/metadata/user")
	os.RemoveAll("./testdata/metadata/events")
	os.RemoveAll("./testdata/metadata/ab_test")
	os.RemoveAll("./testdata/metadata/dss_id")
	os.RemoveAll("./testdata/metadata/guest_id")
	os.RemoveAll("./testdata/metadata/anonymous_id")
	os.RemoveAll("./testdata/metadata/session_id")
	os.RemoveAll("./testdata/metadata/subscription_id")
	c, err := OpenConnection("./testdata", "./testdata/metadata", "user", true, 0, 0, nil)
	assert.Nil(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, len(c.TableBuffers), 6)
}