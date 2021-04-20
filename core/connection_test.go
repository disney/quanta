package core

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	_ "github.com/stretchr/testify/require"
)

func TestCreateConnection(t *testing.T) {
	os.RemoveAll("./testdata/metadata/cities")
	c, err := OpenConnection("./testdata", "./testdata/metadata", "cities", false, 0, 0, nil)
	assert.Nil(t, err)
	assert.NotNil(t, c)
	assert.NotNil(t, c.TableBuffers)
	assert.Equal(t, len(c.TableBuffers), 1)
	assert.NotNil(t, c.TableBuffers["cities"])
}

