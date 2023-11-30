package core

import (
	"testing"

	"github.com/disney/quanta/shared"
	"github.com/stretchr/testify/assert"
	_ "github.com/stretchr/testify/require"
)

// FIXME: make this work or delete. It never finishes. (nobody home at port 4000)
func xTestCreateSession(t *testing.T) {

	conn := shared.NewDefaultConnection()
	conn.ServicePort = 0
	errx := conn.Connect(nil)
	assert.Nil(t, errx)
	tableCache := NewTableCacheStruct()
	c, err := OpenSession(tableCache, "./testdata", "cities", false, conn)
	assert.Nil(t, err)
	assert.NotNil(t, c)
	assert.NotNil(t, c.TableBuffers)
	assert.Equal(t, len(c.TableBuffers), 1)
	assert.NotNil(t, c.TableBuffers["cities"])
}

// func TestCreateRecursiveSession(t *testing.T) {
// 	os.RemoveAll("./testdata/metadata/user")
// 	os.RemoveAll("./testdata/metadata/events")
// 	os.RemoveAll("./testdata/metadata/ab_test")
// 	os.RemoveAll("./testdata/metadata/dss_id")
// 	os.RemoveAll("./testdata/metadata/guest_id")
// 	os.RemoveAll("./testdata/metadata/anonymous_id")
// 	os.RemoveAll("./testdata/metadata/session_id")
// 	os.RemoveAll("./testdata/metadata/subscription_id")
// 	c, err := OpenSession("./testdata", "./testdata/metadata", "user", true, 0, 0, nil)
// 	assert.Nil(t, err)
// 	assert.NotNil(t, c)
// 	assert.Equal(t, len(c.TableBuffers), 6)
// }
