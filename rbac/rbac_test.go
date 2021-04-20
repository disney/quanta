package rbac

import (
	"os"
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/disney/quanta/client"
	"github.com/disney/quanta/server"
)

type RBACTestSuite struct {
	suite.Suite
	client *quanta.KVStore
	server *server.KVStore
}

func (suite *RBACTestSuite) SetupSuite() {

	os.RemoveAll("./testdata/metadata")
	os.RemoveAll("./testdata/UserRoles")
	var err error
	u.SetupLogging("debug")

	endpoint, err := server.NewEndPoint("./testdata")
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), endpoint)
	endpoint.Port = 0 // Enable in memory instance
	endpoint.SetNode(server.NewDummyNode(endpoint))
	suite.server, err = server.NewKVStore(endpoint)
	assert.NoError(suite.T(), err)
	go func() {
		endpoint.Start()
	}()

	conn := quanta.NewDefaultConnection()
	conn.ServicePort = 0
	//conn.Quorum = 3
	err = conn.Connect()
	assert.NoError(suite.T(), err)

	suite.client = quanta.NewKVStore(conn)
	assert.NotNil(suite.T(), suite.client)
}

func (suite *RBACTestSuite) TearDownSuite() {
	suite.server.Shutdown()
	os.RemoveAll("./testdata/metadata")
	os.RemoveAll("./testdata/UserRoles")
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestRBACTestSuite(t *testing.T) {
	suite.Run(t, new(RBACTestSuite))
}

func (suite *RBACTestSuite) TestAUnknownUser() {
	_, err := NewAuthContext(suite.client, "USER001", false)
	assert.EqualError(suite.T(), err, "Unknown user USER001")
}

func (suite *RBACTestSuite) TestCreateUser() {
	ctx, err := NewAuthContext(suite.client, "USER001", true)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), ctx)
}

func (suite *RBACTestSuite) TestGrantInvalidGrantee() {
	ctx, err := NewAuthContext(suite.client, "USER001", false)
	assert.NoError(suite.T(), err)
	err = ctx.GrantRole(SystemAdmin, "junkgrantee", "junkdb", false)
	assert.EqualError(suite.T(), err, "Error in GrantRole(load) [User junkgrantee not found]")
}

func (suite *RBACTestSuite) TestGrantInvalidLevel() {
	_, err := NewAuthContext(suite.client, "USER002", true)
	assert.NoError(suite.T(), err)
	ctx, err := NewAuthContext(suite.client, "USER001", false)
	assert.NoError(suite.T(), err)
	err = ctx.GrantRole(SystemAdmin, "USER002", "junkdb", false)
	assert.EqualError(suite.T(), err, "Error in GrantRole [Cannot grant a role above grantor's level]")
}

func (suite *RBACTestSuite) TestGrantInvalidNoSelfGrant() {
	ctx, err := NewAuthContext(suite.client, "USER001", false)
	assert.NoError(suite.T(), err)
	err = ctx.GrantRole(SystemAdmin, "USER001", "junkdb", false)
	assert.EqualError(suite.T(), err, "Cannot grant roles to self")
}

func (suite *RBACTestSuite) TestGrantMissingGrantee() {
	ctx, err := NewAuthContext(suite.client, "USER001", false)
	assert.NoError(suite.T(), err)
	err = ctx.GrantRole(SystemAdmin, "", "junkdb", false)
	assert.EqualError(suite.T(), err, "Grantee must be specified")
}

func (suite *RBACTestSuite) TestGrantMissingRole() {
	_, err := NewAuthContext(suite.client, "USER002", true)
	assert.NoError(suite.T(), err)
	ctx, err := NewAuthContext(suite.client, "USER001", false)
	assert.NoError(suite.T(), err)
	err = ctx.GrantRole(0, "USER002", "junkdb", false)
	assert.EqualError(suite.T(), err, "Role must be specified")
}

func (suite *RBACTestSuite) TestGrantSuccess() {
	_, err := NewAuthContext(suite.client, "USER002", true)
	assert.NoError(suite.T(), err)
	ctx, err := NewAuthContext(suite.client, "USER001", false)
	assert.NoError(suite.T(), err)
	err = ctx.GrantRole(SystemAdmin, "USER001", "", true)
	assert.NoError(suite.T(), err)
	err = ctx.GrantRole(DomainUser, "USER002", "quanta", false)
	assert.NoError(suite.T(), err)
}

func (suite *RBACTestSuite) TestPermNoSystemAdmin() {

	ctx, err := NewAuthContext(suite.client, "USER002", false)
	assert.NoError(suite.T(), err)
	ok, err := ctx.IsAuthorized(CreateOrAlterView, "quanta")
	assert.NoError(suite.T(), err)
	assert.False(suite.T(), ok)
}

func (suite *RBACTestSuite) TestPermNoValidDatabase() {

	ctx, err := NewAuthContext(suite.client, "USER002", false)
	assert.NoError(suite.T(), err)
	ok, err := ctx.IsAuthorized(ViewDatabase, "invaliddbname")
	assert.EqualError(suite.T(), err, "Attempting ViewDatabase, user USER002 has NoRole for database invaliddbname")
	assert.False(suite.T(), ok)
}

func (suite *RBACTestSuite) TestPermSuccess() {

	ctx, err := NewAuthContext(suite.client, "USER002", false)
	assert.NoError(suite.T(), err)
	ok, err := ctx.IsAuthorized(ViewDatabase, "quanta")
	assert.NoError(suite.T(), err)
	assert.True(suite.T(), ok)
}
