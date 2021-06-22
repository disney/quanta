package rbac

import (
	"fmt"
	"reflect"

	u "github.com/araddon/gou"
	"github.com/disney/quanta/client"
	"gopkg.in/yaml.v2"
)

const (
	// UserRoles - Literal name for UserRole store (uses KVStore).
	UserRoles = "UserRoles"
)

// AuthContext - Authorization Services API
type AuthContext struct {
	Store  *quanta.KVStore // KVStore client
	UserID string          // User Identifier
}

// NewAuthContext - Construct API context for RBAC auth services
func NewAuthContext(store *quanta.KVStore, userID string, createUser bool) (*AuthContext, error) {

	if userID == "" {
		return nil, fmt.Errorf("User ID not specified")
	}
	if store == nil {
		return nil, fmt.Errorf("No connected session")
	}

	kvResult, err := store.Lookup(UserRoles, userID, reflect.String)
	if err != nil {
		return nil, fmt.Errorf("Error in NewAuthContext(Lookup UserRoles) [%v]", err)
	}

	if kvResult == nil && !createUser {
		return nil, fmt.Errorf("Unknown user %s", userID)
	}

	if kvResult == nil && createUser {
		user := User{UserID: userID}
		user.save(store)
	}
	return &AuthContext{Store: store, UserID: userID}, nil
}

// GrantRole - Grant or alter user role assignment
func (c *AuthContext) GrantRole(role Role, userID, database string, asRoot bool) error {

	if role == NoRole {
		return fmt.Errorf("Role must be specified")
	}
	if database == "" && role != SystemAdmin {
		return fmt.Errorf("Database must be specified")
	}
	if userID == "" {
		return fmt.Errorf("Grantee must be specified")
	}
	if c.UserID == userID && !asRoot {
		return fmt.Errorf("Cannot grant roles to self")
	}
	grantor, err := load(c.Store, c.UserID)
	if err != nil {
		return fmt.Errorf("Error in grantor GrantRole(load) [%v]", err)
	}
	grantee, err := load(c.Store, userID)
	if err != nil {
		return fmt.Errorf("Error in grantee GrantRole(load) [%v]", err)
	}

	if grantor == nil {
		return fmt.Errorf("Error in GrantRole(load) [User %s not found]", c.UserID)
	}
	if grantee == nil {
		return fmt.Errorf("Error in GrantRole(load) [User %s not found]", userID)
	}

	if role > grantor.getRole(database) && !asRoot {
		return fmt.Errorf("Error in GrantRole [Cannot grant a role above grantor's level]")
	}

	grantee.setRole(role, database)
	return grantee.save(c.Store)
}

// IsAuthorized - Check user permissions for a given database resource.
func (c *AuthContext) IsAuthorized(perm Permission, database string) (bool, error) {

	grantee, err := load(c.Store, c.UserID)
	if err != nil {
		return false, fmt.Errorf("Error in IsAuthorized(load) [%v]", err)
	}

	if grantee.IsSystemAdmin {
		return true, nil
	}

	authRole := grantee.getRole(database)
	if authRole == NoRole {
		err := fmt.Errorf("Attempting %s, user %s has NoRole for database %s", perm.String(), c.UserID, database)
		u.Error(err)
		return false, err
	}

	return perm <= authRole.MaxPermission(), nil
}

// Role - User roles
type Role int

// Constant defines for roles.
const (
	NoRole = Role(iota)
	DomainUser
	DomainAdmin
	SystemAdmin
)

// String - Return string respresentation of Role.
func (rt Role) String() string {

	switch rt {
	case NoRole:
		return "NoRole"
	case SystemAdmin:
		return "SystemAdmin"
	case DomainAdmin:
		return "DomainAdmin"
	case DomainUser:
		return "DomainUser"
	default:
		return "NoRole"
	}
}

// RoleFromString - Construct a Role from the string representation.
func RoleFromString(rt string) Role {

	switch rt {
	case "NoRole":
		return NoRole
	case "SystemAdmin":
		return SystemAdmin
	case "DomainAdmin":
		return DomainAdmin
	case "DomainUser":
		return DomainUser
	default:
		return NoRole
	}
}

// Permission - Access Permissions.
type Permission int

// Constant defines for permissions.
const (
	NoPermission = Permission(iota)
	CreateSession
	ViewDatabase
	WriteDatabase
	CreateOrAlterTable
	ExportData
	CreateOrAlterView
)

// String - Return string respresentation of Permission.
func (pt Permission) String() string {

	switch pt {
	case NoPermission:
		return "NoPermission"
	case CreateSession:
		return "CreateSession"
	case ViewDatabase:
		return "ViewDatabase"
	case WriteDatabase:
		return "WriteDatabase"
	case CreateOrAlterTable:
		return "CreateOrAlterTable"
	case CreateOrAlterView:
		return "CreateOrAlterView"
	case ExportData:
		return "ExportData"
	default:
		return "NoPermission"
	}
}

// PermissionFromString - Construct a Permission from the string representation.
func PermissionFromString(pt string) Permission {

	switch pt {
	case "NoPermission":
		return NoPermission
	case "CreateSession":
		return CreateSession
	case "ViewDatabase":
		return ViewDatabase
	case "WriteDatabase":
		return WriteDatabase
	case "CreateOrAlterTable":
		return CreateOrAlterTable
	case "CreateOrAlterView":
		return CreateOrAlterView
	case "ExportData":
		return ExportData
	default:
		return NoPermission
	}
}

// MaxPermission - Return max Permission for Role
func (rt Role) MaxPermission() Permission {

	switch rt {
	case NoRole:
		return NoPermission
	case DomainUser:
		return ViewDatabase
	case DomainAdmin:
		return ExportData
	case SystemAdmin:
		return CreateOrAlterView
	default:
		return NoPermission
	}

}

// DbRole - Encapsulates User to Role mapping for specific database schemas
type DbRole struct {
	Database string `yaml:"database"`
	Role     string `yaml:"role"`
}

// User to Role mapping container
type User struct {
	UserID        string   `yaml:"id"`
	IsSystemAdmin bool     `yaml:"isSystemAdmin"`
	DBRoles       []DbRole `yaml:"dbRoles"`
}

func load(store *quanta.KVStore, userID string) (*User, error) {

	b, err := store.Lookup(UserRoles, userID, reflect.String)
	if err != nil {
		return nil, fmt.Errorf("Error loading user [%v]", err)
	}
	if b == nil {
		return nil, nil
	}
	var user User
	err = yaml.Unmarshal([]byte(b.(string)), &user)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func (user *User) getRole(database string) Role {

	if user.IsSystemAdmin {
		return RoleFromString("SystemAdmin")
	}
	for _, dbRole := range user.DBRoles {
		if dbRole.Database == database {
			return RoleFromString(dbRole.Role)
		}
	}
	return NoRole
}

func (user *User) setRole(role Role, database string) {

	if role == SystemAdmin {
		user.IsSystemAdmin = true
		return
	}
	for i, dbRole := range user.DBRoles {
		if dbRole.Database == database {
			user.DBRoles[i].Role = role.String()
			return
		}
	}
	user.DBRoles = append(user.DBRoles, DbRole{Database: database, Role: role.String()})
	return
}

func (user *User) save(store *quanta.KVStore) error {

	b, err := yaml.Marshal(&user)
	if err != nil {
		return fmt.Errorf("Error in save(Marshal User for %s) [%v]", user.UserID, err)
	}
	if err := store.Put(UserRoles, user.UserID, string(b)); err != nil {
		return fmt.Errorf("Error in save(Put new UserRole for %s) [%v]", user.UserID, err)
	}
	u.Debugf("Saved user [%s]", string(b))
	return nil
}
