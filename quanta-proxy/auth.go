package main
//
// Authenticator for quanta-proxy MySQL sessions.  Uses openID connect (JWT) bearer tokens than can be used
// directly to connect to the quanta-proxy.  Alternatively JWT tokens can be exchanged for a temporary
// MySQL password via the token exchange service.  If a JWT token is presented it must be done via the
// 'userName'.
//

import (
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jwt"
	"github.com/siddontang/go-mysql/server"
	"sync"
	"time"
)

var (
	// Ensure CredentialProvider interface is implemented
	_ server.CredentialProvider = (*AuthProvider)(nil)
)

// AuthProvider - CredentialProvider interface implementation
type AuthProvider struct {
	userPool sync.Map
}

// MySQLAccount - State for accounts.
type MySQLAccount struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Expires  int64  `json:"expires"`
}

// NewAuthProvider - Construct NewAuthProvider.
func NewAuthProvider() *AuthProvider {
	return &AuthProvider{}
}

// GetCredential - CredentialProvider.GetCredential implementation.
func (m *AuthProvider) GetCredential(username string) (password string, found bool, err error) {

	if len(username) <= 32 {
		v, ok := m.userPool.Load(username)
		if !ok {
			return "", false, nil
		}
		account := v.(MySQLAccount)
		expireTs := time.Unix(account.Expires, 0)
		if expireTs.After(time.Now()) {
			return account.Password, true, nil
		}
		return "", false, nil // session has expired
	}

	_, errx := m.Verify(username, publicKeySet)
	if errx != nil {
		m.userPool.Delete(username) // expire user if it exists
		return "", false, errx
	}
	return "", true, nil
}

// CheckUsername - CredentialProvider.CheckUsername implementation.
func (m *AuthProvider) CheckUsername(username string) (bool, error) {

	if len(username) > 32 {
		return true, nil // Must be a token will verify inside GetCredential()
	}

	// Verify user name exists
	_, ok := m.userPool.Load(username)
	return ok, nil
}

// AddUser - Called by tokenservice to create new account.
func (m *AuthProvider) AddUser(a MySQLAccount) {
	m.userPool.Store(a.User, a)
}

// Verify a JWT
func (m *AuthProvider) Verify(tokenString string, keySet *jwk.Set) (jwt.Token, error) {

	token, err := jwt.ParseString(tokenString, jwt.WithKeySet(keySet), jwt.WithValidate(true))
	return token, err
}
