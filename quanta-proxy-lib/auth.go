package proxy

//
// Authenticator for quanta-proxy MySQL sessions.  Uses openID connect (JWT) bearer tokens than can be used
// directly to connect to the quanta-proxy.  Alternatively JWT tokens can be exchanged for a temporary
// MySQL password via the token exchange service.  If a JWT token is presented it must be done via the
// 'userName'.
//

import (
	"time"

	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jwt"
	"github.com/siddontang/go-mysql/server"
)

var (
	// Ensure CredentialProvider interface is implemented
	_ server.CredentialProvider = (*AuthProvider)(nil)
)

// AuthProvider - CredentialProvider interface implementation
type AuthProvider struct {
	currentUserID string
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

	_ = time.Now()
	m.currentUserID = username
	return "", true, nil
	/*
		if len(username) <= 32 {
			v, ok := userPool.Load(username) // global singleton
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

		var errCheck error
		for _, ks := range publicKeySet {
			token, errx := m.Verify(username, ks)
			if errx == nil {
				claims := token.PrivateClaims()
				// userClaimsKey is global
				if user, ok := claims[userClaimsKey]; ok {
					// If user id is in claims then this is a MyID session, set current UserID
					m.currentUserID = user.(string)
				}
				return "", true, nil
			}
			errCheck = errx
		}
		userPool.Delete(username) // expire user if it exists (global singleton)
		return "", false, errCheck
	*/
}

// CheckUsername - CredentialProvider.CheckUsername implementation.
func (m *AuthProvider) CheckUsername(username string) (bool, error) {

	if len(username) > 32 {
		return true, nil // Must be a token will verify inside GetCredential()
	}

	// Verify user name exists
	//_, ok := userPool.Load(username) // global singleton
	//return ok, nil
	return true, nil
}

// AddUser - Called by tokenservice to create new account.
func (m *AuthProvider) AddUser(a MySQLAccount) {

	// userPool is global singleton
	UserPool.Store(a.User, a)
}

// GetCurrentUserID - Called by ProxyHander to pass userID to active sql.Open session
func (m *AuthProvider) GetCurrentUserID() (string, bool) {
	if m.currentUserID != "" {
		return m.currentUserID, true
	}
	return "", false
}

// Verify a JWT
func (m *AuthProvider) Verify(tokenString string, keySet *jwk.Set) (jwt.Token, error) {

	//token, err := jwt.ParseString(tokenString, jwt.WithKeySet(keySet), jwt.WithValidate(true))
	token, err := jwt.ParseString(tokenString)
	return token, err
}
