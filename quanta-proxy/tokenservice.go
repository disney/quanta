package main
//
// TokenExchangeService is responsible for redeeming a JWT token and returning the user name and a
// temporary password that has an expiration timestamp obtained from the token.  It's primary purpose
// is to support authentication for MySQL compatible tools.
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

var (
	lowerCharSet   = "abcdedfghijklmnopqrst"
	upperCharSet   = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	specialCharSet = "!@#$%*"
	numberSet      = "0123456789"
	allCharSet     = lowerCharSet + upperCharSet + specialCharSet + numberSet
	minSpecialChar = 1
	minNum         = 1
	minUpperCase   = 1
	passwordLength = 8
)

// TokenExchangeService - Token service state.
type TokenExchangeService struct {
	port          int
	authProvider  *AuthProvider
    userClaimsKey string
}

// StartTokenService - Construct and initialize token service.
func StartTokenService(port int, userKey string, authProvider *AuthProvider) {

	ts := &TokenExchangeService{port: port, authProvider: authProvider, userClaimsKey: userKey}
	http.HandleFunc("/", ts.HandleRequest)

	go func() {
		var err = http.ListenAndServe(fmt.Sprintf(":%d", ts.port), nil)
		if err != nil {
			log.Fatalf("Server failed starting. Error: %s", err)
		}
	}()

}

// HandleRequest - Service request handler.
func (s *TokenExchangeService) HandleRequest(w http.ResponseWriter, r *http.Request) {
	log.Println("Incoming Request:", r.Method)
	switch r.Method {
	case http.MethodPost:
		s.CreateAccount(w, r)
		break
	default:
		ErrorResponse(&w, 405, "Method not allowed", "Method not allowed", nil)
		break
	}
}

// CreateAccount - Service request for a new account.
func (s *TokenExchangeService) CreateAccount(w http.ResponseWriter, r *http.Request) {

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		ErrorResponse(&w, 400, "Server error", "Server error", err)
		return
	}
	defer r.Body.Close()
	token, err := s.authProvider.Verify(string(buf), publicKeySet)
	if err != nil {
		ErrorResponse(&w, 401, "Access Denied", "Access Denied", err)
		return
	}

	var account MySQLAccount
	claims := token.PrivateClaims()
	if user, ok := claims[s.userClaimsKey]; ok {
		account.User = user.(string)
	} else {
		ErrorResponse(&w, 400, "Server error", "Server error", fmt.Errorf("cannot obtain username from token"))
		return
	}
	account.Expires = token.Expiration().Unix()
	rand.Seed(time.Now().Unix())
	account.Password = generatePassword(passwordLength, minSpecialChar, minNum, minUpperCase)
	s.authProvider.AddUser(account)
	SuccessResponse(&w, account)
}

func generatePassword(passwordLength, minSpecialChar, minNum, minUpperCase int) string {
	var password strings.Builder

	//Set special character
	for i := 0; i < minSpecialChar; i++ {
		random := rand.Intn(len(specialCharSet))
		password.WriteString(string(specialCharSet[random]))
	}

	//Set numeric
	for i := 0; i < minNum; i++ {
		random := rand.Intn(len(numberSet))
		password.WriteString(string(numberSet[random]))
	}

	//Set uppercase
	for i := 0; i < minUpperCase; i++ {
		random := rand.Intn(len(upperCharSet))
		password.WriteString(string(upperCharSet[random]))
	}

	remainingLength := passwordLength - minSpecialChar - minNum - minUpperCase
	for i := 0; i < remainingLength; i++ {
		random := rand.Intn(len(allCharSet))
		password.WriteString(string(allCharSet[random]))
	}
	inRune := []rune(password.String())
	rand.Shuffle(len(inRune), func(i, j int) {
		inRune[i], inRune[j] = inRune[j], inRune[i]
	})
	return string(inRune)
}
