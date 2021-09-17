package main

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"github.com/araddon/qlbridge/expr"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/lex"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"net"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr/builtins"

	"github.com/lestrrat-go/jwx/jwk"
	mysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/server"
	"github.com/siddontang/go-mysql/test_util/test_keys"
    "github.com/hashicorp/consul/api"

	"github.com/disney/quanta/core"
	"github.com/disney/quanta/custom/functions"
	"github.com/disney/quanta/sink"
	"github.com/disney/quanta/source"
	"github.com/disney/quanta/shared"
)

// Variables to identify the build
var (
	Version string
	Build   string
)

// Exit Codes
const (
	Success         = 0
	InvalidHostPort = 100
)

var (
	logging       *string
	environment   *string
	proxyHostPort *string
	username      *string
	password      *string
	reWhitespace  *regexp.Regexp
	publicKeySet  []*jwk.Set
	userPool      sync.Map
	authProvider  *AuthProvider
	userClaimsKey string
    needsRefresh  bool
)

func main() {

	app := kingpin.New("quanta-proxy", "MySQL Proxy adapter to Quanta").DefaultEnvars()
	app.Version("Version: " + Version + "\nBuild: " + Build)

	logging = app.Flag("logging", "Logging level [debug, info]").Default("info").String()
	environment = app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	proxyHostPort = app.Flag("proxy-host-port", "Host:port mapping of MySQL Proxy server").Default("0.0.0.0:4000").String()
	quantaPort := app.Flag("quanta-port", "Port number for Quanta service").Default("4000").Int()
	publicKeyURL := app.Arg("public-key-url", "URL for JWT public key.").String()
	tokenservicePort := app.Arg("tokenservice-port", "Token exchance service port").Default("4001").Int()
	userKey := app.Flag("user-key", "Key used to get user id from JWT claims").Default("username").String()
	username = app.Flag("username", "User account name for MySQL DB").Default("root").String()
	password = app.Flag("password", "Password for account for MySQL DB (just press enter for now when logging in on mysql console)").Default("").String()
	consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()

	kingpin.MustParse(app.Parse(os.Args[1:]))

	if strings.ToUpper(*logging) == "DEBUG" || strings.ToUpper(*logging) == "TRACE" {
		if strings.ToUpper(*logging) == "TRACE" {
			expr.Trace = true
		}
		u.SetupLogging("debug")
	} else {
		core.InitLogging(*logging, *environment, "Proxy", Version, "Quanta")
	}

	consulAddr := *consul
	u.Infof("Connecting to Consul at: [%s] ...\n", consulAddr)
    consulConfig := &api.Config{Address: consulAddr}
    errx := shared.RegisterSchemaChangeListener(consulConfig, schemaChangeListener)
    if errx != nil {
        log.Fatal(errx)
    }


	if publicKeyURL != nil {
		publicKeySet = make([]*jwk.Set, 0)
		urls := strings.Split(*publicKeyURL, ",")
		for _, url := range urls {
			u.Infof("Retrieving JWT public key from [%s]", url)
			keySet, err := jwk.Fetch(url)
			if err != nil {
				log.Fatal(err)
			}
			publicKeySet = append(publicKeySet, keySet)
		}
	}
	userClaimsKey = *userKey
	// Start the token exchange service
	u.Infof("Starting the token exchange service on port %d", *tokenservicePort)
	authProvider = NewAuthProvider() // this instance is global used by tokenservice
	StartTokenService(*tokenservicePort, authProvider)

	// Match 2 or more whitespace chars inside string
	reWhitespace = regexp.MustCompile(`[\s\p{Zs}]{2,}`)

	// load all of our built-in functions
	builtins.LoadAllBuiltins()
	sink.LoadAll()      // Register output sinks
	functions.LoadAll() // Custom functions

	var err error
	var src *source.QuantaSource

	src, err = source.NewQuantaSource("", consulAddr, *quantaPort)
	if err != nil {
		log.Println(err)
	}
	schema.RegisterSourceAsSchema("quanta", src)

	// Start server endpoint
	l, err := net.Listen("tcp", *proxyHostPort)
	if err != nil {
		panic(err.Error())
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			u.Errorf(err.Error())
			return
		}
		go onConn(conn)
	}

}

func schemaChangeListener(e shared.SchemaChangeEvent) {

    core.ClearTableCache()
    needsRefresh = true
    switch e.Event {
        case shared.Drop:
            schema.DefaultRegistry().SchemaDrop("quanta", e.Table, lex.TokenTable)
            u.Infof("Dropped table %s", e.Table)
    }
}

func onConn(conn net.Conn) {
	var tlsConf = server.NewServerTLSConfig(test_keys.CaPem, test_keys.CertPem, test_keys.KeyPem, tls.VerifyClientCertIfGiven)
	svr := server.NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, test_keys.PubPem, tlsConf)
	authProvider := NewAuthProvider() // Per connection (session) instance
	sconn, err := server.NewCustomizedConn(conn, svr, authProvider, NewProxyHandler(authProvider))
	if err != nil {
		u.Errorf(err.Error())
		return
	}

	// Dispatch loop
	for {
		if sconn == nil {
			break
		}
		if err := sconn.HandleCommand(); err != nil {
			u.Errorf(err.Error())
			break
		}

	}
}

// ProxyHandler - Handler type definition (lack of generics) for use via MySQL connection
type ProxyHandler struct {
	authProvider *AuthProvider
	db           *sql.DB
}

// NewProxyHandler - Create a new proxy handler
func NewProxyHandler(authProvider *AuthProvider) *ProxyHandler {


	h := &ProxyHandler{authProvider: authProvider}
	var err error
	h.db, err = sql.Open("qlbridge", "quanta")
	if err != nil {
		panic(err.Error())
	}
	return h
}

func (h *ProxyHandler) checkSessionUserID(enforce bool) error {

	if userID, ok := h.authProvider.GetCurrentUserID(); ok {
		setter := fmt.Sprintf("set @userid = '%s'", userID)
		if _, err := h.db.Exec(setter); err != nil {
			return err
		}
	} else if enforce {
		return fmt.Errorf("User ID must be set,  run exec  'set @userid = <userID>'")
	}
	return nil
}

// UseDB - Set DB schema name
func (h *ProxyHandler) UseDB(dbName string) error {
	h.checkSessionUserID(true)
	u.Debugf("UseDB handler called with '%s'\n", dbName)
	return nil
}

func (h *ProxyHandler) handleQuery(query string, binary bool) (*mysql.Result, error) {

    if needsRefresh {
        needsRefresh = false
        var err error
        h.db.Close()
        schema.DefaultRegistry().SchemaRefresh("quanta")
        h.db, err = sql.Open("qlbridge", "quanta")
        if err != nil {
            panic(err.Error())
        }
    }

	// Ignore java driver handshake
	if strings.Contains(strings.ToLower(query), "mysql-connector-java") {
		r, err := generateJavaDriverHandshake(binary)
		if err != nil {
			return nil, err
		}
		return &mysql.Result{0, 0, 0, r}, nil
	}

	u.Debugf("handleQuery called with [%v]\n", query)

	query = reWhitespace.ReplaceAllString(query, " ")
	hasInto := strings.Contains(strings.ToLower(query), "into")
	ss := strings.Split(query, " ")
	if strings.ToLower(ss[0]) == "select" && hasInto {
		ss[0] = "selectinto"
	}

	switch strings.ToLower(ss[0]) {
	case "select", "describe", "show":

		h.checkSessionUserID(true)

		var r *mysql.Resultset
		var err error

		//for handling go mysql driver select @@max_allowed_packet
		if strings.Contains(strings.ToLower(query), "max_allowed_packet") {
			r, err = mysql.BuildSimpleResultset([]string{"@@max_allowed_packet"}, [][]interface{}{
				{mysql.MaxPayloadLen},
			}, binary)
			if err != nil {
				return nil, err
			}
			return &mysql.Result{0, 0, 0, r}, nil
		}
		//for handling go mysql driver select @@version_comment
		if strings.Contains(strings.ToLower(query), "version_comment") {
			r, err = mysql.BuildSimpleResultset([]string{"@@max_allowed_packet"}, [][]interface{}{
				{"V0.1"},
			}, binary)
			if err != nil {
				return nil, err
			}
			return &mysql.Result{0, 0, 0, r}, nil
		}

		u.Debugf("running query [%v]\n", query)
		rows, err2 := h.db.Query(query)
		if err2 != nil {
			u.Errorf("could not execute query: %v", err2)
			return nil, err2
		}
		defer rows.Close()

		cols, _ := rows.Columns()

		// This code is a hack to work around mapping slices to interface pointers
		readCols := make([]interface{}, len(cols))
		writeCols := make([]string, len(cols))
		for i := range writeCols {
			readCols[i] = &writeCols[i]
		}

		rs := make([][]interface{}, 0)
		for rows.Next() {
			row := make([]interface{}, len(cols))
			rows.Scan(readCols...)
			for i := range writeCols {
				row[i] = writeCols[i]
			}
			rs = append(rs, row)
		}
		// End hack

		r, err = mysql.BuildSimpleResultset(cols, rs, binary)
		if err != nil {
			return nil, fmt.Errorf("%v", err)
		}
		return &mysql.Result{0, 0, 0, r}, nil
	case "insert", "delete", "update", "replace", "selectinto":
		h.checkSessionUserID(true)
		result, err := h.db.Exec(query)
		if err != nil {
			u.Errorf("could not execute stmt: %v", err)
			return nil, err
		}
		insertID, err1 := result.LastInsertId()
		rowCount, err2 := result.RowsAffected()
		if err1 != nil {
			u.Errorf("could not execute stmt: %v", err1)
			return nil, err1
		}
		if err2 != nil {
			u.Errorf("could not execute stmt: %v", err2)
			return nil, err2
		}
		return &mysql.Result{0, uint64(insertID), uint64(rowCount), nil}, nil
	case "set":
		h.checkSessionUserID(false)
		_, err := h.db.Exec(query)
		if err != nil {
			u.Errorf("could not execute set: %v", err)
			return nil, err
		}
		return &mysql.Result{0, uint64(0), uint64(0), nil}, nil
	default:
		return nil, fmt.Errorf("invalid query %s", query)
	}
}

// HandleQuery - Handle incoming query.
func (h *ProxyHandler) HandleQuery(query string) (*mysql.Result, error) {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("Panic recover: \n" + string(debug.Stack()))
			u.Error(err)
		}
	}()
	return h.handleQuery(query, false)
}

// HandleFieldList - Generate field list.
func (h *ProxyHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}

// HandleStmtPrepare - Process prepared statements.
func (h *ProxyHandler) HandleStmtPrepare(sql string) (params int, columns int, ctx interface{}, err error) {
	ss := strings.Split(sql, " ")
	switch strings.ToLower(ss[0]) {
	case "select":
		params = 1
		columns = 2
	case "insert":
		params = 2
		columns = 0
	case "replace":
		params = 2
		columns = 0
	case "update":
		params = 1
		columns = 0
	case "delete":
		params = 1
		columns = 0
	default:
		err = fmt.Errorf("invalid prepare %s", sql)
	}
	return params, columns, nil, err
}

// HandleStmtClose - Handle Close
func (h *ProxyHandler) HandleStmtClose(context interface{}) error {
	h.db.Close()
	return nil
}

// HandleStmtExecute - Handle Execute
func (h *ProxyHandler) HandleStmtExecute(ctx interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return h.handleQuery(query, true)
}

// HandleOtherCommand - Handle Command
func (h *ProxyHandler) HandleOtherCommand(cmd byte, data []byte) error {
	return mysql.NewError(mysql.ER_UNKNOWN_ERROR, fmt.Sprintf("command %d is not supported now", cmd))
}

func generateJavaDriverHandshake(binary bool) (*mysql.Resultset, error) {

	return mysql.BuildSimpleResultset([]string{
		"auto_increment_increment",
		"character_set_client",
		"character_set_connection",
		"character_set_results",
		"character_set_server",
		"collation_server",
		"collation_connection",
		"init_connect",
		"interactive_timeout",
		"license",
		"lower_case_table_names",
		"max_allowed_packet",
		"net_buffer_length",
		"net_write_timeout",
		"query_cache_size",
		"query_cache_type",
		"sql_mode",
		"system_time_zone",
		"time_zone",
		"transaction_isolation",
		"wait_timeout",
	}, [][]interface{}{{
		1,
		"utf8",
		"utf8",
		"utf8",
		"utf8",
		"utf8_unicode_ci",
		"utf8_unicode_ci",
		"",
		28800,
		"Disney/Apache 2.0",
		0,
		mysql.MaxPayloadLen,
		1048576,
		60,
		0,
		0,
		"NO_ENGINE_SUBSTITUTION",
		"UTC",
		"UTC",
		"READ-UNCOMMITTED",
		31536000,
	}}, binary)
}
