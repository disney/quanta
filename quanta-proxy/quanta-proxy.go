package main

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/rel"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/araddon/qlbridge/schema"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/hashicorp/consul/api"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	mysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/server"
	"github.com/siddontang/go-mysql/test_util/test_keys"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/disney/quanta/core"
	"github.com/disney/quanta/custom/functions"
	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/sink"
	"github.com/disney/quanta/source"
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
	src             *source.QuantaSource
	sessionPoolSize int
	quantaPort      *int
	consulAddr      string
	logging         *string
	environment     *string
	proxyHostPort   *string
	username        *string
	password        *string
	reWhitespace    *regexp.Regexp
	publicKeySet    []*jwk.Set
	userPool        sync.Map
	authProvider    *AuthProvider
	userClaimsKey   string
	metrics         *cloudwatch.CloudWatch
	connectCount    *Counter
	queryCount      *Counter
	updateCount     *Counter
	insertCount     *Counter
	deleteCount     *Counter
	connectCountL   *Counter
	queryCountL     *Counter
	updateCountL    *Counter
	insertCountL    *Counter
	deleteCountL    *Counter
	queryTime       *Counter
	updateTime      *Counter
	insertTime      *Counter
	deleteTime      *Counter
)

func main() {

	app := kingpin.New("quanta-proxy", "MySQL Proxy adapter to Quanta").DefaultEnvars()
	app.Version("Version: " + Version + "\nBuild: " + Build)

	logging = app.Flag("log-level", "Logging level [ERROR, WARN, INFO, DEBUG]").Default("WARN").String()
	environment = app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	proxyHostPort = app.Flag("proxy-host-port", "Host:port mapping of MySQL Proxy server").Default("0.0.0.0:4000").String()
	quantaPort = app.Flag("quanta-port", "Port number for Quanta service").Default("4000").Int()
	publicKeyURL := app.Arg("public-key-url", "URL for JWT public key.").String()
	region := app.Arg("region", "AWS region for cloudwatch metrics").Default("us-east-1").String()
	tokenservicePort := app.Arg("tokenservice-port", "Token exchance service port").Default("4001").Int()
	userKey := app.Flag("user-key", "Key used to get user id from JWT claims").Default("username").String()
	username = app.Flag("username", "User account name for MySQL DB").Default("root").String()
	password = app.Flag("password", "Password for account for MySQL DB (just press enter for now when logging in on mysql console)").Default("").String()
	consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()
	poolSize := app.Flag("session-pool-size", "Session pool size").Int()

	kingpin.MustParse(app.Parse(os.Args[1:]))

	if strings.ToUpper(*logging) == "DEBUG" || strings.ToUpper(*logging) == "TRACE" {
		if strings.ToUpper(*logging) == "TRACE" {
			expr.Trace = true
		}
		u.SetupLogging("debug")
	} else {
		shared.InitLogging(*logging, *environment, "Proxy", Version, "Quanta")
	}

	go func() {
		// Initialize Prometheus metrics endpoint.
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()

	consulAddr = *consul
	log.Printf("Connecting to Consul at: [%s] ...\n", consulAddr)
	consulConfig := &api.Config{Address: consulAddr}
	errx := shared.RegisterSchemaChangeListener(consulConfig, schemaChangeListener)
	if errx != nil {
		u.Error(errx)
		os.Exit(1)
	}

	if publicKeyURL != nil {
		publicKeySet = make([]*jwk.Set, 0)
		urls := strings.Split(*publicKeyURL, ",")
		for _, url := range urls {
			log.Printf("Retrieving JWT public key from [%s]", url)
			keySet, err := jwk.Fetch(url)
			if err != nil {
				u.Error(err)
				os.Exit(1)
			}
			publicKeySet = append(publicKeySet, keySet)
		}
	}
	userClaimsKey = *userKey
	// Start the token exchange service
	log.Printf("Starting the token exchange service on port %d", *tokenservicePort)
	authProvider = NewAuthProvider() // this instance is global used by tokenservice
	StartTokenService(*tokenservicePort, authProvider)

	// If the pool size is not configured then set it to the number of available CPUs
	sessionPoolSize = *poolSize
	if sessionPoolSize == 0 {
		sessionPoolSize = runtime.NumCPU()
		log.Printf("Session Pool Size not set, defaulting to number of available CPUs = %d", sessionPoolSize)
	} else {
		log.Printf("Session Pool Size = %d", sessionPoolSize)
	}

	// Match 2 or more whitespace chars inside string
	reWhitespace = regexp.MustCompile(`[\s\p{Zs}]{2,}`)

	// load all of our built-in functions
	builtins.LoadAllBuiltins()
	sink.LoadAll()      // Register output sinks
	functions.LoadAll() // Custom functions

	sess, errx := session.NewSession(&aws.Config{
		Region: aws.String(*region),
	})
	if errx != nil {
		u.Error(errx)
		os.Exit(1)
	}
	metrics = cloudwatch.New(sess)

	var err error
	src, err = source.NewQuantaSource("", consulAddr, *quantaPort, sessionPoolSize)
	if err != nil {
		u.Error(err)
	}
	schema.RegisterSourceAsSchema("quanta", src)

	// Start metrics publisher
	var ticker *time.Ticker
	ticker = metricsTicker(src)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			u.Warn("Interrupted,  shutting down ...")
			ticker.Stop()
			src.Close()
			os.Exit(0)
		}
	}()

	queryCount = &Counter{}
	updateCount = &Counter{}
	insertCount = &Counter{}
	deleteCount = &Counter{}
	connectCount = &Counter{}
	queryCountL = &Counter{}
	updateCountL = &Counter{}
	insertCountL = &Counter{}
	deleteCountL = &Counter{}
	connectCountL = &Counter{}
	queryTime = &Counter{}
	updateTime = &Counter{}
	insertTime = &Counter{}
	deleteTime = &Counter{}

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
	src.GetSessionPool().Recover(nil)
	src.GetSessionPool().Lock()
	defer src.GetSessionPool().Unlock()
	switch e.Event {
	case shared.Drop:
		schema.DefaultRegistry().SchemaDrop("quanta", e.Table, lex.TokenTable)
		log.Printf("Dropped table %s", e.Table)
	case shared.Modify:
		log.Printf("Truncated table %s", e.Table)
		time.Sleep(time.Second * 5)
		schema.DefaultRegistry().SchemaRefresh("quanta")
	case shared.Create:
		schema.DefaultRegistry().SchemaDrop("quanta", "quanta", lex.TokenSource)
		var err error
		src, err = source.NewQuantaSource("", consulAddr, *quantaPort, sessionPoolSize)
		if err != nil {
			u.Error(err)
		}
		schema.RegisterSourceAsSchema("quanta", src)
		//schema.DefaultRegistry().SchemaRefresh("quanta")
		log.Printf("Created table %s", e.Table)
	}
}

func onConn(conn net.Conn) {
	var tlsConf = server.NewServerTLSConfig(test_keys.CaPem, test_keys.CertPem, test_keys.KeyPem, tls.VerifyClientCertIfGiven)
	svr := server.NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, test_keys.PubPem, tlsConf)
	authProvider := NewAuthProvider() // Per connection (session) instance
	handler := NewProxyHandler(authProvider)
	sconn, err := server.NewCustomizedConn(conn, svr, authProvider, handler)
	if err != nil {
		if err.Error() == "invalid sequence 32 != 1" {
			return
		}
		u.Errorf(err.Error())
		u.Errorf("error from remote address %v", conn.RemoteAddr())
		return
	}
	defer handler.Close()
	connectCount.Add(1)
	// Dispatch loop
	for {
		if sconn == nil {
			break
		}
		if err := sconn.HandleCommand(); err != nil {
			if err.Error() != "connection closed" {
				u.Debug(err.Error())
			}
			break
		}
	}
}

// ProxyHandler - Handler type definition (lack of generics) for use via MySQL connection
type ProxyHandler struct {
	authProvider *AuthProvider
	db           *sql.DB
	stmts        map[interface{}]*sql.Stmt
}

// NewProxyHandler - Create a new proxy handler
func NewProxyHandler(authProvider *AuthProvider) *ProxyHandler {

	h := &ProxyHandler{authProvider: authProvider, stmts: make(map[interface{}]*sql.Stmt, 0)}
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

func (h *ProxyHandler) handleQuery(query string, args []interface{}, binary bool, 
		ctx interface{}) (*mysql.Result, error) {

	var stmt *sql.Stmt
	if ctx != nil {
		stmt = h.stmts[ctx] 
	}
	u.Debugf("found cached stmt %#p", stmt)

	// Ignore java driver handshake
	if strings.Contains(strings.ToLower(query), "mysql-connector-java") {
		r, err := generateJavaDriverHandshake(binary)
		if err != nil {
			return nil, err
		}
		return &mysql.Result{0, 0, 0, r}, nil
	}

	u.Debugf("handleQuery called with [%v], arg count = %d", query, len(args))

	query = reWhitespace.ReplaceAllString(query, " ")
	hasInto := strings.Contains(strings.ToLower(query), "into")
	ss := strings.Split(query, " ")
	if strings.ToLower(ss[0]) == "select" && hasInto {
		ss[0] = "selectinto"
	}

	operation := strings.ToLower(ss[0])
	switch operation {
	case "begin", "commit", "rollback":
		return nil, nil   // Just returns an "OK" packet

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
			r, err = mysql.BuildSimpleResultset([]string{"@@version_comment"}, [][]interface{}{
				{"- Quanta version " + Version + " - Build: " + Build},
			}, binary)
			if err != nil {
				return nil, err
			}
			return &mysql.Result{0, 0, 0, r}, nil
		}
		//for handling go mysql driver select @@isolation_level
		if strings.Contains(strings.ToLower(query), "transaction_isolation") {
			r, err = mysql.BuildSimpleResultset([]string{"@@transaction_isolation"}, [][]interface{}{
				{"READ UNCOMMITTED"},
			}, binary)
			if err != nil {
				return nil, err
			}
			return &mysql.Result{0, 0, 0, r}, nil
		}
		//for handling go mysql driver select @@isolation_level
		if strings.Contains(strings.ToLower(query), "show collation") {
			r, err = mysql.BuildSimpleResultset([]string{""}, [][]interface{}{
				{""},
			}, binary)
			if err != nil {
				return nil, err
			}
			return &mysql.Result{0, 0, 0, r}, nil
		}
		if strings.Contains(strings.ToLower(query), "select cast") {
			r, err = mysql.BuildSimpleResultset([]string{""}, [][]interface{}{
				{""},
			}, binary)
			if err != nil {
				return nil, err
			}
			return &mysql.Result{0, 0, 0, r}, nil
		}
		//u.Errorf("running query [%v]", query)
		start := time.Now()
		var rows *sql.Rows
		var err2 error
		if stmt != nil {
			rows, err2 = stmt.Query(args...)
			if err2 != nil {
				u.Errorf("could not execute prepared query: %v - %v", err2, query)
				return nil, err2
			}
		} else {
			rows, err2 = h.db.Query(query, args...)
			if err2 != nil {
				u.Errorf("could not execute query: %v", err2)
				return nil, err2
			}
		}
		defer rows.Close()
		queryCount.Add(1)

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
		elapsed := time.Since(start)
		queryTime.Add(int(elapsed.Milliseconds()))
		r, err = mysql.BuildSimpleResultset(cols, rs, binary)
		if err != nil {
			return nil, fmt.Errorf("%v", err)
		}
		return &mysql.Result{0, 0, 0, r}, nil
	case "insert", "delete", "update", "replace", "selectinto":
		h.checkSessionUserID(true)
		start := time.Now()
		var result sql.Result
		var err error
		if stmt != nil {
			result, err = stmt.Exec(args...)
			if err != nil {
				u.Errorf("could not execute prepared stmt: %v - %v", err, query)
				return nil, err
			}
		} else {
			result, err = h.db.Exec(query, args...)
			if err != nil {
				u.Errorf("could not execute stmt: %v", err)
				return nil, err
			}
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
		elapsed := time.Since(start)
		if operation == "update" {
			updateTime.Add(int(elapsed.Milliseconds()))
			updateCount.Add(1)
		}
		if operation == "insert" {
			insertTime.Add(int(elapsed.Milliseconds()))
			insertCount.Add(1)
		}
		if operation == "delete" {
			deleteTime.Add(int(elapsed.Milliseconds()))
			deleteCount.Add(1)
		}
		return &mysql.Result{0, uint64(insertID), uint64(rowCount), nil}, nil
	case "set":
		h.checkSessionUserID(false)
		_, err := h.db.Exec(query, args...)
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
	return h.handleQuery(query, nil, false, nil)
}

// HandleFieldList - Generate field list.
func (h *ProxyHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}

// HandleStmtPrepare - Process prepared statements.
func (h *ProxyHandler) HandleStmtPrepare(sql string) (params int, columns int, ctx interface{}, err error) {

	params = strings.Count(sql, "?")
	psql := strings.ReplaceAll(sql, "?", "0")
	var stmt rel.SqlStatement
	stmt, err = rel.ParseSql(psql)
	if err != nil {
		return
	}
	switch v := interface{}(stmt).(type) {
	case *rel.SqlSelect:
		//columns = len(v.Columns)
		// Prepare not implemented. Silently ignored and creates new stmt upon query
	case *rel.SqlUpdate:
		// Prepare not implemented. Silently ignored and creates new stmt upon exec
	case *rel.SqlDelete:
		// Prepare not implemented. Silently ignored and creates new stmt upon exec
	case *rel.SqlInsert:
		var table *schema.Table
		table, err = src.Table(v.Table)
		if err != nil {
			return
		}
		for _, x := range v.Columns {
			if !table.HasField(x.SourceField) {
				err = fmt.Errorf("field %s.%s does not exist", v.Table, x.SourceField)
				return
			}
		}
	default:
		err = fmt.Errorf("unhandled type %T", v)
	}
	s, errx := h.db.Prepare(sql)
	if errx != nil {
		err = errx
	} else {
		ctx = s
		h.stmts[ctx] = s
	}
	return
}

// HandleStmtClose - Handle Close
func (h *ProxyHandler) HandleStmtClose(ctx interface{}) error {
	
	stmt, ok := h.stmts[ctx]
	if ok {
		stmt.Close()
		delete(h.stmts, ctx)
	}
	return nil
}

func (h *ProxyHandler) Close() {

	for k, v := range h.stmts {
		v.Close()
		delete(h.stmts, k)
	}
	h.db.Close()
}

// HandleStmtExecute - Handle Execute
func (h *ProxyHandler) HandleStmtExecute(ctx interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return h.handleQuery(query, args, true, ctx)
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

// Counter - Generic counter with mutex (threading) support
type Counter struct {
	num  int64
	lock sync.Mutex
}

// Add function provides thread safe addition of counter value based on input parameter.
func (c *Counter) Add(n int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.num += int64(n)
}

// Get function provides thread safe read of counter value.
func (c *Counter) Get() (ret int64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	ret = c.num
	return
}

// Set function provides thread safe set of counter value.
func (c *Counter) Set(n int64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.num = n
	return
}

func metricsTicker(src *source.QuantaSource) *time.Ticker {
	t := time.NewTicker(time.Second * 10)
	start := time.Now()
	lastTime := time.Now()
	go func() {
		for range t.C {
			duration := time.Since(start)
			lastTime = publishMetrics(duration, lastTime, src)
		}
	}()
	return t
}

// Global storage for Prometheus metrics
var (
	pQueryCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "query_count",
		Help: "The total number of queries processed",
	})

	pQueriesPerSec = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "queries_per_second",
		Help: "The total number of queries processed per second",
	})

	pAvgQueryLatency = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "avg_query_latency",
		Help: "Average query latency in milliseconds",
	})

	pUpdateCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "update_count",
		Help: "The total number of updates processed",
	})

	pUpdatesPerSec = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "updates_per_second",
		Help: "The total number of updates processed per second",
	})

	pAvgUpdateLatency = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "avg_update_latency",
		Help: "Average update latency in milliseconds",
	})

	pInsertCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "insert_count",
		Help: "The total number of inserts processed",
	})

	pInsertsPerSec = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "inserts_per_second",
		Help: "The total number of inserts processed per second",
	})

	pAvgInsertLatency = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "avg_insert_latency",
		Help: "Average insert latency in milliseconds",
	})

	pDeleteCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "delete_count",
		Help: "The total number of deletes processed",
	})

	pDeletesPerSec = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "deletes_per_second",
		Help: "The total number of deletes processed per second",
	})

	pAvgDeleteLatency = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "avg_delete_latency",
		Help: "Average delete latency in milliseconds",
	})

	pUptimeHours = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "uptime_hours",
		Help: "Hours of up time",
	})

	pConnPoolSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "connection_pool_size",
		Help: "The size of the Quanta session pool",
	})

	pConnInUse = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "connections_in_use",
		Help: "Number of Quanta sessions currently (actively) in use.",
	})

	pConnPooled = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "connections_in_pool",
		Help: "Number of Quanta sessions currently pooled.",
	})

	pConnMaxInUse = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "max_connections_in_use",
		Help: "Maximum nunber of Quanta sessions in use.",
	})

	pConnPerSec = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "connections_per_sec",
		Help: "Number of Quanta sessions requested per second.",
	})
)

func publishMetrics(upTime time.Duration, lastPublishedAt time.Time, src *source.QuantaSource) time.Time {

	connectionPoolSize, connectionsInUse, pooled, maxUsed := src.GetSessionPool().Metrics()
	interval := time.Since(lastPublishedAt).Seconds()
	avgQueryLatency := float64(queryTime.Get())
	if queryCount.Get() > 0 {
		avgQueryLatency = float64(queryTime.Get()) / float64(queryCount.Get())
	}
	avgUpdateLatency := float64(updateTime.Get())
	if updateCount.Get() > 0 {
		avgUpdateLatency = float64(updateTime.Get()) / float64(updateCount.Get())
	}
	avgInsertLatency := float64(insertTime.Get())
	if insertCount.Get() > 0 {
		avgInsertLatency = float64(insertTime.Get()) / float64(insertCount.Get())
	}
	avgDeleteLatency := float64(deleteTime.Get())
	if deleteCount.Get() > 0 {
		avgDeleteLatency = float64(deleteTime.Get()) / float64(deleteCount.Get())
	}
	_, err := metrics.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: aws.String("Quanta-Proxy"),
		MetricData: []*cloudwatch.MetricDatum{
			{
				MetricName: aws.String("ConnectionPoolSize"),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(float64(connectionPoolSize)),
			},
			{
				MetricName: aws.String("ConnectionsInUse"),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(float64(connectionsInUse)),
			},
			{
				MetricName: aws.String("MaxConnectionsInUse"),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(float64(maxUsed)),
			},
			{
				MetricName: aws.String("ConnectionsInPool"),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(float64(pooled)),
			},
			{
				MetricName: aws.String("ConnectionsPerSec"),
				Unit:       aws.String("Count/Second"),
				Value:      aws.Float64(float64(connectCount.Get()-connectCountL.Get()) / interval),
			},
			{
				MetricName: aws.String("Queries"),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(float64(queryCount.Get())),
			},
			{
				MetricName: aws.String("QueriesPerSec"),
				Unit:       aws.String("Count/Second"),
				Value:      aws.Float64(float64(queryCount.Get()-queryCountL.Get()) / interval),
			},
			{
				MetricName: aws.String("AvgQueryLatency"),
				Unit:       aws.String("Milliseconds"),
				Value:      aws.Float64(float64(avgQueryLatency)),
			},
			{
				MetricName: aws.String("Updates"),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(float64(updateCount.Get())),
			},
			{
				MetricName: aws.String("UpdatesPerSec"),
				Unit:       aws.String("Count/Second"),
				Value:      aws.Float64(float64(updateCount.Get()-updateCountL.Get()) / interval),
			},
			{
				MetricName: aws.String("AvgUpdateLatency"),
				Unit:       aws.String("Milliseconds"),
				Value:      aws.Float64(float64(avgUpdateLatency)),
			},
			{
				MetricName: aws.String("Inserts"),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(float64(insertCount.Get())),
			},
			{
				MetricName: aws.String("InsertsPerSec"),
				Unit:       aws.String("Count/Second"),
				Value:      aws.Float64(float64(insertCount.Get()-insertCountL.Get()) / interval),
			},
			{
				MetricName: aws.String("AvgInsertLatency"),
				Unit:       aws.String("Milliseconds"),
				Value:      aws.Float64(float64(avgInsertLatency)),
			},
			{
				MetricName: aws.String("Deletes"),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(float64(deleteCount.Get())),
			},
			{
				MetricName: aws.String("DeletesPerSec"),
				Unit:       aws.String("Count/Second"),
				Value:      aws.Float64(float64(deleteCount.Get()-deleteCountL.Get()) / interval),
			},
			{
				MetricName: aws.String("AvgDeleteLatency"),
				Unit:       aws.String("Milliseconds"),
				Value:      aws.Float64(float64(avgDeleteLatency)),
			},
			{
				MetricName: aws.String("UpTimeHours"),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(float64(upTime) / float64(1000000000*3600)),
			},
		},
	})
	// Update Prometheus metrics
	pQueryCount.Set(float64(queryCount.Get()))
	pQueriesPerSec.Set(float64(queryCount.Get()-queryCountL.Get()) / interval)
	pAvgQueryLatency.Set(float64(avgQueryLatency))
	pUptimeHours.Set(float64(upTime) / float64(1000000000*3600))
	pConnPoolSize.Set(float64(connectionPoolSize))
	pConnInUse.Set(float64(connectionsInUse))
	pConnMaxInUse.Set(float64(maxUsed))
	pConnPooled.Set(float64(pooled))
	pConnPerSec.Set(float64(connectCount.Get()-connectCountL.Get()) / interval)
	pUpdateCount.Set(float64(updateCount.Get()))
	pUpdatesPerSec.Set(float64(updateCount.Get()-updateCountL.Get()) / interval)
	pAvgUpdateLatency.Set(float64(avgUpdateLatency))
	pInsertCount.Set(float64(insertCount.Get()))
	pInsertsPerSec.Set(float64(insertCount.Get()-insertCountL.Get()) / interval)
	pAvgInsertLatency.Set(float64(avgInsertLatency))
	pDeleteCount.Set(float64(deleteCount.Get()))
	pDeletesPerSec.Set(float64(deleteCount.Get()-deleteCountL.Get()) / interval)
	pAvgDeleteLatency.Set(float64(avgDeleteLatency))

	connectCountL.Set(connectCount.Get())
	queryCountL.Set(queryCount.Get())
	updateCountL.Set(updateCount.Get())
	insertCountL.Set(insertCount.Get())
	deleteCountL.Set(deleteCount.Get())
	if err != nil {
		u.Error(err)
	}
	return time.Now()
}
