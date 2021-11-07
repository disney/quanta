package source

// QuantaSource - Implementation of the data source interfaces for query processor.

import (
	"database/sql/driver"
	"fmt"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/disney/quanta/core"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
	"io/ioutil"
	"log"
	"runtime"
	"strings"
	"sync"
)

const (
	sourceType = "quanta"
)

var (
	// Ensure QuantaSource implements schema.Source
	_ schema.Source = (*QuantaSource)(nil)

	// Ensure QuantaSource implements variety of Connection interfaces.
	_ schema.Conn        = (*QuantaSource)(nil)
	_ schema.ConnColumns = (*QuantaSource)(nil)
	_ schema.ConnScanner = (*QuantaSource)(nil)
)

// QuantaSource implements qlbridge `Source` to support Quanta indexes
// to have a Schema and implement and be operated on by Sql Statements.
type QuantaSource struct {
	*schema.Schema // schema
	exit           chan bool
	colIndex       map[string]int

	// Quanta specific after here
	//result         *pcli.QueryResult
	lastResultPos int
	baseDir       string
	servicePort   int
	consulClient  *api.Client
	connPoolMap   map[string]*ConnectionPool
	connPoolLock  sync.Mutex
}

// NewQuantaSource - Construct a QuantaSource.
func NewQuantaSource(baseDir, consulAddr string, servicePort int) (*QuantaSource, error) {

	m := &QuantaSource{}
	m.servicePort = servicePort
	var err error
	if consulAddr != "" {
		m.consulClient, err = api.NewClient(&api.Config{Address: consulAddr})
		if err != nil {
			return m, err
		}
	}

	m.baseDir = baseDir
	if m.baseDir != "" {
		log.Printf("Constructing QuantaSource at baseDir '%s'", baseDir)
	}

	// name is a string and cols is an []string
	m.exit = make(chan bool, 1)

	return m, nil
}

// Init initilize this db
func (m *QuantaSource) Init() {
	m.connPoolMap = make(map[string]*ConnectionPool)
}

// Setup this db with parent schema.
func (m *QuantaSource) Setup(ss *schema.Schema) error {

	m.Schema = ss
	return nil
}

// ConnectionPool - Pool of Quanta connections
type ConnectionPool struct {
	pool chan *core.Connection
}

func newConnectionPool() *ConnectionPool {
	return &ConnectionPool{pool: make(chan *core.Connection, runtime.NumCPU())}
}

func (m *QuantaSource) getPoolByTableName(tableName string) *ConnectionPool {
	m.connPoolLock.Lock()
	var cp *ConnectionPool
	var found bool
	if cp, found = m.connPoolMap[tableName]; !found {
		cp = newConnectionPool()
		m.connPoolMap[tableName] = cp
	}
	m.connPoolLock.Unlock()
	return cp
}

// BorrowConnection - get a pooled connection.
func (m *QuantaSource) BorrowConnection(tableName string) *core.Connection {

	cp := m.getPoolByTableName(tableName)
	select {
	case r := <-cp.pool:
		return r
	default:
		conn := m.NewConnection(tableName)
		if conn == nil {
			panic("borrowConnection cannot open new connection")
		}
		return conn
	}
}

// ReturnConnection - return a connection to the pool.
func (m *QuantaSource) ReturnConnection(tableName string, conn *core.Connection) {

	cp := m.getPoolByTableName(tableName)
	select {
	case cp.pool <- conn:
	default:
	}
	return
}

func (m *QuantaSource) NewConnection(tableName string) *core.Connection {

	conn, err := core.OpenConnection(m.baseDir, tableName, false, 0, m.servicePort, m.consulClient)
	if err != nil {
		u.Errorf("error opening quanta connection - %v", err)
		return nil
	}
	return conn
}

// Open a Conn for this source @table name
func (m *QuantaSource) Open(tableName string) (schema.Conn, error) {

	u.Debugf("Open(%v)", tableName)

	if m.Schema == nil {
		u.Warnf("no schema?")
		return nil, nil
	}
	tableName = strings.ToLower(tableName)
	tbl, err := m.Schema.Table(tableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		u.Errorf("Could not find table for '%s'.'%s'", m.Schema.Name, tableName)
		return nil, fmt.Errorf("Could not find '%v'.'%v' schema)", m.Schema.Name, tableName)
	}

	conn := m.BorrowConnection(tableName)
	if conn == nil {
		return nil, fmt.Errorf("Error opening connection for table %s.", tableName)
	}

	return NewSQLToQuanta(m, tbl, conn), nil
}

// Table by name
func (m *QuantaSource) Table(table string) (*schema.Table, error) {

	conn := m.BorrowConnection(table)
	if conn == nil {
		return nil, fmt.Errorf("Error opening connection for table %s.", table)
	}
	tb, found := conn.TableBuffers[table]
	if !found {
		return nil, fmt.Errorf("cannot find table buffer for %s", table)
	}
	ts := tb.Table
	pkMap := make(map[string]*core.Attribute)
	pka, _ := ts.GetPrimaryKeyInfo()
	for _, v := range pka {
		pkMap[v.FieldName] = v
	}
	tbl := schema.NewTable(table)
	cols := make([]string, 0)
	for _, v := range ts.Attributes {
		if v.FieldName == "" {
			if v.MappingStrategy == "ChildRelation" {
				continue // Ignore these
			}
			return nil, fmt.Errorf("field name missing from schema definition")
		}
		cols = append(cols, v.FieldName)
		f := schema.NewField(v.FieldName, shared.ValueTypeFromString(v.Type),
			0, v.Required, v.DefaultValue, v.ForeignKey, "-", v.Desc)
		f.Extra = v.MappingStrategy
		if v.ForeignKey != "" {
			f.Key = fmt.Sprintf("FK: %s", v.ForeignKey)
		} else {
			if f.Name == pka[0].FieldName && len(pka) > 1 {
				f.Key = "PK*"
			} else if _, found := pkMap[f.Name]; found {
				f.Key = "PK"
			} else {
				f.Key = "-"
			}
		}
		if v.Desc != "" {
			f.Description = v.Desc
		} else {
			f.Description = "-"
		}
		if v.SourceName != "" {
			f.Collation = v.SourceName
		}
		tbl.AddField(f)
	}
	tbl.SetColumns(cols)
	rows := make([][]driver.Value, 0)
	for _, v := range tbl.Fields {
		rows = append(rows, m.AsRow(v))
	}
	tbl.SetRows(rows)
	m.ReturnConnection(table, conn)
	return tbl, nil
}

// AsRow - Return values as a row.
func (m *QuantaSource) AsRow(f *schema.Field) []driver.Value {
	row := make([]driver.Value, len(schema.DescribeFullCols))
	//NewField(name string, valType value.ValueType, size int, allowNulls bool, defaultVal driver.Value, key, collation, description string) *Field {
	// []string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
	row[0] = f.Name
	row[1] = value.ValueType(f.Type).String() // should we send this through a dialect-writer?  bc dialect specific?
	row[2] = f.Collation
	row[3] = f.NoNulls
	row[4] = f.Key
	row[5] = f.DefVal
	row[6] = f.Extra
	row[7] = "-"
	row[8] = f.Description // should we put native type in here?
	return row
}

// Close this source
func (m *QuantaSource) Close() error {

	m.connPoolLock.Lock()
	for _, v := range m.connPoolMap {
		close(v.pool)
		for x := range v.pool {
			x.CloseConnection()
		}
	}
	m.connPoolLock.Unlock()
	defer func() { recover() }()
	close(m.exit)
	return nil
}

// Tables list
func (m *QuantaSource) Tables() []string {
	return m.ListTableNames()
}

//func (m *QuantaSource) SetColumns(cols []string)                  { m.tbl.SetColumns(cols) }

// Columns - Return column name strings.
func (m *QuantaSource) Columns() []string {
	//return m.tbl.Columns()
	log.Println("QuantaSource: Columns() called!")
	return nil
}

// Next values.
func (m *QuantaSource) Next() schema.Message {
	log.Println("QuantaSource: Next() called!")
	return nil
}

// ListTableNames - Return table name strings.
func (m *QuantaSource) ListTableNames() []string {

	if m.baseDir == "" {
		lock, errx := shared.Lock(m.consulClient, "admin-tool", "admin-tool")
		if errx != nil {
			log.Printf("listTableNames: cannot obtain lock %v", errx)
			return []string{}
		}
		defer shared.Unlock(m.consulClient, lock)
		tables, errx := shared.GetTables(m.consulClient)
		if errx != nil {
			log.Printf("shared.getTables failed: %v", errx)
			return []string{}
		}
		return tables
	}

	list := make([]string, 0)
	files, err := ioutil.ReadDir(m.baseDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		if f.IsDir() {
			list = append(list, f.Name())
		}
	}
	return list
}
