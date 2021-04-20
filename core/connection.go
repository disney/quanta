package core

import (
	"fmt"
	"github.com/araddon/dateparse"
	"github.com/hashicorp/consul/api"
	"github.com/json-iterator/go"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/disney/quanta/client"
	"github.com/disney/quanta/shared"
	"log"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var (
	leadingInt = regexp.MustCompile(`^[-+]?\d+`)
)

const (
	reservationSize = 1000
	ifDelim          = "."
	primaryKey       = "P"
	secondaryKey     = "S"
)

// Connection - State for session (non-threadsafe)
type Connection struct {
	BasePath     string // path to schema directory
	MetadataPath string // path to metadata directory
	Client       *quanta.BitmapIndex
	StringIndex  *quanta.StringSearch
	KVStore      *quanta.KVStore
	TableBuffers map[string]*TableBuffer
	Nested       bool
	DateFilter   *time.Time // optional filter to only include records matching timestamp
	BytesRead    int        // Bytes read for a row (record)
	stateLock    sync.Mutex
}

// TableBuffer - State info for table.
type TableBuffer struct {
	Table            *Table // table schema
	sequencer        *shared.Sequencer
	CurrentColumnID  uint64
	CurrentTimestamp time.Time // Time quantum value
	CurrentPKValue   []interface{}
	PKMap            map[string]*Attribute
	PKAttributes     []*Attribute
	SKMap            map[string][]*Attribute
	rowCache         map[string]interface{} // row value cache ensures parquet data is only read once
}

// NewTableBuffer - Construct a TableBuffer
func NewTableBuffer(table *Table) (*TableBuffer, error) {
	tb := &TableBuffer{Table: table}
	tb.PKMap = make(map[string]*Attribute)
	tb.PKAttributes = make([]*Attribute, 0)
	if table.PrimaryKey != "" {
		pka, err := table.GetPrimaryKeyInfo()
		if err != nil {
			return nil, err
		}
		tb.PKAttributes = pka
		for _, v := range pka {
			tb.PKMap[v.FieldName] = v
		}
	}
	tb.rowCache = make(map[string]interface{})
	var err error
	if table.SecondaryKeys != "" {
		tb.SKMap, err = table.GetAlternateKeyInfo()
	}
	return tb, err
}

//
// OpenConnection - Creates a connected session to the underlying core.
// (This is intentionally not thread-safe for maximum throughput.)
//
func OpenConnection(path, metadataPath, name string, nested bool, bufSize uint, port int,
	consul *api.Client) (*Connection, error) {

	if name == "" {
		return nil, fmt.Errorf("table name is nil")
	}
	tableBuffers := make(map[string]*TableBuffer, 0)
	tab, err := LoadSchema(path, metadataPath, name, consul)
	if err != nil {
		return nil, err
	} else if nested {
		if err = recurseAndLoadSchema(path, metadataPath, tableBuffers, tab); err != nil {
			return nil, fmt.Errorf("Error loading child tables %v", err)
		}
	} else {
		// Do scan to see if there are parent relations.   If so, open the parent too.
		for _, v := range tab.Attributes {
			if v.MappingStrategy == "ParentRelation" && v.ForeignKey != "" {
				fkTable, _, _ := v.GetFKSpec()
				parent, err2 := LoadSchema(path, metadataPath, fkTable, consul)
                if err != nil {
					return nil, fmt.Errorf("Error loading parent schema - %v", err2)
				}
				if tb, err := NewTableBuffer(parent); err == nil {
					tableBuffers[fkTable] = tb
				} else {
					return nil, fmt.Errorf("OpenConnection error - %v", err)
				}
			}
		}
	}

	if tb, err := NewTableBuffer(tab); err == nil {
		tableBuffers[name] = tb
	} else {
		return nil, fmt.Errorf("OpenConnection error - %v", err)
	}
	s := &Connection{BasePath: path, MetadataPath: metadataPath, TableBuffers: tableBuffers, Nested: nested}

	conn := quanta.NewDefaultConnection()
	conn.ServicePort = port
	conn.Quorum = 3
	if err := conn.Connect(); err != nil {
		log.Fatal(err)
	}

	s.StringIndex = quanta.NewStringSearch(conn, 1000)
	s.KVStore = quanta.NewKVStore(conn)
	s.Client = quanta.NewBitmapIndex(conn, 3000000)
	s.Client.KVStore = s.KVStore
	return s, nil
}

// SetDateFilter - Filter by date
func (s *Connection) SetDateFilter(filter *time.Time) {
	s.DateFilter = filter
}

func recurseAndLoadSchema(basePath, metadataPath string, tableBuffers map[string]*TableBuffer, curTable *Table) error {

	for _, v := range curTable.Attributes {
		_, ok := tableBuffers[v.ChildTable]
		if v.ChildTable != "" && !ok {
			table, err := LoadSchema(basePath, metadataPath, v.ChildTable, curTable.ConsulClient)
			if err != nil {
				return err
			}
			err = recurseAndLoadSchema(basePath, metadataPath, tableBuffers, table)
			if err != nil {
				return fmt.Errorf("while loading %s, %v", table.Name, err)
			}
			if tb, err := NewTableBuffer(table); err == nil {
				tableBuffers[v.ChildTable] = tb
			} else {
				return fmt.Errorf("recurseAndLoadSchema error - %v", err)
			}
		}
	}
	return nil
}

// IsDriverForTables - Is this the driver table?
func (s *Connection) IsDriverForTables(tables []string) bool {

	for _, v := range tables {
		if _, ok := s.TableBuffers[v]; !ok {
			return false
		}
	}
	return true
}

// PutRow - Entry point.  Load a row of data from parquet file.
func (s *Connection) PutRow(name string, row *reader.ParquetReader) error {

	s.ResetRowCache()
	pqTablePath := fmt.Sprintf("%s.%s", row.SchemaHandler.GetRootExName(), name)
	return s.recursivePutRow(name, row, pqTablePath, false)
}

func (s *Connection) recursivePutRow(name string, row *reader.ParquetReader, pqTablePath string,
	isChild bool) error {

	tbuf, ok := s.TableBuffers[name]
	if !ok {
		return fmt.Errorf("Table %s invalid or not opened. (recursivePutRow) %s", name, pqTablePath)
	}
	recurse := len(s.TableBuffers) > 1
	curTable := tbuf.Table

	if curTable.PrimaryKey != "" {
		// Here we force the primary key to be handled first for table so that columnID is established in tbuf
		if hasValues, err := s.processPrimaryKey(tbuf, row, pqTablePath, isChild); err != nil {
			return err
		} else if !hasValues {
			return nil // nothing to do, no values in child relation
		}
	}

	if curTable.SecondaryKeys != "" {
		if err := s.processAlternateKeys(tbuf, row, pqTablePath, isChild); err != nil {
			return err
		}
	}

	for _, v := range curTable.Attributes {
		if curTable.PrimaryKey != "" {
			if _, found := tbuf.PKMap[v.FieldName]; found {
				continue // Already handled at this point
			}
		}
		// Construct parquet column path
		if recurse && v.MappingStrategy == "ChildRelation" && v.ChildTable != "" {
			// Should we verify that it is a parquet repetition type if child relation?
			pqChildPath := fmt.Sprintf("%s.%s", pqTablePath, v.SourceName)
			if strings.HasPrefix(v.SourceName, "/") {
				pqChildPath = fmt.Sprintf("%s.%s", row.SchemaHandler.GetRootExName(), v.SourceName[1:])
			} else if strings.HasPrefix(v.SourceName, "^") {
				pqChildPath = fmt.Sprintf("%s.%s.list.element.%s", row.SchemaHandler.GetRootExName(), v.Parent.Name,
					v.SourceName[1:])
			}
			if err := s.recursivePutRow(v.ChildTable, row, pqChildPath, true); err != nil {
				return err
			}
		} else if v.MappingStrategy == "ParentRelation" && v.ForeignKey != "" {
			// Foreign key processing
			fkTable, fkFieldSpec, _ := v.GetFKSpec()
			relBuf, ok := s.TableBuffers[fkTable]
			if !ok {
				return fmt.Errorf("Could not locate parent table buffer for [%s]", fkTable)
			}
			var relColumnID uint64
			okToMap := true
			if !s.Nested {
				if v.SourceName == "" {
					return fmt.Errorf("Not a nested import, source must be specified for %s", v.FieldName)
				}

				lookupKey, err := s.resolveFKLookupKey(&v, tbuf, row)
				if err != nil {
					return fmt.Errorf("resolveFKLookupKey %v", err)
				}
				// Not a nested import structure, must lookup the columnID of the relation
				// TODO: Very expensive, implement lookup cache
				colID, found, err := s.lookupColumnID(relBuf, lookupKey, fkFieldSpec)
				if err != nil {
					return fmt.Errorf("lookupColumnID %s,  %v", lookupKey, err)
				}
				relColumnID = colID
				okToMap = found
				// At the moment, if the FK lookup fails the value is not mapped.
				// TODO: Make this enforced by default and provide configurability
			} else {
				relColumnID = relBuf.CurrentColumnID
				// TODO: Verify this with nested structure
			}
			if okToMap {
				// Store the parent table ColumnID in the IntBSI for join queries
				if _, err := v.MapValue(relColumnID, s); err != nil {
					return fmt.Errorf("Error Mapping FK [%s].[%s] - %v", v.Parent.Name, v.FieldName, err)
				}
			}
		} else {
			vals, pqps, err := s.readParquetColumn(row, pqTablePath, &v, isChild)
			if err != nil {
				return fmt.Errorf("Parquet reader error - %v", err)
			}
			for _, cval := range vals {
				if cval != nil {
					// Map and index the value
					if _, err := v.MapValue(cval, s); err != nil {
						return fmt.Errorf("%s - %v", pqps[0], err)
					}
				}
			}
		}
	}
	return nil
}

// This function ensures that each parquet column is read once and only once for each row
func (s *Connection) readParquetColumn(row *reader.ParquetReader, pqTablePath string, v *Attribute,
	isChild bool) ([]interface{}, []string, error) {

	if v.SourceName == "" {
		return nil, nil, fmt.Errorf("readParquetColumn: attribute sourceName is empty for %s", v.FieldName)
	}
	// Compound foreighn keys are comprised of multiple source references separated by +
	sources := strings.Split(v.SourceName, "+")
	pqColPaths := make([]string, len(sources))
	retVals := make([]interface{}, len(sources))
	for i, source := range sources {
		pqColPath := fmt.Sprintf("%s.list.element.%s", pqTablePath, source)
		if !isChild {
			pqColPath = fmt.Sprintf("%s.%s", pqTablePath, source)
		}
		if strings.HasPrefix(source, "/") {
			pqColPath = fmt.Sprintf("%s.%s", row.SchemaHandler.GetRootExName(), source[1:])
		} else if strings.HasPrefix(source, "^") {
			pqColPath = fmt.Sprintf("%s.%s.list.element.%s", row.SchemaHandler.GetRootExName(), v.Parent.Name,
				source[1:])
		}
		pqColPaths[i] = pqColPath
		// Check cache first
		tbuf, ok := s.TableBuffers[v.Parent.Name]
		if !ok {
			return nil, nil, fmt.Errorf("readParquetColumn: table not open for %s", v.Parent.Name)
		}
		val, found := tbuf.rowCache[pqColPath]
		if found {
			retVals[i] = val
			continue
		}
		vals, _, _, err := row.ReadColumnByPath(pqColPath, 1)
        if err != nil {
			return nil, nil, fmt.Errorf("Parquet reader error for %s [%v]", pqColPath, err)
		}
		s.BytesRead += int(unsafe.Sizeof(vals))
		if len(vals) == 0 || (len(vals) == 1 && vals[0] == nil) {
			if !v.Required {
				return nil, nil, nil
			}
			return nil, nil, fmt.Errorf("field %s - %s is required", v.FieldName, pqColPath)
		}
		if v.Required && (v.Type == "String" || v.Type == "Date" || v.Type == "DateTime") {
			if str, ok := vals[0].(string); ok {
				if str == "" {
					return nil, nil, fmt.Errorf("for field [%s], source [%s] is required", v.FieldName, pqColPath)
				}
			}
		}
		retVals[i] = vals[0]
		tbuf.rowCache[pqColPath] = vals[0]
	}
	return retVals, pqColPaths, nil
}

//
// Complete handling of primary key.
//    1. Uniqueness check against value in KVStore
//    2. ColumnID establishment for all fields in this row.
//    3. Value mapping.
//
// returns true if there are values to process.
//
func (s *Connection) processPrimaryKey(tbuf *TableBuffer, row *reader.ParquetReader, pqTablePath string,
	isChild bool) (bool, error) {

	if tbuf.Table.TimeQuantumType == "" {
		tbuf.CurrentTimestamp = time.Unix(0, 0)
	}

	tbuf.CurrentPKValue = make([]interface{}, len(tbuf.PKAttributes))
	pqColPaths := make([]string, len(tbuf.PKAttributes))
	var pkLookupVal strings.Builder
	for i, pk := range tbuf.PKAttributes {
		var cval interface{}
		vals, pqps, err := s.readParquetColumn(row, pqTablePath, pk, isChild)
		if err != nil {
			return false, fmt.Errorf("readParquetColumn for PK - %v", err)
		}
		pqColPaths[i] = pqps[0]
		if vals == nil || len(vals) == 0 || (len(vals) == 1 && vals[0] == nil) {
			if isChild { // Nothing to do here, no child value
				return false, nil
			}
			return false, fmt.Errorf("empty or nil value for PK field %s, len %d", pqColPaths[i],
				len(vals))
		}
		if len(vals) > 1 {
			return false, fmt.Errorf("multiple values for PK field %s [%v], Schema mapping issue?",
				pqColPaths[0], err)
		}
		cval = vals[0]
		tbuf.CurrentPKValue[i] = cval

		switch reflect.ValueOf(cval).Kind() {
		case reflect.String:
			// Do nothing already a string
			if i == 0 {
				if pk.MappingStrategy == "SysMillisBSI" || pk.MappingStrategy == "SysMicroBSI" {
					strVal := cval.(string)
					loc, _ := time.LoadLocation("Local")
					ts, err := dateparse.ParseIn(strVal, loc)
					if err != nil {
						return false, fmt.Errorf("Date parse error for PK field %s - value %s - %v",
							pqColPaths[i], strVal, err)
					}
					tFormat := shared.YMDTimeFmt
					if tbuf.Table.TimeQuantumType == "YMDH" {
						tFormat = shared.YMDHTimeFmt
					}
					sf := ts.Format(tFormat)
					tq, _ := time.Parse(tFormat, sf)
					if s.DateFilter != nil && *s.DateFilter != tq {
						// Fitler is set and dates don't match so continue on.
						return false, nil
					}
					if tbuf.CurrentTimestamp.UnixNano() > 0 && tbuf.CurrentTimestamp != tq {
						/*
						 * if the time partition value changes, then must get a new sequencer.
						 * Doing this actually sucks because it will leave large gaps in sequence numbers.
						 * Ideally we would load one time range at a time.  Better than a bug though.
						 */
						tbuf.sequencer = nil
					}
					tbuf.CurrentTimestamp = tq // Establish time quantum for record
				}
			}
		case reflect.Int64:
			orig := cval.(int64)
			cval = fmt.Sprintf("%d", orig)

			if i == 0 {
				tFormat := shared.YMDTimeFmt
				if tbuf.Table.TimeQuantumType == "YMDH" {
					tFormat = shared.YMDHTimeFmt
				}
				if pk.MappingStrategy == "SysMillisBSI" || pk.MappingStrategy == "SysMicroBSI" {
					ts := time.Unix(0, orig*1000000)
					if pk.MappingStrategy == "SysMicroBSI" {
						ts = time.Unix(0, orig*1000)
					}
					sf := ts.Format(tFormat)
					tq, _ := time.Parse(tFormat, sf)
					if s.DateFilter != nil && *s.DateFilter != tq {
						// Fitler is set and dates don't match so continue on.
						return false, nil
					}
					if tbuf.CurrentTimestamp.UnixNano() > 0 && tbuf.CurrentTimestamp != tq {
						// See above comment.
						tbuf.sequencer = nil
					}
					tbuf.CurrentTimestamp = tq // Establish time quantum for record
				}
			}
		case reflect.Float64:
			orig := cval.(float64)
			f := fmt.Sprintf("%%10.%df", pk.Scale)
			cval = fmt.Sprintf(f, orig)
		default:
			return false, fmt.Errorf("PK Lookup value [%v] unknown type, it is [%v]", cval,
				reflect.ValueOf(cval).Kind())
		}
		if pkLookupVal.Len() == 0 {
			pkLookupVal.WriteString(cval.(string))
		} else {
			pkLookupVal.WriteString(fmt.Sprintf("+%s", cval.(string)))
		}
	}

	// Can't use batch operation here unfortunately, but at least we have local batch cache
	if lColID, ok := s.Client.LookupLocalPKString(tbuf.Table.Name, tbuf.Table.PrimaryKey, pkLookupVal.String()); !ok {
		var colID uint64
		var errx error
		var found bool
		if !tbuf.Table.DisableDedup {
			colID, found, errx = s.lookupColumnID(tbuf, pkLookupVal.String(), "")
			if errx != nil {
				return false, fmt.Errorf("Dedup lookup error - %v", errx)
			}
		}
		if found {
			tbuf.CurrentColumnID = colID
		} else {
			// Generate new ColumnID
			if tbuf.sequencer == nil || tbuf.sequencer.IsFullySubscribed() {
				seq, err := s.Client.CheckoutSequence(tbuf.Table.Name, tbuf.PKAttributes[0].FieldName,
					tbuf.CurrentTimestamp, reservationSize)
				if err != nil {
					return false, fmt.Errorf("Sequencer checkout error for %s.%s - %v]", tbuf.Table.Name,
						tbuf.PKAttributes[0].FieldName, err)
				}
				tbuf.sequencer = seq
			}
			tbuf.CurrentColumnID, _ = tbuf.sequencer.Next()
			// Add the PK via local cache batch operation
			s.Client.SetKeyString(tbuf.Table.Name, tbuf.Table.PrimaryKey, primaryKey, pkLookupVal.String(),
				tbuf.CurrentColumnID)
		}
	} else {
		if tbuf.Table.DisableDedup {
			log.Printf("WARN: PK %s found in cache but dedup is disabled.  PK mapping error?", pkLookupVal.String())
		}
		tbuf.CurrentColumnID = lColID
	}

	// Map the value(s) and update table
	//log.Printf("PK = %s [%v]", pk.FieldName, cval)
	for i, v := range tbuf.CurrentPKValue {
		if _, err := tbuf.PKAttributes[i].MapValue(v, s); err != nil {
			return false, fmt.Errorf("PK mapping error %s - %v", pqColPaths[i], err)
		}
	}

	return true, nil
}

// Handle Secondary Keys.  Create the index in backing store
func (s *Connection) processAlternateKeys(tbuf *TableBuffer, row *reader.ParquetReader, pqTablePath string,
	isChild bool) error {

	pqColPaths := make([]string, len(tbuf.SKMap))
	var skLookupVal strings.Builder
	i := 0
	for k, keyAttrs := range tbuf.SKMap {
		for _, v := range keyAttrs {
			var cval interface{}
			vals, pqps, err := s.readParquetColumn(row, pqTablePath, v, isChild)
			if err != nil {
				return fmt.Errorf("readParquetColumn for SK - %v", err)
			}
			pqColPaths[i] = pqps[0]
			if vals == nil || len(vals) == 0 || (len(vals) == 1 && vals[0] == nil) {

				if isChild { // Nothing to do here, no child value
					return nil
				}
				return fmt.Errorf("Empty or nil value for SK field %s, len %d", pqColPaths[i],
					len(vals))
			}
			if len(vals) > 1 {
				return fmt.Errorf("Multiple values for SK field %s [%v], Schema mapping issue?",
					pqColPaths[0], err)
			}
			cval = vals[0]

			switch reflect.ValueOf(cval).Kind() {
			case reflect.String:
				// Do nothing already a string
				if v.MappingStrategy == "SysMillisBSI" || v.MappingStrategy == "SysMicroBSI" {
					strVal := cval.(string)
					loc, _ := time.LoadLocation("Local")
					ts, err := dateparse.ParseIn(strVal, loc)
					if err != nil {
						return fmt.Errorf("Date parse error for SK field %s - value %s - %v",
							pqColPaths[i], strVal, err)
					}
					cval = fmt.Sprintf("%d", ts.UnixNano())
				}
			case reflect.Int64:
				orig := cval.(int64)
				cval = fmt.Sprintf("%d", orig)

			default:
				return fmt.Errorf("SK Lookup value [%v] unknown type, it is [%v]", cval,
					reflect.ValueOf(cval).Kind())
			}
			if skLookupVal.Len() == 0 {
				skLookupVal.WriteString(cval.(string))
			} else {
				skLookupVal.WriteString(fmt.Sprintf("+%s", cval.(string)))
			}
		}
		s.Client.SetKeyString(tbuf.Table.Name, k, secondaryKey, skLookupVal.String(),
			tbuf.CurrentColumnID)
		i++
	}
	return nil
}

func (s *Connection) lookupColumnID(tbuf *TableBuffer, lookupVal, fkFieldSpec string) (uint64, bool, error) {

	kvIndex := fmt.Sprintf("%s%s%s.PK", tbuf.Table.Name, ifDelim, tbuf.Table.PrimaryKey)
	if fkFieldSpec != "" {
		// Use the secondary/alternate key specification
		kvIndex = fmt.Sprintf("%s%s%s.SK", tbuf.Table.Name, ifDelim, fkFieldSpec)
	}
	kvResult, err := s.KVStore.Lookup(kvIndex, lookupVal, reflect.Uint64)
	if err != nil {
		return 0, false, fmt.Errorf("KVStore error for [%s] = [%s], [%v]", kvIndex, lookupVal, err)
	}
	if kvResult == nil {
		return 0, false, nil
	}
	return kvResult.(uint64), true, nil
}

// LookupKeyBatch - Process a batch of keys.
func (s *Connection) LookupKeyBatch(tbuf *TableBuffer, lookupVals map[interface{}]interface{},
	fkFieldSpec string) (map[interface{}]interface{}, error) {

	kvIndex := fmt.Sprintf("%s%s%s.PK", tbuf.Table.Name, ifDelim, tbuf.Table.PrimaryKey)
	if fkFieldSpec != "" {
		// Use the secondary/alternate key specification
		kvIndex = fmt.Sprintf("%s%s%s.SK", tbuf.Table.Name, ifDelim, fkFieldSpec)
	}
	lookupVals, err := s.KVStore.BatchLookup(kvIndex, lookupVals)
	if err != nil {
		return nil, fmt.Errorf("KVStore.LookupBatch error for [%s] - [%v]", kvIndex, err)
	}
	return lookupVals, nil
}

func (s *Connection) resolveFKLookupKey(v *Attribute, tbuf *TableBuffer,
	row *reader.ParquetReader) (string, error) {

	var retVal strings.Builder
	pqTablePath := fmt.Sprintf("%s.%s", row.SchemaHandler.GetRootExName(), tbuf.Table.Name)
	vals, _, err := s.readParquetColumn(row, pqTablePath, v, false)
	if err != nil {
		return "", err
	}
	for _, val := range vals {
		if val != nil {
			if retVal.Len() == 0 {
				retVal.WriteString(fmt.Sprintf("%v", val))
			} else {
				retVal.WriteString(fmt.Sprintf("+%v", val))
			}
		}
	}
	return retVal.String(), nil
}

// ResetRowCache - Clear cache.
func (s *Connection) ResetRowCache() {
	for _, v := range s.TableBuffers {
		v.rowCache = make(map[string]interface{})
	}
	s.BytesRead = 0
}

// Flush - Flush data to backend.
func (s *Connection) Flush() {

	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	if s.StringIndex != nil {
		if err := s.StringIndex.Flush(); err != nil {
			log.Println(err)
		}
	}
	if s.Client != nil {
		if err := s.Client.Flush(); err != nil {
			log.Println(err)
		}
	}
}

// CloseConnection - Close the session, flushing if necessary..
func (s *Connection) CloseConnection() {

	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	if s.StringIndex != nil {

		if err := s.StringIndex.Flush(); err != nil {
			log.Println(err)
		}
		s.StringIndex = nil
	}

	if s.Client != nil {

		if err := s.Client.Flush(); err != nil {
			log.Println(err)
		}
		if err := s.Client.Disconnect(); err != nil {
			log.Println(err)
		}
		s.Client = nil
	}
}

// MapValue - Convenience function for Mapper interface.
func (s *Connection) MapValue(tableName, fieldName string, value interface{}, update bool) (val uint64, err error) {

	tbuf, ok := s.TableBuffers[tableName]
	if !ok {
		return 0, fmt.Errorf("Table %s invalid or not opened. (MapValue)", tableName)
	}
	attr, err := tbuf.Table.GetAttribute(fieldName)
	if err != nil {
		return 0, fmt.Errorf("attribute '%s' not found", fieldName)
	}

	/*
		if attr.SkipIndex {
			if update {
				return 0, nil
			} else {
				return 0, fmt.Errorf("attribute '%s' is not indexed and can't be used in a query", fieldName)
			}
		}
	*/
	if update {
		return attr.MapValue(value, s)
	}
	return attr.MapValue(value, nil) // Non load use case pass nil connection context
}

// PutRowKafka - Kafka specific version of PutRow.
func (s *Connection) PutRowKafka(name string, data []byte) error {

	return s.recursivePutRowKafka(name, data, "", false)
}

func (s *Connection) recursivePutRowKafka(name string, line []byte, jsTablePath string, isChild bool) error {

	tbuf, ok := s.TableBuffers[name]
	if !ok {
		return fmt.Errorf("Table %s invalid or not opened. (recursivePutRowKafka) %s", name, jsTablePath)
	}
	recurse := len(s.TableBuffers) > 1
	curTable := tbuf.Table

	if curTable.PrimaryKey != "" {
		// Here we force the primary key to be handled first for table so that columnID is established in tbuf
		if hasValues, err := s.processPrimaryKeyKafka(tbuf, line, jsTablePath, isChild); err != nil {
			return err
		} else if !hasValues {
			return nil // nothing to do, no values in child relation
		}
	}

	for _, v := range curTable.Attributes {
		if curTable.PrimaryKey != "" && v.FieldName == curTable.PrimaryKey {
			continue // Already handled at this point
		}
		if v.MappingStrategy == "ParentRelation" && v.ForeignKey != "" {
			// Foreign key processing
			parBuf, ok := s.TableBuffers[v.ForeignKey]
			if !ok {
				return fmt.Errorf("Could not locate parent table buffer for [%s]", v.ForeignKey)
			}
			if parBuf.CurrentPKValue == nil {
				return fmt.Errorf("Parent PK value is nil while processing FK [%s]", v.ForeignKey)
			}
			// Store the parent table ColumnID in the IntBSI for join queries
			if _, err := v.MapValue(parBuf.CurrentColumnID, s); err != nil {
				return fmt.Errorf("Error Mapping FK [%s].[%s] - %v", v.Parent.Name, v.FieldName, err)
			}
			continue
		}
		// Construct javascript column path
		if recurse && v.MappingStrategy == "ChildRelation" && v.ChildTable != "" {
			jsChildPath := fmt.Sprintf("%s.%s.0", jsTablePath, v.SourceName)
			// Should we verify that it is a parquet repetition type if child relation?
			if jsTablePath == "" {
				jsChildPath = fmt.Sprintf("%s.0", v.SourceName)
			}

			if strings.HasPrefix(v.SourceName, "/") {
				jsChildPath = v.SourceName[1:]
			} else if strings.HasPrefix(v.SourceName, "^") {
				jsChildPath = fmt.Sprintf("%s.0.%s", v.Parent.Name, v.SourceName[1:])
			}
			if err := s.recursivePutRowKafka(v.ChildTable, line, jsChildPath, true); err != nil {
				return err
			}
		} else {
			jsColPath := resolveJSColumnPathForField(jsTablePath, &v, isChild)
			vals := readColumnByPath(jsColPath, line)
			for _, cval := range vals {
				if cval != nil {
					// Map and index the value
					if _, err := v.MapValue(cval, s); err != nil {
						return fmt.Errorf("%s - %v", jsColPath, err)
					}
				}
			}
		}
	}
	return nil
}

func (s *Connection) processPrimaryKeyKafka(tbuf *TableBuffer, line []byte, jsTablePath string,
	isChild bool) (bool, error) {

	pk, _ := tbuf.Table.GetAttribute(tbuf.Table.PrimaryKey)
	// TODO:  Add robust checking for time quantum usage rules, etc.
	if tbuf.Table.TimeQuantumType != "" && (pk.Type != "Date" && pk.Type != "DateTime") {
		return false, fmt.Errorf("time partitions enabled for PK %s, Type must be Date or DateTime", pk.FieldName)
	}
	if tbuf.Table.TimeQuantumType == "" {
		tbuf.CurrentTimestamp = time.Unix(0, 0)
	}
	jsColPath := resolveJSColumnPathForField(jsTablePath, pk, isChild)
	var cval interface{}
	vals := readColumnByPath(jsColPath, line)
	if len(vals) == 0 || (len(vals) == 1 && vals[0] == nil) {
		if isChild { // Nothing to do here, no child value
			return false, nil
		}
		return false, fmt.Errorf("Empty or nil value for PK field %s", jsColPath)
	}
	if len(vals) > 1 {
		return false, fmt.Errorf("Multiple values for PK field %s, Schema mapping issue?", jsColPath)
	}
	cval = vals[0]
	tbuf.CurrentPKValue = []interface{}{cval}

	// Check KVStore for value lookup string to columnID (.PK extension)
	kvIndex := fmt.Sprintf("%s%s%s.PK", tbuf.Table.Name, ifDelim, pk.FieldName)
	switch reflect.ValueOf(cval).Kind() {
	case reflect.String:
		// Do nothing already a string
	case reflect.Float64:
		orig := int64(math.RoundToEven(cval.(float64)))
		cval = fmt.Sprintf("%d", orig)
		tFormat := shared.YMDTimeFmt
		if tbuf.Table.TimeQuantumType == "YMDH" {
			tFormat = shared.YMDHTimeFmt
		}
		if pk.MappingStrategy == "SysMillisBSI" || pk.MappingStrategy == "SysMicroBSI" {
			ts := time.Unix(0, orig*1000000)
			if pk.MappingStrategy == "SysMicroBSI" {
				ts = time.Unix(0, orig*1000)
			}
			s := ts.Format(tFormat)
			tq, _ := time.Parse(tFormat, s)
			tbuf.CurrentTimestamp = tq // Establish time quantum for record
		}
	default:
		return false, fmt.Errorf("PK Lookup value [%v] unknown type for [%s], it is [%v]", cval, kvIndex,
			reflect.ValueOf(cval).Kind())
	}
	// Can't use batch operation here unfortunately, but at least we have local batch cache
	if lColID, ok := s.Client.LookupLocalPKString(tbuf.Table.Name, pk.FieldName, cval); !ok {
		kvResult, err := s.KVStore.Lookup(kvIndex, cval, reflect.Uint64)
		if err != nil {
			return false, fmt.Errorf("KVStore error for [%s], %v", kvIndex, err)
		}
		if kvResult != nil {
			tbuf.CurrentColumnID = kvResult.(uint64)
			//log.Printf("Found existing PK (%s) for value [%v]", kvIndex, cval)
		} else {
			// Generate new ColumnID
			if tbuf.sequencer == nil || tbuf.sequencer.IsFullySubscribed() {
				seq, err := s.Client.CheckoutSequence(tbuf.Table.Name, tbuf.Table.PrimaryKey,
					tbuf.CurrentTimestamp, reservationSize)
				if err != nil {
					return false, fmt.Errorf("Sequencer checkout error for %s [%s], %v", tbuf.Table.Name,
						tbuf.Table.PrimaryKey, err)
				}
				tbuf.sequencer = seq
			}
			tbuf.CurrentColumnID, _ = tbuf.sequencer.Next()
			// Add the PK via local cache batch operation
			s.Client.SetKeyString(tbuf.Table.Name, pk.FieldName, "P", cval, tbuf.CurrentColumnID)
		}
	} else {
		tbuf.CurrentColumnID = lColID
	}

	// Map the value and update table
	//log.Printf("PK = %s [%v]", pk.FieldName, cval)
	if _, err := pk.MapValue(cval, s); err != nil {
		return false, fmt.Errorf("%s - %v", jsColPath, err)
	}

	return true, nil
}

func resolveJSColumnPathForField(jsTablePath string, v *Attribute, isChild bool) (jsColPath string) {

	// BEGIN CUSTOM CODE FOR VISION
	//if strings.HasSuffix(jsTablePath, "media.0") {
	if v.Parent.Name == "media" {
		jsColPath = fmt.Sprintf("events.0.event_tracktype_properties.%s", v.SourceName)
		return
	}
	if v.Parent.Name == "events" {
		s := strings.Split(v.SourceName, ".")
		if len(s) > 1 {
			switch s[0] {
			case "media", "pzncon", "ad", "prompt", "api":
				jsColPath = fmt.Sprintf("events.0.event_tracktype_properties.%s", s[1])
				return
			}
		}
	}
	// END CUSTOM CODE FOR VISION
	jsColPath = fmt.Sprintf("%s.%s", jsTablePath, v.SourceName)
	/*
	   if !isChild {
	       //jsColPath = fmt.Sprintf("%s.%s", jsTablePath, v.SourceName)
	       jsColPath = fmt.Sprintf("%s.%s", jsTablePath, v.SourceName)
	   }
	*/
	if strings.HasPrefix(v.SourceName, "/") {
		jsColPath = v.SourceName[1:]
	} else if strings.HasPrefix(v.SourceName, "^") {
		jsColPath = fmt.Sprintf("%s.%s", v.Parent.Name, v.SourceName[1:])
	}
	return
}

func readColumnByPath(path string, line []byte) []interface{} {

	s := strings.Split(path, ".")
	p := make([]interface{}, len(s))
	var returnArray bool
	for i, v := range s {
		if val, err := strconv.ParseInt(v, 10, 32); err == nil {
			p[i] = int(val)
			returnArray = (i == len(s)-1)
		} else {
			p[i] = v
		}
	}

	val := jsoniter.Get(line, p...)
	if returnArray {
		//return val.GetInterface()
		return []interface{}{val.GetInterface()}
	}
	return []interface{}{val.GetInterface()}
}
