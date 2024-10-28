package core

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/disney/quanta/qlbridge/datasource"
	"github.com/disney/quanta/qlbridge/expr"
	"github.com/disney/quanta/qlbridge/rel"
	"github.com/disney/quanta/qlbridge/value"
	"github.com/disney/quanta/qlbridge/vm"
	"github.com/disney/quanta/shared"
	"github.com/xitongsys/parquet-go/reader"
)

var (
	leadingInt = regexp.MustCompile(`^[-+]?\d+`)
)

const (
	reservationSize        = 1000
	ifDelim                = "/"
	primaryKey             = "P"
	secondaryKey           = "S"
	julianDayOfEpoch int64 = 2440588
	microsPerDay     int64 = 3600 * 24 * 1000 * 1000
	batchBufferSize		   = 90000000			// This is a stopgap to prevent overrunning memory
)

// Session - State for session (non-threadsafe)
type Session struct {
	BasePath     string // path to schema directory
	BitIndex     *shared.BitmapIndex
	BatchBuffer  *shared.BatchBuffer
	StringIndex  *shared.StringSearch
	KVStore      *shared.KVStore
	TableBuffers map[string]*TableBuffer
	Nested       bool
	DateFilter   *time.Time // optional filter to only include records matching timestamp
	BytesRead    int        // Bytes read for a row (record)
	CreatedAt    time.Time
	stateLock    sync.Mutex
	flushing     bool

	tableCache *TableCacheStruct
}

// TableBuffer - State info for table.
type TableBuffer struct {
	Table            *Table // table schema
	sequencerCache   map[int64]*shared.Sequencer
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
	tb.sequencerCache = make(map[int64]*shared.Sequencer)
	tb.PKMap = make(map[string]*Attribute)
	tb.PKAttributes = make([]*Attribute, 0)
	tb.CurrentTimestamp = time.Unix(0, 0)
	pka, errx := table.GetPrimaryKeyInfo()
	if errx != nil {
		return nil, errx
	}
	tb.PKAttributes = pka
	for _, v := range pka {
		tb.PKMap[v.FieldName] = v
	}
	tb.rowCache = make(map[string]interface{})
	var err error
	if table.SecondaryKeys != "" {
		tb.SKMap, err = table.GetAlternateKeyInfo()
	}
	return tb, err
}

// NextColumnID - Get a new column ID in the sequence for a given Time Quantum
func (t *TableBuffer) NextColumnID(bi *shared.BitmapIndex) error {

	sequencer, ok := t.sequencerCache[t.CurrentTimestamp.UnixNano()]
	if !ok || sequencer.IsFullySubscribed() {
		seq, err := bi.CheckoutSequence(t.Table.Name, t.PKAttributes[0].FieldName,
			t.CurrentTimestamp, reservationSize)
		if err != nil {
			t.CurrentColumnID = 0
			return fmt.Errorf("Sequencer checkout error for %s.%s - %v]", t.Table.Name,
				t.PKAttributes[0].FieldName, err)
		}
		sequencer = seq
		t.sequencerCache[t.CurrentTimestamp.UnixNano()] = sequencer
	}
	t.CurrentColumnID, _ = sequencer.Next()
	return nil
}

// ShouldLookupPrimaryKey - Does this table have a primary key
func (t *TableBuffer) ShouldLookupPrimaryKey() bool {

	if t.PKAttributes[0].ColumnID {
		return false
	}

	if t.Table.TimeQuantumType != "" && len(t.PKAttributes) > 1 {
        if t.PKAttributes[1].ColumnID {
			return false
		}
		return true
	}
	if t.Table.TimeQuantumType == "" && len(t.PKAttributes) > 0 {
		return true
	}
	return false
}

// OpenSession - Creates a connected session to the underlying core.
// (This is intentionally not thread-safe for maximum throughput.)
func OpenSession(tableCache *TableCacheStruct, path, name string, nested bool, conn *shared.Conn) (*Session, error) {

// FIXME - is the nested flag necessary?
	if name == "" {
		return nil, fmt.Errorf("table name is nil")
	}
	if tableCache == nil {
		return nil, fmt.Errorf("table cache is nil")
	}

	consul := conn.Consul
	kvStore := shared.NewKVStore(conn)

	tableBuffers := make(map[string]*TableBuffer, 0)
	tab, err := LoadTable(tableCache, path, kvStore, name, consul)
	if err != nil {
		return nil, err
	} else if nested {
		if err = recurseAndLoadTable(path, kvStore, tableBuffers, tab); err != nil {
			return nil, fmt.Errorf("Error loading child tables %v", err)
		}
	}
	// Do scan to see if there are parent relations.   If so, open the parent too.
	for _, v := range tab.Attributes {
		if v.MappingStrategy == "ParentRelation" && v.ForeignKey != "" {
			fkTable, _, _ := v.GetFKSpec()
			parent, err2 := LoadTable(tableCache, path, kvStore, fkTable, consul)
			if err != nil {
				return nil, fmt.Errorf("Error loading parent schema - %v", err2)
			}
			if tb, ok := tableBuffers[fkTable]; !ok {
				if tb, err = NewTableBuffer(parent); err == nil {
					tableBuffers[fkTable] = tb
				} else {
					return nil, fmt.Errorf("OpenSession error - %v", err)
				}
			}
		}
	}

	if tb, ok := tableBuffers[name]; !ok {
		if tb, err = NewTableBuffer(tab); err == nil {
			tableBuffers[name] = tb
		} else {
			return nil, fmt.Errorf("OpenSession error - %v", err)
		}
	}
	s := &Session{BasePath: path, TableBuffers: tableBuffers, Nested: nested}
	s.StringIndex = shared.NewStringSearch(conn, 1000)
	s.KVStore = kvStore
	s.BitIndex = shared.NewBitmapIndex(conn)
	s.BatchBuffer = shared.NewBatchBuffer(s.BitIndex, s.KVStore, batchBufferSize)
	s.CreatedAt = time.Now().UTC()
	s.tableCache = tableCache

	return s, nil
}

func recurseAndLoadTable(basePath string, kvStore *shared.KVStore, tableBuffers map[string]*TableBuffer, curTable *Table) error {
	tableCache := curTable.tableCache

	for _, v := range curTable.Attributes {
		_, ok := tableBuffers[v.ChildTable]
		if v.ChildTable != "" && !ok {
			table, err := LoadTable(tableCache, basePath, kvStore, v.ChildTable, curTable.ConsulClient)
			if err != nil {
				return err
			}
			err = recurseAndLoadTable(basePath, kvStore, tableBuffers, table)
			if err != nil {
				return fmt.Errorf("while loading %s, %v", table.Name, err)
			}
			if tb, err := NewTableBuffer(table); err == nil {
				tableBuffers[v.ChildTable] = tb
			} else {
				return fmt.Errorf("recurseAndLoadTable error - %v", err)
			}
		}
		if v.ForeignKey != ""  {
			fkTable, _, _ := v.GetFKSpec()
			_, ok = tableBuffers[v.ChildTable]
			if !ok {
				table, err := LoadTable(tableCache, basePath, kvStore, fkTable, curTable.ConsulClient)
				if err != nil {
					return err
				}
				if tb, err := NewTableBuffer(table); err == nil {
					tableBuffers[fkTable] = tb
				} else {
					return fmt.Errorf("recurseAndLoadTable error - %v", err)
				}
			}
		}
	}
	return nil
}

// IsDriverForTables - Is this the driver table?
func (s *Session) IsDriverForTables(tables []string) bool {

	for _, v := range tables {
		if _, ok := s.TableBuffers[v]; !ok {
			return false
		}
	}
	return true
}

// IsDriverForJoin - Is this the driver table?
func (s *Session) IsDriverForJoin(table, joinCol string) bool {

	tbuf, ok := s.TableBuffers[table]
	if !ok {
		return false
	}
	attr, err := tbuf.Table.GetAttribute(joinCol)
	if err != nil {
		return false
	}
	if attr.ForeignKey == "" {
		return false
	}

	return true
}

// CurrentColumnID - Returns the current column ID after call to PutRow
func (s *Session) CurrentColumnID(name string) (uint64, error) {
	tbuf, ok := s.TableBuffers[name]
	if !ok {
		return 0, fmt.Errorf("cannot locate buffer for table %s", name)
	}
	return tbuf.CurrentColumnID, nil
}

// PutRow - Entry point.  Load a row of data from source (Parquet/Kinesis/Kafka)
func (s *Session) PutRow(name string, row interface{}, providedColID uint64, ignoreSourcePath, useNerd bool) error {

	s.ResetRowCache()
	pqTablePath := "/"
	if r, ok := row.(*reader.ParquetReader); ok {
		pqTablePath = fmt.Sprintf("%s.%s", r.SchemaHandler.GetRootExName(), name)
	} else if r, ok := row.(map[string]interface{}); ok {
		if tbuf, ok2 := s.TableBuffers[name]; ok2 {
			tbuf.rowCache = r
		} else {
			return fmt.Errorf("cannot locate buffer for table %s", name)
		}
	} else {
		return fmt.Errorf("cannot process row type %T", row)
	}
	return s.recursivePutRow(name, row, pqTablePath, providedColID, false, ignoreSourcePath, useNerd)
}

func (s *Session) recursivePutRow(name string, row interface{}, pqTablePath string, providedColID uint64,
	isChild, ignoreSourcePath, useNerdCapitalization bool) error {

	tbuf, ok := s.TableBuffers[name]
	if !ok {
		return fmt.Errorf("table %s invalid or not opened. (recursivePutRow) %s", name, pqTablePath)
	}
	recurse := len(s.TableBuffers) > 1
	curTable := tbuf.Table

	// Here we force the primary key to be handled first for table so that columnID is established in tbuf
	isUpdate, err := s.processPrimaryKey(tbuf, row, pqTablePath, providedColID, isChild,
		ignoreSourcePath, useNerdCapitalization)
	if err != nil {
		return err
	}

	if curTable.SecondaryKeys != "" {
		if err := s.processAlternateKeys(tbuf, row, pqTablePath, isChild, ignoreSourcePath,
			useNerdCapitalization); err != nil {
			return err
		}
	}

	for _, v := range curTable.Attributes {
		if _, found := tbuf.PKMap[v.FieldName]; found {
			continue // Already handled at this point
		}
		// Construct parquet column path
		if recurse && v.MappingStrategy == "ChildRelation" && v.ChildTable != "" {
			// Should we verify that it is a parquet repetition type if child relation?
			if val, err := shared.GetPath(v.SourceName, tbuf.rowCache, false, false); err == nil {
				if vz, ok := val.([]interface{}); ok {
					childBuf, ok := s.TableBuffers[v.ChildTable]
					if !ok {
						return fmt.Errorf("child table %s invalid or not opened. (recursivePutRow) %s", 
							v.ChildTable, v.SourceName)
					}
					for _, z := range vz {
						// need to populate the rowcache for the child table
						childBuf.rowCache = row.(map[string]interface{})
						childBuf.rowCache[v.SourceName] = z
						if err := s.recursivePutRow(v.ChildTable, childBuf.rowCache, v.SourceName, 
								providedColID, true, ignoreSourcePath, useNerdCapitalization); err != nil {
							return err
						}
					}
				}
			} else {
				u.Errorf("recursion into child  = %s, %v, %#v", v.SourceName, err, tbuf.rowCache )
			}
			return nil
		} else if v.MappingStrategy == "ParentRelation" && v.ForeignKey != "" {
			// Foreign key processing
			fkTable, fkFieldSpec, _ := v.GetFKSpec()
			relBuf, ok := s.TableBuffers[fkTable]
			if !ok {
				return fmt.Errorf("Could not locate parent table buffer for [%s]", fkTable)
			}
			var relColumnID uint64
			okToMap := true
			//if !s.Nested {
			if !isChild {
				// Directly provided parent columnID
				if v.Type == "Integer" && (!relBuf.ShouldLookupPrimaryKey() || fkFieldSpec == "@rownum") {
					vals, _, err := s.readColumn(row, pqTablePath, &v, false, ignoreSourcePath, useNerdCapitalization)
					//vals, _, err := s.readColumn(row, pqTablePath, &v, false, true, false)
					if err != nil {
						return err
					}
					if len(vals) != 1 {
						return fmt.Errorf("Expected 1 value from direct parent id mapping.")
					}
					if vals[0] == nil {
						continue
					}
					switch reflect.ValueOf(vals[0]).Kind() {
					case reflect.String:
						if colId, err := strconv.ParseInt(vals[0].(string), 10, 64); err == nil {
							relColumnID = uint64(colId)
						} else {
							return fmt.Errorf("cannot parse string %v for parent relation %v type is %T",
								vals[0], v.FieldName, vals[0])
						}
					case reflect.Int64:
						relColumnID = uint64(vals[0].(int64))
					default:
						return fmt.Errorf("cannot cast %v to uint64 for parent relation %v type is %T",
							vals[0], v.FieldName, vals[0])
					}
				} else { // Lookup based
					//if v.SourceName == "" {
					//	return fmt.Errorf("Not a nested import, source must be specified for %s", v.FieldName)
					//}

					//lookupKey, err := s.resolveFKLookupKey(&v, tbuf, row, ignoreSourcePath, useNerdCapitalization)
					lookupKey, err := s.resolveFKLookupKey(&v, tbuf, row, true, false)
					if err != nil {
						return fmt.Errorf("resolveFKLookupKey %v", err)
					}
					// Not a nested import structure, must lookup the columnID of the relation
					// TODO: Very expensive, implement lookup cache
					colID, found, err := s.lookupColumnID(relBuf, lookupKey, fkFieldSpec)
					if err != nil {
						return fmt.Errorf("lookupColumnID %s,  %v", lookupKey, err)
					}
					if !found {
						return fmt.Errorf("cannot find value '%s' in parent table '%v' for column %s.%s",
							lookupKey, v.ForeignKey, v.Parent.Name, v.FieldName)
					}
					relColumnID = colID
					okToMap = found
					// At the moment, if the FK lookup fails the value is not mapped.
					// TODO: Make this enforced by default and provide configurability
				}
			} else {
				relColumnID = relBuf.CurrentColumnID
				// TODO: Verify this with nested structure
			}
			if okToMap {
				// Store the parent table ColumnID in the IntBSI for join queries
				if _, err := v.MapValue(relColumnID, s, false); err != nil {
					return fmt.Errorf("Error Mapping FK [%s].[%s] - %v", v.Parent.Name, v.FieldName, err)
				}
			}
		} else {
			vals, pqps, err := s.readColumn(row, pqTablePath, &v, isChild, ignoreSourcePath, 
					useNerdCapitalization)
			if err != nil {
				return fmt.Errorf("Parquet reader error - %v", err)
			}
			for _, cval := range vals {
				if cval != nil {
					// Map and index the value
					if _, err := v.MapValue(cval, s, isUpdate); err != nil {
						return fmt.Errorf("%s - %v", pqps[0], err)
					}
				}
			}
		}
	}
	return nil
}

// // This function ensures that each parquet column is read once and only once for each row
func (s *Session) readColumn(row interface{}, pqTablePath string, v *Attribute,
	isChild, ignoreSourcePath, useNerdCapitalization bool) ([]interface{}, []string, error) {

	// If we are ignoring source path and it is not defined then this must be a defaulted value
	//if !ignoreSourcePath && v.SourceName == "" {
	if v.DefaultValue != "" {
		//if v.DefaultValue != "" {
		retVals := make([]interface{}, 0)
		retVals = append(retVals, s.getDefaultValueForColumn(v, row, ignoreSourcePath, useNerdCapitalization))
		pqColPaths := []string{""}
		return retVals, pqColPaths, nil
		//}
		//return nil, nil, fmt.Errorf("readColumn: attribute sourceName is empty for %s", v.FieldName)
		return nil, []string{""}, nil
	}
	// Compound foreign keys are comprised of multiple source references separated by +
	sources := strings.Split(v.SourceName, "+")
	pqColPaths := make([]string, len(sources))
	retVals := make([]interface{}, len(sources))
	for i, source := range sources {
		root := "/"
		isParquet := false
		pqColPath := source
		if r, ok := row.(*reader.ParquetReader); ok {
			root = r.SchemaHandler.GetRootExName()
			isParquet = true
		}
		if isParquet {
			pqColPath = fmt.Sprintf("%s.list.element.%s", pqTablePath, source)
			if !isChild {
				pqColPath = fmt.Sprintf("%s.%s", pqTablePath, source)
				if useNerdCapitalization {
					pqColPath = fmt.Sprintf("%s.%s", pqTablePath, strings.Title(source))
				}
			}
			if !ignoreSourcePath {
				if strings.HasPrefix(source, "/") {
					pqColPath = fmt.Sprintf("%s.%s", root, source[1:])
					if useNerdCapitalization {
						pqColPath = fmt.Sprintf("%s.%s", strings.Title(root), strings.Title(source[1:]))
					}
				} else if strings.HasPrefix(source, "^") {
					pqColPath = fmt.Sprintf("%s.%s.list.element.%s", root, v.Parent.Name, source[1:])
				}
			} else {
				if useNerdCapitalization {
					pqColPath = fmt.Sprintf("%s.%s", strings.Title(root), strings.Title(v.FieldName))
				} else {
					pqColPath = fmt.Sprintf("%s.%s", root, v.FieldName)
				}
			}
		}
		pqColPaths[i] = pqColPath
		// Check cache first
		tbuf, ok := s.TableBuffers[v.Parent.Name]
		if !ok {
			return nil, nil, fmt.Errorf("readColumn: table not open for %s", v.Parent.Name)
		}
		val, found := tbuf.rowCache[pqColPath]
		if !found && !isParquet && isChild {
			val, found = tbuf.rowCache[pqTablePath]
			if found {
				val, found = val.(map[string]interface{})[v.SourceName]
			}
		}
		if !found && !isParquet {
			//val, found = tbuf.rowCache[source[1:]]
			src := v.FieldName
			if len(source) > 1 {
				src = source[1:]
			}
			if isChild {
				src = pqColPath
			}
			var err error
			found = true
			if val, err = shared.GetPath(src, tbuf.rowCache, ignoreSourcePath, useNerdCapitalization); err != nil {
				found = false
				if v.Required {
					u.Warnf("field %s, source %s = %v", v.FieldName, source, err)
				}
			}
		}
		if !isParquet {
			if (found && v.Required && val == nil) || (!found && v.Required) {
				return nil, nil, fmt.Errorf("field %s - %s is required", v.FieldName, source)
			}
			if aryVal, ok := val.([]interface{}); ok { // JSON array is StringEnum multi value
				s := make([]string, len(aryVal))
				for x, y := range aryVal {
					s[x] = fmt.Sprint(y)
					retVals[i] = s
				}
			} else {
				retVals[i] = val
			}
			continue
		} else {
			if found {
				retVals[i] = val
				continue
			}
		}
		if r, ok := row.(*reader.ParquetReader); ok {
			vals, _, _, err := r.ReadColumnByPath(pqColPath, 1)
			if err != nil {
				return nil, nil, fmt.Errorf("Parquet reader error for %s [%v]", pqColPath, err)
			}
			s.BytesRead += int(unsafe.Sizeof(vals))
			if v.DefaultValue != "" {
				if len(vals) == 0 {
					vals = append(vals, []string{""})
				}
				if str, ok := vals[0].(string); ok {
					if str == "" {
						vals[0] = fmt.Sprintf("%v", s.getDefaultValueForColumn(v, row, ignoreSourcePath,
							useNerdCapitalization))
					}
				}
			}
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
			if v.Type == "DateTime" {
				str, ok := vals[0].(string)
				if ok && len(str) == 12 { // Handle INT96
					ts := INT96ToTime(str)
					vals[0] = ts.Format(time.RFC3339)
				}
			}
			retVals[i] = vals[0]
			tbuf.rowCache[pqColPath] = vals[0]
		} else {
			return nil, nil,
				fmt.Errorf("for field [%s], source [%s] for non-parquet should have found cached data",
					v.FieldName, pqColPath)
		}
	}
	return retVals, pqColPaths, nil
}

// // Get the defalue value for a column (can be an expression)
func (s *Session) getDefaultValueForColumn(a *Attribute, row interface{}, ignoreSourcePath, useNerd bool) interface{} {

	// add ignoreSourcePath parameter

	var (
		val value.Value
		ok  bool
		r   interface{}
	)
	rm := make(map[string]interface{})

	// convert source paths to fieldname paths in incoming row
	if r, ok = row.(*reader.ParquetReader); ok {
		if r != nil {
			for _, v := range a.Parent.Attributes {
				if v.SourceName == "" {
					continue
				}
				var err error
				var val interface{}
				if val, err = shared.GetPath(v.SourceName, row, ignoreSourcePath, useNerd); err != nil {
					val = v.SourceName
				}
				rm[v.FieldName] = val
			}
		}
		var ctx *datasource.ContextSimple
		if r != nil {
			ctx = datasource.NewContextSimpleNative(rm)
		}
		exprNode, _ := expr.ParseExpression(a.DefaultValue)
		val, ok = vm.Eval(ctx, exprNode)
		if !ok {
			if exprNode != nil {
				switch exprNode.NodeType() {
				case "Func", "Identity":
					return nil
				}
			}
			val = value.NewValue(a.DefaultValue)
		}
		// return fmt.Sprintf("%v", val.Value())
	} else if r, ok = row.(map[string]interface{}); ok {
		if r != nil {
			for _, v := range a.Parent.Attributes {
				source := v.SourceName
				if source == "" {
					source = v.FieldName
				}
				var err error
				var val interface{}
				if val, err = shared.GetPath(source, row, ignoreSourcePath, useNerd); err == nil {
					rm[v.FieldName] = val
					if v.FieldName == a.FieldName {
						return fmt.Sprintf("%v", val)
					}
				}
			}
		}
		var ctx *datasource.ContextSimple
		if r != nil {
			ctx = datasource.NewContextSimpleNative(rm)
		}
		exprNode, _ := expr.ParseExpression(a.DefaultValue)
		val, ok = vm.Eval(ctx, exprNode)
		if !ok {
			if exprNode != nil {
				switch exprNode.NodeType() {
				case "Func", "Identity":
					return nil
				}
			}
			val = value.NewValue(a.DefaultValue)
		}
	}
	return fmt.Sprintf("%v", val.Value())
}

// Complete handling of primary key.
//  1. Uniqueness check against value in KVStore
//  2. ColumnID establishment for all fields in this row.  Generate if provided value = 0
//  3. Value mapping.
//
// returns true if there are values to process.
func (s *Session) processPrimaryKey(tbuf *TableBuffer, row interface{}, pqTablePath string,
	providedColID uint64, isChild, ignoreSourcePath, useNerdCapitalization bool) (bool, error) {

	if tbuf.Table.TimeQuantumType == "" {
		tbuf.CurrentTimestamp = time.Unix(0, 0)
	}

	directColumnID := false
	tbuf.CurrentPKValue = make([]interface{}, len(tbuf.PKAttributes))
	pqColPaths := make([]string, len(tbuf.PKAttributes))
	var pkLookupVal strings.Builder
	for i, pk := range tbuf.PKAttributes {
		var cval interface{}
		vals, pqps, err := s.readColumn(row, pqTablePath, pk, isChild, ignoreSourcePath, useNerdCapitalization)
		if err != nil {
			return false, fmt.Errorf("readColumn for PK - %v", err)
		}
		pqColPaths[i] = pqps[0]
		if vals == nil || len(vals) == 0 || (len(vals) == 1 && vals[0] == nil) {
			if isChild { // Nothing to do here, no child value
				return false, nil
			}
			return false, fmt.Errorf("empty or nil value for PK field %s - %s, len %d", pk.FieldName, pqColPaths[i],
				len(vals))
		}
		if len(vals) > 1 {
			return false, fmt.Errorf("multiple values for PK field %s [%v], Schema mapping issue?",
				pqColPaths[0], err)
		}
		cval = vals[0]
		tbuf.CurrentPKValue[i] = cval

// NEW IMPLEMENTATION STARTS
		mval, err := pk.MapValue(cval, nil, false)
		if err != nil {
			return false, fmt.Errorf("error mapping PK field %s [%v], Schema mapping issue?",
				pqColPaths[0], err)
		}
		strVal := pk.Render(mval)
		switch shared.TypeFromString(pk.Type) {
		case shared.Date, shared.DateTime:
			if i == 0 { // First field in PK is TQ (if TQ != "")
				tbuf.CurrentTimestamp, _, _ = shared.ToTQTimestamp(tbuf.Table.TimeQuantumType, strVal)
			}
			if pk.ColumnID {
				if cID, err := strconv.ParseInt(cval.(string), 10, 64); err == nil {
					tbuf.CurrentColumnID = uint64(cID)
					directColumnID = true
				}
			}
		case shared.Integer:
			if pk.ColumnID {
				if cID, err := strconv.ParseInt(strVal, 10, 64); err == nil {
					tbuf.CurrentColumnID = uint64(cID)
					directColumnID = true
				}
			}
		}

/*  REFACTOR THIS
		switch reflect.ValueOf(cval).Kind() {
		case reflect.String:
			// Do nothing already a string
			if i == 0 { // First field in PK is TQ (if TQ != "")
				if pk.MappingStrategy == "SysMillisBSI" || pk.MappingStrategy == "SysMicroBSI" {
					strVal := cval.(string)
					tbuf.CurrentTimestamp, _, _ = shared.ToTQTimestamp(tbuf.Table.TimeQuantumType, strVal)
				}
			}
			if pk.ColumnID {
				if cID, err := strconv.ParseInt(cval.(string), 10, 64); err == nil {
					tbuf.CurrentColumnID = uint64(cID)
					directColumnID = true
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
					tbuf.CurrentTimestamp, _, _ = shared.ToTQTimestamp(tbuf.Table.TimeQuantumType, ts.Format(tFormat))
				}
			}
		case reflect.Float64:
			orig := cval.(float64)
			f := fmt.Sprintf("%%10.%df", pk.Scale)
			cval = fmt.Sprintf(f, orig)
		case reflect.Float32:
			orig := cval.(float32)
			f := fmt.Sprintf("%%10.%df", pk.Scale)
			cval = fmt.Sprintf(f, orig)
		default:
			return false, fmt.Errorf("PK Lookup value [%v] unknown type, it is [%v]", cval,
				reflect.ValueOf(cval).Kind())
		}
*/
		if pkLookupVal.Len() == 0 {
			pkLookupVal.WriteString(strVal)
		} else {
			pkLookupVal.WriteString(fmt.Sprintf("+%s", strVal))
		}
	}

	if tbuf.ShouldLookupPrimaryKey() {
		// Can't use batch operation here unfortunately, but at least we have local batch cache
		localKey := indexPath(tbuf, tbuf.PKAttributes[0].FieldName, tbuf.Table.PrimaryKey+".PK")
		if lColID, ok := s.BatchBuffer.LookupLocalCIDForString(localKey, pkLookupVal.String()); !ok {
			colID, found, errx := s.lookupColumnID(tbuf, pkLookupVal.String(), "")
			if errx != nil {
				return false, fmt.Errorf("Dedup lookup error - %v", errx)
			}
			if found {
				tbuf.CurrentColumnID = colID
				return true, nil
			} else {
				if providedColID == 0 {
					// Generate new ColumnID.   Lookup the sequencer from the local cache by TQ
					errx = tbuf.NextColumnID(s.BitIndex)
					if errx != nil {
						return false, errx
					}
				} else {
					tbuf.CurrentColumnID = providedColID
				}
				// Add the PK via local cache batch operation
				s.BatchBuffer.SetPartitionedString(localKey, pkLookupVal.String(), tbuf.CurrentColumnID)
			}
		} else {
			tbuf.CurrentColumnID = lColID
			u.Warnf("PK %s found in cache.  PK mapping error for %s?", pkLookupVal.String(), tbuf.Table.Name )
		}
	} else {
		if !directColumnID {
			if providedColID == 0 {
				// Generate new ColumnID.   Lookup the sequencer from the local cache by TQ
				errx := tbuf.NextColumnID(s.BitIndex)
				if errx != nil {
					return false, errx
				}
			} else {
				tbuf.CurrentColumnID = providedColID
			}
		}
	}

	// Map the value(s) and update table
	for i, v := range tbuf.CurrentPKValue {
		if v == nil {
			return false, fmt.Errorf("PK mapping error %s - nil value", pqColPaths[i])
		}
		if _, err := tbuf.PKAttributes[i].MapValue(v, s, false); err != nil {
			return false, fmt.Errorf("PK mapping error %s - %v", pqColPaths[i], err)
		}
	}

	return false, nil
}

// Handle Secondary Keys.  Create the index in backing store
func (s *Session) processAlternateKeys(tbuf *TableBuffer, row interface{}, pqTablePath string,
	isChild, ignoreSourcePath, useNerdCapitalization bool) error {

	pqColPaths := make([]string, len(tbuf.SKMap))
	var skLookupVal strings.Builder
	i := 0
	for k, keyAttrs := range tbuf.SKMap {
		for _, v := range keyAttrs {
			var cval interface{}
			vals, pqps, err := s.readColumn(row, pqTablePath, v, isChild, ignoreSourcePath, useNerdCapitalization)
			if err != nil {
				return fmt.Errorf("readColumn for SK - %v", err)
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
		//s.BatchBuffer.SetKeyString(tbuf.Table.Name, k, secondaryKey, skLookupVal.String(),
		//	tbuf.CurrentColumnID)
		lookupKey := indexPath(tbuf, tbuf.PKAttributes[0].FieldName, k+".SK")
		s.BatchBuffer.SetPartitionedString(lookupKey, skLookupVal.String(), tbuf.CurrentColumnID)
		i++
	}
	return nil
}

func (s *Session) lookupColumnID(tbuf *TableBuffer, lookupVal, fkFieldSpec string) (uint64, bool, error) {

	kvIndex := indexPath(tbuf, tbuf.PKAttributes[0].FieldName, tbuf.Table.PrimaryKey+".PK")

	if fkFieldSpec != "" {
		// Use the secondary/alternate key specification.  In this case tbuf is the FK table
		kvIndex = indexPath(tbuf, tbuf.PKAttributes[0].FieldName, fkFieldSpec+".SK")
	}
	kvResult, err := s.KVStore.Lookup(kvIndex, lookupVal, reflect.Uint64, true)
	if err != nil {
		return 0, false, fmt.Errorf("KVStore error for [%s] = [%s], [%v]", kvIndex, lookupVal, err)
	}
	if kvResult == nil {
		return 0, false, nil
	}
	return kvResult.(uint64), true, nil
}

func indexPath(tbuf *TableBuffer, field, path string) string {

	lookupPath := fmt.Sprintf("%s/%s/%s,%s", tbuf.Table.Name, field, path,
		tbuf.CurrentTimestamp.Format(timeFmt))
	if tbuf.Table.TimeQuantumType == "YMDH" {
		ts := tbuf.CurrentTimestamp
		key := fmt.Sprintf("%s/%s/%s", tbuf.Table.Name, field, ts.Format(timeFmt))
		fpath := fmt.Sprintf("/%s/%s/%s/%s/%s", tbuf.Table.Name, field, path,
			fmt.Sprintf("%d%02d%02d", ts.Year(), ts.Month(), ts.Day()), ts.Format(timeFmt))
		lookupPath = key + "," + fpath
	}
	return lookupPath
}

// LookupKeyBatch - Process a batch of keys.
/*
func (s *Session) LookupKeyBatch(tbuf *TableBuffer, lookupVals map[interface{}]interface{},
	fkFieldSpec string) (map[interface{}]interface{}, error) {

	kvIndex := fmt.Sprintf("%s%s%s.PK", tbuf.Table.Name, ifDelim, tbuf.Table.PrimaryKey)
	if fkFieldSpec != "" {
		// Use the secondary/alternate key specification
		kvIndex = fmt.Sprintf("%s%s%s.SK", tbuf.Table.Name, ifDelim, fkFieldSpec)
	}
	lookupVals, err := s.KVStore.BatchLookup(kvIndex, lookupVals, false)
	if err != nil {
		return nil, fmt.Errorf("KVStore.LookupBatch error for [%s] - [%v]", kvIndex, err)
	}
	return lookupVals, nil
}
*/

func (s *Session) resolveFKLookupKey(v *Attribute, tbuf *TableBuffer, row interface{},
	ignoreSourcePath, useNerdCapitalization bool) (string, error) {

	var retVal strings.Builder
	root := "/"
	pqTablePath := fmt.Sprintf("%s%s", root, tbuf.Table.Name)
	if r, ok := row.(*reader.ParquetReader); ok {
		root = r.SchemaHandler.GetRootExName()
		pqTablePath = fmt.Sprintf("%s.%s", root, tbuf.Table.Name)
	}
	vals, _, err := s.readColumn(row, pqTablePath, v, false, ignoreSourcePath, useNerdCapitalization)
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
func (s *Session) ResetRowCache() {
	for _, v := range s.TableBuffers {
		v.rowCache = make(map[string]interface{})
	}
	s.BytesRead = 0
}


// Flushing - Flush in progress
func (s *Session) IsFlushing() bool {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	return s.flushing
}

func (s *Session) flush() error {

    if s.BatchBuffer != nil && !s.BatchBuffer.IsEmpty() {
		s.flushing = true
		defer func() {s.flushing = false}()
		start := time.Now()
        fb := shared.NewBatchBuffer(s.BitIndex, s.KVStore, batchBufferSize)
        s.BatchBuffer.MergeInto(fb)
		mergeTime := time.Since(start)
        s.BatchBuffer = shared.NewBatchBuffer(s.BitIndex, s.KVStore, batchBufferSize)
        if err := fb.Flush(); err != nil {
			u.Error(err)
			return err
		}
		duration := time.Since(start)
		if duration > time.Duration(30 * time.Second) {
			u.Debugf("FLUSH DURATION %v, MERGE TIME = %v", duration, mergeTime)
		}
	}
	if s.StringIndex != nil {
		if err := s.StringIndex.Flush(); err != nil {
			u.Error(err)
			return err
		}
	}
	return nil
}

// Flush - Flush data to backend.
func (s *Session) Flush() error {

	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	return s.flush()
}

// CloseSession - Close the session, flushing if necessary..
func (s *Session) CloseSession() error {

	if s == nil {
		u.Warn("attempt to close a session already closed")
		return nil
	}
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	err := s.flush()
	if err == nil {
		s.StringIndex = nil
		s.BitIndex = nil
	}
	return err

}


// UpdateRow - Perform an in-place update of a row.
func (s *Session) UpdateRow(table string, columnID uint64, updValueMap map[string]*rel.ValueColumn,
	timePartition time.Time) error {

	tbuf, ok := s.TableBuffers[table]
	if !ok {
		return fmt.Errorf("table %s is not open for this session", table)
	}
	tbuf.CurrentColumnID = columnID
	tbuf.CurrentTimestamp = timePartition
	for k, vc := range updValueMap {
		if _, found := tbuf.PKMap[k]; found {
			return fmt.Errorf("cannot update PK column %s.%s", table, k)
		}
		_, err := s.MapValue(table, k, vc.Value.Value(), true)
		if err != nil {
			return err
		}
	}
	return nil
}

// Commit - Block until the server nodes have persisted their work queues to a savepoint.
func (s *Session) Commit() error {

	if s.BitIndex == nil {
		return fmt.Errorf("attempting commit of a closed session")
	} 

	s.BitIndex.Commit()
	return nil

}

// MapValue - Convenience function for Mapper interface.
func (s *Session) MapValue(tableName, fieldName string, value interface{}, update bool) (val *big.Int, err error) {

	var table *Table
	var attr *Attribute
	table, err = LoadTable(s.tableCache, s.BasePath, s.KVStore, tableName, s.KVStore.Conn.Consul)
	if err != nil {
		return
	}
	attr, err = table.GetAttribute(fieldName)
	if err != nil {
		return nil, fmt.Errorf("attribute '%s' not found", fieldName)
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
		return attr.MapValue(value, s, update)
	}
	return attr.MapValue(value, nil, update) // Non load use case pass nil connection context
}

func fromJulianDay(days int32, nanos int64) time.Time {
	nanos = ((int64(days)-julianDayOfEpoch)*microsPerDay + nanos/1000) * 1000
	sec, nsec := nanos/time.Second.Nanoseconds(), nanos%time.Second.Nanoseconds()
	t := time.Unix(sec, nsec)
	return t.UTC()
}

// INT96ToTime - Handle parquet INT96 values.
func INT96ToTime(int96 string) time.Time {
	nanos := binary.LittleEndian.Uint64([]byte(int96[:8]))
	days := binary.LittleEndian.Uint32([]byte(int96[8:]))
	return fromJulianDay(int32(days), int64(nanos))
}
