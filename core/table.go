package core

// Table metadata management functions.

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"plugin"
	"reflect"
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/disney/quanta/qlbridge/expr"
	"github.com/disney/quanta/shared"
	"github.com/hamba/avro/v2"
	"github.com/hashicorp/consul/api"
)

// Table - Table structure.
type Table struct {
	*shared.BasicTable
	Attributes         []Attribute
	AttributeNameMap   map[string]*Attribute
	SelectorNode       expr.Node
	SelectorIdentities []string
	AvroSchema         avro.Schema
	kvStore            *shared.KVStore
	tableCache         *TableCacheStruct // copied from bitmap from session
}

type TableCacheStruct struct { // used in core Table
	TableCache     map[string]*Table
	TableCacheLock sync.RWMutex
}

// Attribute - Field structure.
type Attribute struct {
	*shared.BasicAttribute
	Parent         *Table
	valueMap       map[interface{}]uint64
	reverseMap     map[uint64]interface{}
	mapperInstance Mapper
	localLock      sync.RWMutex
}

func (a *Attribute) GetParent() shared.TableInterface {
	return a.Parent // not BasicAttribute parent
}

const (
	// SEP - Path Separator
	SEP = string(os.PathSeparator)
)

func NewTableCacheStruct() *TableCacheStruct {
	tcs := &TableCacheStruct{}
	tcs.TableCache = make(map[string]*Table)
	return tcs
}

// LoadTable - Load and initialize table object.
func LoadTable(tableCache *TableCacheStruct, path string, kvStore *shared.KVStore, name string, consulClient *api.Client) (*Table, error) {

	tableCache.TableCacheLock.Lock()
	defer tableCache.TableCacheLock.Unlock()

	if t, ok := tableCache.TableCache[name]; ok {
		coreTable := t //, ok := t.(*Table)
		// if !ok {
		// 	return nil, fmt.Errorf("table %s is not a core table", name)
		// }
		coreTable.kvStore = kvStore
		//u.Debugf("Found table %s in cache.", name)
		return coreTable, nil
	}

	u.Debugf("Loading table %s.", name)
	sch, err := shared.LoadSchema(path, name, consulClient)
	if err != nil {
		return nil, err
	}

	table := &Table{BasicTable: sch, kvStore: kvStore, Attributes: make([]Attribute, len(sch.Attributes))}
	table.tableCache = tableCache
	for j := range sch.Attributes { // wrap BasicAttributes
		v := &Attribute{BasicAttribute: &sch.Attributes[j]}
		table.Attributes[j] = *v // copylocks is ok here
		table.Attributes[j].Parent = table
	}

	table.AttributeNameMap = make(map[string]*Attribute)

	// Refactor this atw fixme
	/*
		lock, err := shared.Lock(consulClient, name, "LoadSchema")
		if err != nil {
			return nil, err
		}
		defer lock.Unlock()
	*/

	var fieldMap map[string]*Field
	var errx error
	if fieldMap, errx = table.LoadFieldValues(); errx != nil {
		return nil, errx
	}

	i := 1
	for j, v := range table.Attributes {

		if v.SourceName == "" && v.FieldName == "" {
			return nil, fmt.Errorf("a valid attribute must have an input source name or field name.  Neither exists")
		}

		// Register a plugin if present
		if v.MappingStrategy == "Custom" || v.MappingStrategy == "CustomBSI" {
			if v.MapperConfig == nil {
				return nil, fmt.Errorf("custom plugin configuration missing")
			}
			if pname, ok := v.MapperConfig["name"]; !ok {
				return nil, fmt.Errorf("custom plugin name not specified")
			} else if plugPath, ok := v.MapperConfig["plugin"]; !ok {
				return nil, fmt.Errorf("custom plugin SO name not specified")
			} else {
				plug, err := plugin.Open(plugPath + ".so")
				if err != nil {
					return nil, fmt.Errorf("cannot open '%s' %v", plugPath, err)
				}
				symFactory, err := plug.Lookup("New" + pname)
				if err != nil {
					return nil, fmt.Errorf("new"+pname+"%v", err)
				}
				factory, ok := symFactory.(func(map[string]string) (Mapper, error))
				if !ok {
					return nil, fmt.Errorf("unexpected type from module symbol New%s", pname)
				}
				Register(pname, factory)
			}
		}

		if v.MappingStrategy == "ParentRelation" {
			if v.ForeignKey == "" {
				return nil, fmt.Errorf("foreign key table name must be specified for %s", v.FieldName)
			}
		}
		if v.MappingStrategy != "ChildRelation" {
			if table.Attributes[j].mapperInstance, err = ResolveMapper(&v); err != nil {
				return nil, err
			}
		}

		if v.FieldName != "" {

			// check to see if there are values in the API call (if applicable)
			lookupName := table.Name + SEP + v.FieldName + ".StringEnum"
			// if there are values in schema.yaml then override string enum values in global cache
			if f, ok := fieldMap[lookupName]; ok && len(table.Attributes[j].Values) > 0 {
				// Pull it in
				values := make([]FieldValue, 0)
				for _, x := range table.Attributes[j].Values {
					values = append(values, FieldValue{Mapping: x.Value.(string), Value: uint64(x.RowID),
						Label: x.Value.(string)})
				}
				f.Values = values
			}

			// Dont allow string enum values to override local cache
			if x, ok := fieldMap[lookupName]; ok && len(table.Attributes[j].Values) == 0 {
				var values []shared.Value = make([]shared.Value, 0)
				for _, z := range x.Values {
					if z.Mapping == "" {
						z.Mapping = z.Label
					}
					values = append(values, shared.Value{Value: z.Mapping, RowID: uint64(z.Value), Desc: z.Label})
				}
				table.Attributes[j].Values = values
			}

			// check to see if there is an external json values file and load it
			if x, err3 := ioutil.ReadFile(path + SEP + name + SEP + v.FieldName + ".json"); err3 == nil {
				var values []shared.Value
				if err4 := json.Unmarshal(x, &values); err4 == nil {
					table.Attributes[j].Values = values
				}
			}

			table.AttributeNameMap[v.FieldName] = &table.Attributes[j]
		}

		if v.FieldName == "" {
			if v.MappingStrategy == "ChildRelation" {
				if v.ChildTable == "" {
					// Child table name must be leaf in path ('.' is path sep)
					idx := strings.LastIndex(v.SourceName, ".")
					if idx >= 0 {
						table.Attributes[j].ChildTable = v.SourceName[idx+1:]
					} else {
						table.Attributes[j].ChildTable = v.SourceName
					}
				}
				continue
			}
			v.FieldName = v.SourceName
			table.AttributeNameMap[v.SourceName] = &table.Attributes[j]
		}

		// Enable lookup by alias (field name)
		if v.SourceName == "" || v.SourceName != v.FieldName {
			table.AttributeNameMap[v.FieldName] = &table.Attributes[j]
		}
		table.Attributes[j].valueMap = make(map[interface{}]uint64)
		table.Attributes[j].reverseMap = make(map[uint64]interface{})
		if len(table.Attributes[j].Values) > 0 {
			for _, x := range table.Attributes[j].Values {
				table.Attributes[j].valueMap[x.Value] = x.RowID
				table.Attributes[j].reverseMap[x.RowID] = x.Value
			}
		}

		if v.Type == "NotExist" || v.Type == "NotDefined" || v.Type == "JSON" {
			continue
		}
		table.Attributes[j].Ordinal = i

		i++
	}

	if table.PrimaryKey == "" && table.TimeQuantumField == "" {
		// If the table is partitioned then either a primary key (with time field in first position) or
		// the time quantum field must be specified specified.
		if table.TimeQuantumType != "" {
			return nil, fmt.Errorf("The table %s is partitioned but 'timeQuantumField' is not specified", table.Name)
		}
	} else {
		pka, err := table.GetPrimaryKeyInfo()
		if err != nil {
			return nil,
				fmt.Errorf("A primary key field was defined but it does not contain valid field name(s) [%s] - %v",
					table.PrimaryKey, err)
		}
		timeQuantumField := strings.TrimSpace(table.TimeQuantumField)
		var timeQuantumAttr *Attribute
		if timeQuantumField == "" && table.TimeQuantumType != "" && len(pka) < 2 {
			return nil, fmt.Errorf("time partitions enabled for but 'timeQuantumField' not specified")
		}
		if timeQuantumField == "" && table.TimeQuantumType != "" && len(pka) >= 2 {
			timeQuantumField = pka[0].FieldName
		}
		if timeQuantumField != "" {
			if at, err := table.GetAttribute(timeQuantumField); err == nil {
				timeQuantumAttr = at
			}
		}
		if table.TimeQuantumType != "" && (timeQuantumAttr.Type != "Date" && timeQuantumAttr.Type != "DateTime") {
			return nil, fmt.Errorf("time partitions enabled for %s, Type must be Date or DateTime", timeQuantumField)
		}
	}

    // Parse and verify selector expression if it exists.
	if table.Selector != "" {
		table.SelectorNode, err = expr.ParseExpression(table.Selector)
		if err != nil {
			return nil, fmt.Errorf("parsing of selector %v failed: %v", table.Selector, err)
		}
		table.SelectorIdentities = expr.FindAllIdentityField(table.SelectorNode)
		u.Infof("table seletor enabled -> %v <-",table.Selector)
	}
	table.AvroSchema = shared.ToAvroSchema(table.BasicTable)
	tableCache.TableCache[name] = table
	return table, nil
}

// GetAttribute - Get a table's attribute by name.
func (t *Table) GetAttribute(name string) (*Attribute, error) {

	if name == "@rownum" {
		return t.GetRownumAttribute(), nil
	}

	if t == nil || t.AttributeNameMap == nil {
		return nil, fmt.Errorf("schema cache not re-initialized ")
	}

	if attr, ok := t.AttributeNameMap[name]; ok {
		return attr, nil
	}
	return nil, fmt.Errorf("attribute '%s' not found", name)
}

// GetRownumAttribute - Return a description of @rownum
func (t *Table) GetRownumAttribute() *Attribute {

	b := &shared.BasicAttribute{}
	b.FieldName = "@rownum"
	b.MappingStrategy = "IntBSI"
	b.Type = "Integer"
	at := &Attribute{BasicAttribute: b, Parent: t}
	mi, err := ResolveMapper(at)
	if err == nil {
		at.mapperInstance = mi
	}
	return at
}

// GetPrimaryKeyInfo - Return attributes for a given PK.
func (t *Table) GetPrimaryKeyInfo() ([]*Attribute, error) {
	s := strings.Split(t.PrimaryKey, "+")
	attrs := make([]*Attribute, len(s))
	var v string
	i := 0
	if t.TimeQuantumField != "" {
		if len(s) > 1 {
			attrs = make([]*Attribute, len(s)+1)
		} else {
			attrs = make([]*Attribute, 1)
		}
		if at, err := t.GetAttribute(strings.TrimSpace(t.TimeQuantumField)); err == nil {
			attrs[0] = at
			i++
		} else {
			return nil, err
		}
	}
	if t.PrimaryKey != "" {
		for i, v = range s {
			if attr, err := t.GetAttribute(strings.TrimSpace(v)); err == nil {
				attrs[i] = attr
			} else {
				return nil, err
			}
		}
	}
	return attrs, nil
}

// GetAlternateKeyInfo - Return attributes for a given SK.
func (t *Table) GetAlternateKeyInfo() (map[string][]*Attribute, error) {

	ret := make(map[string][]*Attribute)
	s1 := strings.Split(t.SecondaryKeys, ",")
	for _, v := range s1 {
		s2 := strings.Split(strings.TrimSpace(v), "+")
		attrs := make([]*Attribute, len(s2))
		for i, w := range s2 {
			if attr, err := t.GetAttribute(strings.TrimSpace(w)); err == nil {
				attrs[i] = attr
			} else {
				return nil, err
			}
		}
		ret[strings.TrimSpace(v)] = attrs
	}

	return ret, nil
}

// GetFKSpec - Get info for foreign key
func (a *Attribute) GetFKSpec() (string, string, error) {
	if a.ForeignKey == "" {
		return "", "", fmt.Errorf("field %s.%s is not a foreign key", a.Parent.Name, a.FieldName)
	}
	s := strings.Split(a.ForeignKey, ".")
	table := s[0]
	hasFieldSpec := len(s) > 1
	fieldSpec := ""
	if hasFieldSpec {
		fieldSpec = s[1]
	}
	return table, fieldSpec, nil
}

// GetValue - Return row ID for a given input value (StringEnum).
func (a *Attribute) GetValue(invalue interface{}) (uint64, error) {

	parentTable := a.Parent //.(*Table)

	parentTable.tableCache.TableCacheLock.RLock()
	defer parentTable.tableCache.TableCacheLock.RUnlock()

	//la, lerr := tableCache[a.Parent.Name].GetAttribute(a.FieldName)
	// why are we doing this? We have the parent, why look in the cache?
	la, lerr := parentTable.tableCache.TableCache[parentTable.Name].GetAttribute(a.FieldName)
	if lerr != nil {
		return 0, fmt.Errorf("Cannot lookup attribute %s from table cache.", a.FieldName)
	}
	la.localLock.RLock()

	value := invalue
	switch invalue.(type) {
	case string:
		value = strings.TrimSpace(invalue.(string))
	}
	var v uint64
	var ok bool
	if v, ok = a.valueMap[value]; !ok {
		/* If the value does not exist in the valueMap local cache  we will add it and then
		 *  Call the string enum service to add it.
		 */

		la.localLock.RUnlock()

		if a.Parent.kvStore == nil {
			return 0, fmt.Errorf("kvStore is not initialized")
		}
		if a.Parent.Name == "" {
			panic("a.Parent.Name is empty")
		}

		la.localLock.Lock()

		// OK, value not anywhere to be found, invoke service to add.
		rowID, err := a.Parent.kvStore.PutStringEnum(a.Parent.Name+SEP+a.FieldName+".StringEnum",
			value.(string))
		if err != nil {
			return 0, err
		}

		a.Values = append(a.Values, shared.Value{Value: value, RowID: rowID})
		a.valueMap[value] = rowID
		a.reverseMap[rowID] = value

		v = rowID
		u.Infof("Added enum for field = %s, value = %v, ID = %v", a.FieldName, value, v)

		la.localLock.Unlock()
		la.localLock.RLock()
	}
	la.localLock.RUnlock()
	return v, nil
}

// GetValueForID - Reverse map a value for a given row ID.  (StringEnum)
func (a *Attribute) GetValueForID(id uint64) (interface{}, error) {

	parentTable := a.Parent

	if parentTable.AttributeNameMap == nil {
		parentTable.tableCache.TableCacheLock.Lock()
		defer parentTable.tableCache.TableCacheLock.Unlock()
	} else {
		parentTable.tableCache.TableCacheLock.RLock()
		defer parentTable.tableCache.TableCacheLock.RUnlock()
	}

	la, lerr := parentTable.tableCache.TableCache[a.Parent.Name].GetAttribute(a.FieldName)
	if lerr != nil {
		return 0, fmt.Errorf("Cannot lookup attribute %s from table cache.", a.FieldName)
	}
	la.localLock.RLock()

	if v, ok := a.reverseMap[id]; ok {
		la.localLock.RUnlock()
		return v, nil
	}
	la.localLock.RUnlock()
	la.localLock.Lock()
	defer la.localLock.Unlock()

	if a.MappingStrategy != "StringEnum" {
		return 0, fmt.Errorf("GetValueForID attribute %s is not a StringEnum", a.FieldName)
	}
	lookupName := a.Parent.Name + SEP + a.FieldName + ".StringEnum"
	x, err := a.Parent.kvStore.Items(lookupName, reflect.String, reflect.Uint64)
	if err != nil {
		return nil, fmt.Errorf("ERROR: Cannot open enum for table %s, field %s. [%v]", a.Parent.Name,
			a.FieldName, err)
	}
	for kk, vv := range x {
		k := kk.(string)
		v := vv.(uint64)
		a.reverseMap[v] = k
	}
	if v, ok := a.reverseMap[id]; ok { // Try again
		return v, nil
	}
	return 0, fmt.Errorf("Attribute %s - Cannot locate value for rowID '%v'", a.FieldName, id)
}

// Transform - Perform a tranformation of a value (optional)
func (a *Attribute) Transform(val interface{}, c *Session) (newVal interface{}, err error) {

	if a.mapperInstance == nil {
		return 0, fmt.Errorf("attribute '%s' MapperInstance is nil", a.FieldName)
	}
	return a.mapperInstance.Transform(a, val, c)
}

// MapValue - Return the row ID for a given value (Standard Bitmap)
func (a *Attribute) MapValue(val interface{}, c *Session) (result uint64, err error) {

	if a.mapperInstance == nil {
		return 0, fmt.Errorf("attribute '%s' MapperInstance is nil", a.FieldName)
	}
	return a.mapperInstance.MapValue(a, val, c)
}

// MapValueReverse - Re-hydrate the original value for a given row ID.
func (a *Attribute) MapValueReverse(id uint64, c *Session) (result interface{}, err error) {

	if a.mapperInstance == nil {
		return 0, fmt.Errorf("attribute '%s' MapperInstance is nil", a.FieldName)
	}
	return a.mapperInstance.MapValueReverse(a, id, c)
}

// ToBackingValue - Re-hydrate the original value.
func (a *Attribute) ToBackingValue(rowIDs []uint64, c *Session) (result string, err error) {

	s := make([]string, len(rowIDs))
	for i, rowID := range rowIDs {
		v, err := a.MapValueReverse(rowID, c)
		if err != nil {
			return "", err
		}
		switch t := v.(type) {
		case string:
			s[i] = v.(string)
		case bool:
			s[i] = fmt.Sprintf("%v", v)
		case int, int32, int64:
			s[i] = fmt.Sprintf("%d", v)
		default:
			return "", fmt.Errorf("ToBackingValue: Unsupported type %T", t)
		}
	}
	return strings.Join(s, a.mapperInstance.GetMultiDelimiter()), nil
}

// Field Metadata struct
type Field struct {
	Name      string       `json:name`
	Label     string       `json:label`
	Fieldtype string       `json:fieldType`
	MinValue  int          `json:minValue`
	MaxValue  int          `json:maxValue`
	Values    []FieldValue `json:values`
	Indextype string       `json:indexType`
}

// FieldValue Metadata struct
type FieldValue struct {
	Label   string `json:label`
	Value   uint64 `json:value`
	Mapping string `json:mapping`
}

// LoadFieldValues from string enum repository.
func (t *Table) LoadFieldValues() (fieldMap map[string]*Field, err error) {

	if t.kvStore == nil {
		return nil, nil
	}
	if t.Name == "" {
		panic("t.Name is nil")
	}

	var attributeFieldMap map[string]*Field = make(map[string]*Field)

	for _, attr := range t.Attributes {
		if attr.MappingStrategy != "StringEnum" {
			continue
		}
		lookupName := t.Name + SEP + attr.FieldName + ".StringEnum"
		x, err := t.kvStore.Items(lookupName, reflect.String, reflect.Uint64)
		if err != nil {
			return nil, fmt.Errorf("ERROR: Cannot open enum for table %s, field %s. [%v]", t.Name,
				attr.FieldName, err)
		}
		for kk, vv := range x {
			k := kk.(string)
			v := vv.(uint64)
			if f, ok := attributeFieldMap[lookupName]; !ok {
				f := &Field{Name: attr.FieldName, Label: attr.FieldName}
				attributeFieldMap[lookupName] = f
				f.Values = make([]FieldValue, 0)
				f.Values = append(f.Values, FieldValue{Label: k, Mapping: k, Value: v})
			} else {
				f.Values = append(f.Values, FieldValue{Label: k, Mapping: k, Value: v})
			}
		}
	}

	return attributeFieldMap, nil
}

// ClearTableCache - Clear the table cache.
// This no longer makes sense.
// needs arg. There's no tableCache except in bitmap from session
func not_ClearTableCache() {
}
