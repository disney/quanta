package core
// Table metadata management functions.

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/akrylysov/pogreb"
	"github.com/araddon/qlbridge/value"
	"github.com/hashicorp/consul/api"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"plugin"
	"strings"
	"sync"
)

// Table - Table structure.
type Table struct {
	Name             string                `yaml:"tableName"`
	PrimaryKey       string                `yaml:"primaryKey,omitempty"`
	SecondaryKeys    string                `yaml:"secondaryKeys,omitempty"`
	DefaultPredicate string                `yaml:"defaultPredicate,omitempty"`
	TimeQuantumType  string                `yaml:"timeQuantumType,omitempty"`
	DisableDedup     bool                  `yaml:"disableDedup"`
	Attributes       []Attribute           `yaml:"attributes"`
	attributeNameMap map[string]*Attribute `yaml:"-"`
	ConsulClient     *api.Client           `yaml:"-"`
	lock             *api.Lock             `yaml:"-"`
	localLock        sync.RWMutex          `yaml:"-"`
	basePath         string                `yaml:"-"`
	metadataPath     string                `yaml:"-"`
}

// Attribute - Field structure.
type Attribute struct {
	Parent           *Table                 `yaml:"-" json:"-"`
	SourceName       string                 `yaml:"sourceName"`
	ChildTable       string                 `yaml:"childTable"`
	FieldName        string                 `yaml:"fieldName"`
	Type             string                 `yaml:"type"`
	ForeignKey       string                 `yaml:"foreignKey,omitempty"`
	MappingStrategy  string                 `yaml:"mappingStrategy"`
	Size             int                    `yaml:"maxLen,omitempty"`
	Ordinal          int                    `yaml:"-"`
	Scale            int                    `yaml:"scale,omitempty"`
	Values           []Value                `yaml:"values,omitempty"`
	MapperConfig     map[string]string      `yaml:"configuration,omitempty"`
	Desc             string                 `yaml:"desc,omitempty"`
	MinValue         int                    `yaml:"minValue,omitempty"`
	MaxValue         int                    `yaml:"maxValue,omitempty"`
	CallTransform    bool                   `yaml:"callTransform,omitempty"`
	HighCard         bool                   `yaml:"highCard"`
	Required         bool                   `yaml:"required,omitempty"`
	Searchable       bool                   `yaml:"searchable,omitempty"`
	DefaultValue     string                 `yaml:"defaultValue,omitempty"`
	valueMap         map[interface{}]uint64 `yaml:"-"`
	reverseMap       map[uint64]interface{} `yaml:"-" json:"-"`
	mapperInstance   Mapper                 `yaml:"-"`
	ColumnID         bool                   `yaml:"columnID,omitempty"`
	ColumnIDMSV      bool                   `yaml:"columnIDMSV,omitempty"`
	IsTimeSeries     bool                   `yaml:"isTimeSeries,omitempty"`
	TimeQuantumType  string                 `yaml:"timeQuantumType,omitempty"`
	Exclusive        bool                   `yaml:"exclusive,omitempty"`
	DelegationTarget string                 `yaml:"delegationTarget,omitempty"`
}

// Value - Metadata value items for StringEnum mapper type.
type Value struct {
	Value interface{} `yaml:"value" json:"value"`
	RowID uint64      `yaml:"rowID" json:"rowID"`
	Desc  string      `yaml:"desc,omitempty" json:"desc,omitempty"`
}

// DataType - Field data types.
type DataType int

// Constant defines for data type.
const (
	NotExist = DataType(iota)
	String
	Integer
	Float
	Date
	DateTime
	Boolean
	JSON
	NotDefined
)

// String - Return string respresentation of DataType
func (vt DataType) String() string {
	switch vt {
	case NotExist:
		return "NotExist"
	case String:
		return "String"
	case Integer:
		return "Integer"
	case Float:
		return "Float"
	case Boolean:
		return "Boolean"
	case JSON:
		return "JSON"
	case Date:
		return "Date"
	case DateTime:
		return "DateTime"
	case NotDefined:
		return "NotDefined"
	default:
		return "NotDefined"
	}
}

// TypeFromString - Construct a DataType from the string representation.
func TypeFromString(vt string) DataType {
	switch vt {
	case "NotExist":
		return NotExist
	case "String":
		return String
	case "Integer":
		return Integer
	case "Float":
		return Float
	case "Boolean":
		return Boolean
	case "JSON":
		return JSON
	case "Date":
		return Date
	case "DateTime":
		return DateTime
	default:
		return NotDefined
	}
}

// ValueTypeFromString - Get value type for a given string representation.j
func ValueTypeFromString(vt string) value.ValueType {
	switch vt {
	case "NotExist":
		return value.NilType
	case "String":
		return value.StringType
	case "Integer":
		return value.IntType
	case "Float":
		return value.NumberType
	case "Boolean":
		return value.BoolType
	case "Date":
		return value.TimeType
	case "DateTime":
		return value.TimeType
	default:
		return value.UnknownType
	}
}

const (
    // SEP - Path Separator
	SEP    = string(os.PathSeparator)
	fDelim = ":"
)

var (
	tableCache     map[string]*Table = make(map[string]*Table, 0)
	tableCacheLock sync.Mutex
)

// LoadSchema - Load a new Table object from configuration.
func LoadSchema(path, metadataPath, name string, consulClient *api.Client) (*Table, error) {

	tableCacheLock.Lock()
	defer tableCacheLock.Unlock()
	if t, ok := tableCache[name]; ok {
		return t, nil
	}

	b, err := ioutil.ReadFile(path + SEP + name + SEP + "schema.yaml")
	if err != nil {
		return nil, err
	}
	var table Table
	err2 := yaml.Unmarshal(b, &table)
	if err2 != nil {
		return nil, err2
	}

	table.ConsulClient = consulClient
	table.basePath = path
	table.metadataPath = metadataPath

	table.attributeNameMap = make(map[string]*Attribute)
	if err := table.Lock(); err != nil {
		return nil, err
	}

	defer table.Unlock()

	var fieldMap map[string]*Field
	var errx error
	if fieldMap, errx = table.LoadFieldValues(); errx != nil {
		return nil, errx
	}

	i := 1
	for j, v := range table.Attributes {

		table.Attributes[j].Parent = &table
		v.Parent = &table

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
			// Force field to be mapped by IntBSIMapper
			v.MappingStrategy = "IntBSI"
		}
		if v.MappingStrategy != "ChildRelation" {
			if table.Attributes[j].mapperInstance, err = ResolveMapper(&v); err != nil {
				return nil, err
			}
		}

		if v.FieldName != "" {

			// check to see if there are values in the API call (if applicable)
			//if x, ok := fieldMap[table.Name + "_" + v.FieldName]; ok {

			// if there are values in schema.yaml then override metadata values in global cache
			if f, ok := fieldMap[v.FieldName]; ok && len(table.Attributes[j].Values) > 0 {
				// Pull it in
				values := make([]FieldValue, 0)
				for _, x := range table.Attributes[j].Values {
					values = append(values, FieldValue{Mapping: x.Value.(string), Value: uint64(x.RowID),
						Label: x.Value.(string)})
				}
				f.Values = values
			}

			// Dont allow metadata values to override local cache
			if x, ok := fieldMap[v.FieldName]; ok && len(table.Attributes[j].Values) == 0 {
				var values []Value = make([]Value, 0)
				for _, z := range x.Values {
					if z.Mapping == "" {
						z.Mapping = z.Label
					}
					values = append(values, Value{Value: z.Mapping, RowID: uint64(z.Value), Desc: z.Label})
				}
				table.Attributes[j].Values = values
			}

			// check to see if there is an external json values file and load it
			if x, err3 := ioutil.ReadFile(path + SEP + name + SEP + v.FieldName + ".json"); err3 == nil {
				var values []Value
				if err4 := json.Unmarshal(x, &values); err4 == nil {
					table.Attributes[j].Values = values
				}
			}

			table.attributeNameMap[v.FieldName] = &table.Attributes[j]
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
			table.attributeNameMap[v.SourceName] = &table.Attributes[j]
		}

		// Enable lookup by alias (field name)
		if v.SourceName == "" || v.SourceName != v.FieldName {
			table.attributeNameMap[v.FieldName] = &table.Attributes[j]
		}
		if len(table.Attributes[j].Values) > 0 {
			table.Attributes[j].valueMap = make(map[interface{}]uint64)
			table.Attributes[j].reverseMap = make(map[uint64]interface{})
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

	if table.PrimaryKey != "" {
		pka, err := table.GetPrimaryKeyInfo()
		if err != nil {
			return nil,
				fmt.Errorf("A primary key field was defined but it is not valid field name(s) [%s] - %v",
					table.PrimaryKey, err)
		}
		if table.TimeQuantumType != "" && (pka[0].Type != "Date" && pka[0].Type != "DateTime") {
			return nil, fmt.Errorf("time partitions enabled for PK %s, Type must be Date or DateTime",
				pka[0].FieldName)
		}
	}

	tableCache[name] = &table
	return &table, nil
}

// GetAttribute - Get a tables attribute by name.
func (t *Table) GetAttribute(name string) (*Attribute, error) {

	if attr, ok := t.attributeNameMap[name]; ok {
		return attr, nil
	}
	return nil, fmt.Errorf("attribute '%s' not found", name)
}

// GetPrimaryKeyInfo - Return attributes for a given PK.
func (t *Table) GetPrimaryKeyInfo() ([]*Attribute, error) {
	s := strings.Split(t.PrimaryKey, "+")
	attrs := make([]*Attribute, len(s))
	for i, v := range s {
		if attr, err := t.GetAttribute(strings.TrimSpace(v)); err == nil {
			attrs[i] = attr
		} else {
			return nil, err
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

// GetValue - Return row ID for a given input value (StringEnum).
func (a *Attribute) GetValue(invalue interface{}) (uint64, error) {

	value := invalue
	switch invalue.(type) {
	case string:
		value = strings.TrimSpace(invalue.(string))
	}
	var v uint64
	var ok bool
	a.Parent.localLock.RLock()
	if v, ok = a.valueMap[value]; !ok {
		/* If the value does not exist in the valueMap local cache  we will add it and then
		 *  Call the metadata service to add it.
		 */

		a.Parent.localLock.RUnlock()
		if err := a.Parent.Lock(); err != nil {
			return 0, err
		}
		defer a.Parent.Unlock()

		// OK, value not anywhere to be found, invoke service to add.
		val, err := a.Parent.AddFieldValue(a.FieldName, value)
        if err != nil {
			return 0, err
		}
		v = val
		log.Printf("Updated metadata for field = %s, value = %v, ID = %v", a.FieldName, value, v)
	} else {
		a.Parent.localLock.RUnlock()
	}
	return v, nil
}

// GetValueForID - Reverse map a value for a given row ID.  (StringEnum)
func (a *Attribute) GetValueForID(id uint64) (interface{}, error) {

	if v, ok := a.reverseMap[id]; ok {
		return v, nil
	}
	return 0, fmt.Errorf("Attribute %s - Cannot locate value for rowID '%v'", a.SourceName, id)
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

// Transform - Perform a tranformation of a value (optional)
func (a *Attribute) Transform(val interface{}, c *Connection) (newVal interface{}, err error) {

	if a.mapperInstance == nil {
		return 0, fmt.Errorf("attribute '%s' MapperInstance is nil", a.FieldName)
	}
	return a.mapperInstance.Transform(a, val, c)
}

// MapValue - Return the row ID for a given value (Standard Bitmap)
func (a *Attribute) MapValue(val interface{}, c *Connection) (result uint64, err error) {

	if a.mapperInstance == nil {
		return 0, fmt.Errorf("attribute '%s' MapperInstance is nil", a.FieldName)
	}
	return a.mapperInstance.MapValue(a, val, c)
}

// MapValueReverse - Re-hydrate the original value for a given row ID.
func (a *Attribute) MapValueReverse(id uint64, c *Connection) (result interface{}, err error) {

	if a.mapperInstance == nil {
		return 0, fmt.Errorf("attribute '%s' MapperInstance is nil", a.FieldName)
	}
	return a.mapperInstance.MapValueReverse(a, id, c)
}

// ToBackingValue - Re-hydrate the original value.
func (a *Attribute) ToBackingValue(rowIDs []uint64, c *Connection) (result string, err error) {

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

// IsBSI - Is this attribute a BSI?
func (a *Attribute) IsBSI() bool {

	// TODO:  Add IsBSI() to Mapper interface and let mappers self describe
	switch a.MappingStrategy {
	case "IntBSI", "FloatScaleBSI", "SysMillisBSI", "SysMicroBSI", "SysSecBSI", "StringHashBSI", "CustomBSI", "ParentRelation":
		return true
	default:
		return false
	}
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

//
// AddFieldValue - Add a new field value.
// NOTE: Must be called inside a mutex/concurrency guard.
//
//
func (t *Table) AddFieldValue(fieldName string, value interface{}) (rowID uint64, err error) {

	db, errx := pogreb.Open(t.metadataPath+SEP+t.Name, nil)
	if errx != nil {
		err = fmt.Errorf("ERROR: Cannot open metadata for table %s. [%v]", t.Name, errx)
		return
	}
	defer db.Close()

	attr, errx := t.GetAttribute(fieldName)
	if errx != nil {
		err = errx
		return
	}

	// Iterate over Values to find maximum rowID
	for _, fv := range attr.Values {
		if fv.RowID > rowID {
			rowID = fv.RowID
		}
		if fv.Value == value.(string) {
			// Should never get here
			rowID = fv.RowID
			err = fmt.Errorf("ERROR: FieldValue %s already exists in metadata for table %s, field %s",
				fv.Value, t.Name, fieldName)
			return
		}
	}
	rowID++

	attr.Values = append(attr.Values, Value{Value: value, RowID: rowID})
	if attr.valueMap == nil {
		attr.valueMap = make(map[interface{}]uint64, 0)
	}
	if attr.reverseMap == nil {
		attr.reverseMap = make(map[uint64]interface{}, 0)
	}
	attr.valueMap[value] = rowID
	attr.reverseMap[rowID] = value

	key := fmt.Sprintf("%s%s%v", fieldName, fDelim, value)
	v := make([]byte, 8)
	binary.LittleEndian.PutUint64(v, rowID)
	if err2 := db.Put([]byte(key), v); err != nil {
		err = err2
		rowID = 0
	}

	//attr.Values = append(attr.Values, Value{Value: value.(string), RowID: rowID, Desc: value.(string)})
	return
}

// LoadFieldValues from metadata repository.
func (t *Table) LoadFieldValues() (fieldMap map[string]*Field, err error) {

	var attributeFieldMap map[string]*Field = make(map[string]*Field)

	tableDir := t.metadataPath + SEP + t.Name
	os.MkdirAll(tableDir, 0755)
	db, errx := pogreb.Open(tableDir, nil)
	if errx != nil {
		return nil, fmt.Errorf("ERROR: Cannot open metadata for table %s. [%v]", t.Name, errx)
	}
	defer db.Close()

	it := db.Items()
	for {
		fullKey, val, err := it.Next()
		if err != nil {
			if err != pogreb.ErrIterationDone {
				return nil, err
			}
			break
		}
		s := strings.SplitN(string(fullKey), fDelim, 2)
		v := binary.LittleEndian.Uint64(val)

		if f, ok := attributeFieldMap[s[0]]; !ok {
			f := &Field{Name: s[0], Label: s[0]}
			attributeFieldMap[s[0]] = f
			f.Values = make([]FieldValue, 0)
			f.Values = append(f.Values, FieldValue{Label: s[1], Mapping: s[1], Value: v})
		} else {
			f.Values = append(f.Values, FieldValue{Label: s[1], Mapping: s[1], Value: v})
		}

	}

	return attributeFieldMap, nil
}

// Lock the table.
func (t *Table) Lock() error {

	var err error
	// If Consul client is not set then we are not running in distributed mode.  Use local mutex.
	if t.ConsulClient == nil {
		t.localLock.Lock()
		return nil
	}

	// create lock key
	opts := &api.LockOptions{
		Key:        t.Name + "/1",
		Value:      []byte("set by loader"),
		SessionTTL: "10s",
		/*
		   		SessionOpts: &api.SessionEntry{
		     	Checks:   []string{"check1", "check2"},
		     	Behavior: "release",
		   		},
		*/
	}

	t.lock, err = t.ConsulClient.LockOpts(opts)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			//log.Println("Interrupted...")
			err = t.lock.Unlock()
			if err != nil {
				return
			}
		}
	}()

	// acquire lock
	//log.Println("Acquiring lock ...")
	stopCh := make(chan struct{})
	lockCh, err := t.lock.Lock(stopCh)
	if err != nil {
		return err
	}
	if lockCh == nil {
		return fmt.Errorf("lock already held")
	}
	return nil
}

// Unlock the table.
func (t *Table) Unlock() error {

	var err error
	if t.ConsulClient == nil {
		t.localLock.Unlock()
		return nil
	}
	if t.lock == nil {
		return fmt.Errorf("lock value was nil (not set)")
	}
	err = t.lock.Unlock()
	if err != nil {
		return err
	}
	return nil
}

// ClearTableCache - Clear the table cache.
func ClearTableCache() {

	tableCacheLock.Lock()
	defer tableCacheLock.Unlock()
	tableCache = make(map[string]*Table, 0)
}
