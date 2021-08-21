package shared

// Server side implementation of table.  TODO, merge this with core.Table to eliminate duplication.

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/consul/api"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"sync"
)

// Table - Table structure.
type Table struct {
	Name             string                `yaml:"tableName"`
	PrimaryKey       string                `yaml:"primaryKey,omitempty"`
	DefaultPredicate string                `yaml:"defaultPredicate,omitempty"`
	UseColumnKey     bool                  `yaml:"useColumnKey,omitempty"`
	TimeQuantumType  string                `yaml:"timeQuantumType,omitempty"`
	Attributes       []Attribute           `yaml:"attributes"`
	attributeNameMap map[string]*Attribute `yaml:"-"`
	ConsulClient     *api.Client           `yaml:"-"`
	lock             *api.Lock             `yaml:"-"`
}

// Attribute - Field structure.
type Attribute struct {
	Parent          *Table  `yaml:"-" json:"-"`
	SourceName      string  `yaml:"sourceName"`
	FieldName       string  `yaml:"fieldName"`
	Type            string  `yaml:"type"`
	ForeignKey      string  `yaml:"foreignKey,omitempty"`
	MappingStrategy string  `yaml:"mappingStrategy"`
	Size            int     `yaml:"maxLen,omitempty"`
	Ordinal         int     `yaml:"-"`
	Scale           int     `yaml:"scale,omitempty"`
	Values          []Value `yaml:"values,omitempty"`
	//MapperConfig     map[string]string `yaml:"configuration,omitempty"`
	Desc          string                `yaml:"desc,omitempty"`
	MinValue      int                   `yaml:"minValue,omitempty"`
	MaxValue      int                   `yaml:"maxValue,omitempty"`
	CallTransform bool                  `yaml:"callTransform,omitempty"`
	HighCard      bool                  `yaml:"highCard"`
	SkipIndex     bool                  `yaml:"skipIndex,omitempty"`
	Required      bool                  `yaml:"required,omitempty"`
	Searchable    bool                  `yaml:"searchable,omitempty"`
	DefaultValue  string                `yaml:"defaultValue,omitempty"`
	valueMap      map[interface{}]int64 `yaml:"-"`
	reverseMap    map[int64]interface{} `yaml:"-" json:"-"`
	//mapperInstance   Mapper  `yaml:"-"`
	ColumnID         bool   `yaml:"columnID,omitempty"`
	ColumnIDMSV      bool   `yaml:"columnIDMSV,omitempty"`
	ColumnKey        bool   `yaml:"columnKey,omitempty"`
	IsTimeSeries     bool   `yaml:"isTimeSeries,omitempty"`
	TimeQuantumType  string `yaml:"timeQuantumType,omitempty"`
	Exclusive        bool   `yaml:"exclusive,omitempty"`
	DelegationTarget string `yaml:"delegationTarget,omitempty"`
}

// Value - Metadata value items for StringEnum mapper type.
type Value struct {
	Value interface{} `yaml:"value" json:"value"`
	RowID int64       `yaml:"rowID" json:"rowID"`
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

/*
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

type RowStatus byte

const (
	Ok      = RowStatus(iota)
	Deleted // Row deleted from index
	Error   // Load process failed to add to index
)
*/

var (
	fieldMap     map[string]*Field
	fieldMapLock sync.Mutex
)

// LoadSchema - Load a new Table object from configuration.
func LoadSchema(path, name string, consulClient *api.Client) (*Table, error) {

	b, err := ioutil.ReadFile(path + string(os.PathSeparator) + name +
		string(os.PathSeparator) + "schema.yaml")
	if err != nil {
		return nil, err
	}
	var table Table
	err2 := yaml.Unmarshal(b, &table)
	if err2 != nil {
		return nil, err2
	}

	table.ConsulClient = consulClient

	table.attributeNameMap = make(map[string]*Attribute)
	if err := table.Lock(); err != nil {
		return nil, err
	}

	defer table.Unlock()
	/*
	   var err3 error
	   if table.MetadataEndpoint != "" && fieldMap == nil {
	       if err3, fieldMap = LoadFieldValues(table.MetadataEndpoint); err3 != nil {
	           return nil, err3
	       }
	   }
	*/

	i := 1
	for j, v := range table.Attributes {

		table.Attributes[j].Parent = &table

		if v.SourceName == "" && v.FieldName == "" {
			return nil, fmt.Errorf("a valid attribute must have an input source name or field name.  Neither exists")
		}

		// Register a plugin if present
		/*
		   if v.MappingStrategy == "Custom" || v.MappingStrategy == "CustomBSI" {
		       if v.MapperConfig == nil {
		           return nil, fmt.Errorf("Custom plugin configuration missing.")
		       }
		       if pname, ok := v.MapperConfig["name"]; !ok {
		           return nil, fmt.Errorf("Custom plugin name not specified.")
		       } else if plugPath, ok := v.MapperConfig["plugin"]; !ok {
		           return nil, fmt.Errorf("Custom plugin SO name not specified.")
		       } else {
		           plug, err := plugin.Open(plugPath + ".so")
		           if err != nil {
		               return nil, fmt.Errorf("Cannot open '%s' %v\n", plugPath, err)
		           }
		           symFactory, err := plug.Lookup("New" + pname)
		           if err != nil {
		               return nil, fmt.Errorf("New" + pname + "%v", err)
		           }
		           factory, ok := symFactory.(func(map[string]string) (Mapper, error))
		           if !ok {
		               return nil, fmt.Errorf("Unexpected type from module symbol New%s\n", pname)
		           }
		           Register(pname, factory)
		       }
		   }

		   if table.Attributes[j].mapperInstance, err = ResolveMapper(&v); err != nil {
		       return nil, err
		   }
		*/

		if v.FieldName != "" {

			// check to see if there are values in the API call (if applicable)
			//if x, ok := fieldMap[table.Name + "_" + v.FieldName]; ok {

			// if there are values in schema.yaml then override metadata values in global cache
			if f, ok := fieldMap[v.FieldName]; ok && len(table.Attributes[j].Values) > 0 {
				// Pull it in
				values := make([]FieldValue, 0)
				for _, x := range table.Attributes[j].Values {
					values = append(values, FieldValue{Mapping: x.Value.(string), Value: int(x.RowID),
						Label: x.Value.(string)})
				}
				f.Values = values
			}

			// Dont allow metadata values to override local cache
			if x, ok := fieldMap[v.FieldName]; ok && len(table.Attributes[j].Values) == 0 {
				var values []Value = make([]Value, 0)
				for _, z := range x.Values {
					values = append(values, Value{Value: z.Mapping, RowID: int64(z.Value), Desc: z.Label})
				}
				table.Attributes[j].Values = values
			}

			// check to see if there is an external json values file and load it
			if x, err3 := ioutil.ReadFile(path + string(os.PathSeparator) + name +
				string(os.PathSeparator) + v.FieldName + ".json"); err3 == nil {
				var values []Value
				if err4 := json.Unmarshal(x, &values); err4 == nil {
					table.Attributes[j].Values = values
				}
			}

			table.attributeNameMap[v.FieldName] = &table.Attributes[j]
		}

		if v.FieldName == "" {
			v.FieldName = v.SourceName
			table.attributeNameMap[v.SourceName] = &table.Attributes[j]
		}

		// Enable lookup by alias (field name)
		if v.SourceName != v.FieldName {
			table.attributeNameMap[v.FieldName] = &table.Attributes[j]
		}
		if len(table.Attributes[j].Values) > 0 {
			table.Attributes[j].valueMap = make(map[interface{}]int64)
			table.Attributes[j].reverseMap = make(map[int64]interface{})
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
			return nil, fmt.Errorf("time quantums enabled for PK %s, Type must be Date or DateTime",
				pka[0].FieldName)
		}
	}
	return &table, nil
}

// GetAttribute - Get a tables attribute by name.
func (t *Table) GetAttribute(name string) (*Attribute, error) {

	if attr, ok := t.attributeNameMap[name]; ok {
		return attr, nil
	}
	return nil, fmt.Errorf("attribute '%s' not found", name)
}

// GetForeignKeyInfo - Return attributes for a given FK.
func (t *Table) GetForeignKeyInfo(relation string) ([]*Attribute, error) {
	attrs := make([]*Attribute, 0)
	for _, v := range t.Attributes {
		if v.ForeignKey == relation {
			attrs = append(attrs, &v)
		}
	}
	if len(attrs) == 0 {
		return nil, fmt.Errorf("Cannot locate foreign key Attribute(s) for %s", relation)
	}
	return attrs, nil
}

// GetPrimaryKeyInfo - Return attributes for a given PK.
func (t *Table) GetPrimaryKeyInfo() ([]*Attribute, error) {
	s := strings.Split(t.PrimaryKey, "+")
	attrs := make([]*Attribute, len(s))
	for i, v := range s {
		if attr, err := t.GetAttribute(v); err == nil {
			attrs[i] = attr
		} else {
			return nil, err
		}
	}
	return attrs, nil
}

// GetValue - Return row ID for a given input value (StringEnum).
func (a *Attribute) GetValue(invalue interface{}) (int64, error) {

	value := invalue
	switch invalue.(type) {
	case string:
		value = strings.TrimSpace(invalue.(string))
	}
	var v int64
	var ok bool
	if v, ok = a.valueMap[value]; !ok {
		/* If the value does not exist in the valueMap local cache  we will add it and then
		 *  Call the metadata service to add it.
		 */
		if err := a.Parent.Lock(); err != nil {
			return 0, err
		}
		defer a.Parent.Unlock()
		// Check if in global cache
		if f, ok2 := fieldMap[a.FieldName]; ok2 {
			// Pull it in
			for _, x := range f.Values {
				if x.Mapping == value {
					a.Values = append(a.Values, Value{Value: value, RowID: int64(x.Value)})
					a.valueMap[value] = int64(x.Value)
					a.reverseMap[int64(x.Value)] = value
					v = int64(x.Value)
					return v, nil // Done
				}
			}
		} else {
			return 0, fmt.Errorf("ERROR: Field %s does not exist in metadata", a.FieldName)
		}

		/*
		   if a.Parent.MetadataEndpoint != "" {

		       // If not in global cache then check service in case recently added (inside lock).
		       if err, fldMap := LoadFieldValues(a.Parent.MetadataEndpoint); err != nil {
		           return 0, err
		       } else {
		           if x, ok := fldMap[a.FieldName]; ok {
		               for _, z := range x.Values {
		                   if z.Mapping == value {
		                       // If the value was found then refresh the caches and return the ID.
		                       fieldMap = fldMap        // Global cache updated

		                       // Update local cache
		                       val :=int64(z.Value)
		                       a.Values = append(a.Values, Value{Value: value, RowID: val})
		                       a.valueMap[value] = val
		                       a.reverseMap[val] = value

		                       return val, nil
		                   }
		               }
		           }
		       }

		       // OK, value not anywhere to be found, invoke service to add.
		       log.Printf("Invoking metadata endpoint %s for field = %s, value = %v",
		           a.Parent.MetadataEndpoint, a.FieldName, value)
		       if err, val := AddFieldValue(a.Parent.MetadataEndpoint, a.FieldName, value); err != nil {
		           return 0, err
		       } else {
		           v = val
		           a.Values = append(a.Values, Value{Value: value, RowID: val})
		           a.valueMap[value] = val
		           a.reverseMap[val] = value
		       }
		   } else {
		       return 0, fmt.Errorf("Attribute %s - Cannot locate rowID for value '%v'", a.SourceName, value)
		   }
		*/
	}
	return v, nil
}

// GetValueForID - Reverse map a value for a given row ID.  (StringEnum)
func (a *Attribute) GetValueForID(id int64) (interface{}, error) {

	if v, ok := a.reverseMap[id]; ok {
		return v, nil
	}
	return 0, fmt.Errorf("Attribute %s - Cannot locate value for rowID '%v'", a.SourceName, id)
}

/*
func (a *Attribute) Transform(val interface{}, c *Connection) (newVal interface{}, err error) {

    if a.mapperInstance == nil {
        return 0, fmt.Errorf("Attribute '%s' MapperInstance is nil .", a.FieldName)
    }
    return a.mapperInstance.Transform(a, val, c)
}

// MapValue - Return the row ID for a given value (Standard Bitmap)
func (a *Attribute) MapValue(val interface{}, c *Connection) (result int64, err error) {

    if a.mapperInstance == nil {
        return 0, fmt.Errorf("Attribute '%s' MapperInstance is nil .", a.FieldName)
    }
    return a.mapperInstance.MapValue(a, val, c)
}

// MapValueReverse - Re-hydrate the original value for a given row ID.
func (a *Attribute) MapValueReverse(id int64, c *Connection) (result interface{}, err error) {

    if a.mapperInstance == nil {
        return 0, fmt.Errorf("Attribute '%s' MapperInstance is nil .", a.FieldName)
    }
    return a.mapperInstance.MapValueReverse(a, id, c)
}
*/

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
	Value   int    `json:value`
	Mapping string `json:mapping`
}

/*
//
// AddFieldValue - Add a new field value.
// NOTE: Must be called inside a mutex/concurrency guard.
//
//
func AddFieldValue(endPoint, fieldName string, value interface{}) (err error, rowID int64) {

    f := map[string]interface{}{"fieldName": fieldName, "label": value, "mapping": value}
    s, _ := json.Marshal(f)
    req, err := http.NewRequest(http.MethodPost, endPoint + "/fieldvalues", bytes.NewBuffer(s))
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("Accept", "application/json")
    req.Header.Set("Cache-Control", "no-cache")

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        return
    }
    defer resp.Body.Close()

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return err, 0
    }

    if resp.StatusCode != http.StatusCreated {
        log.Println("response Status:", resp.Status)
        log.Println("response Headers:", resp.Header)
        log.Println("response Body:", string(body))
        return fmt.Errorf("ERROR %v\n", string(body)), 0
    }

    var fv FieldValue
    jsonErr := json.Unmarshal(body, &fv)
    if jsonErr != nil {
        return jsonErr, 0
    }

    field, ok := fieldMap[fieldName]
    if !ok {
        return fmt.Errorf("ERROR: Field %s does not exist in metadata!", fieldName), 0
    }
    field.Values = append(field.Values, FieldValue{Mapping: value.(string), Value: fv.Value, Label: value.(string)})
    rowID = int64(fv.Value)
    return
}

func LoadFieldValues(endPoint string) (err error, fieldMap map[string]*Field) {

    var attributeFieldMap map[string]*Field = make(map[string]*Field)

    httpClient := http.Client{}
    req, err := http.NewRequest(http.MethodGet, endPoint + "/fields", nil)
    if err != nil {
        return err, nil
    }
    res, err := httpClient.Do(req)
    if err != nil {
        return err, nil
    }
    defer res.Body.Close()
    body, err := ioutil.ReadAll(res.Body)
    if err != nil {
        return err, nil
    }

    var fields []Field
    jsonErr := json.Unmarshal(body, &fields)
    if jsonErr != nil {
        return jsonErr, nil
    }

    for i, v := range fields {
        attributeFieldMap[v.Name] = &fields[i]
    }

    return nil, attributeFieldMap
}
*/

// Lock the table.
func (t *Table) Lock() error {

	var err error
	// If Consul client is not set then we are not running in distributed mode.  Use local mutex.
	if t.ConsulClient == nil {
		fieldMapLock.Lock()
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
		fieldMapLock.Unlock()
		return nil
	}
	if t.lock == nil {
		return fmt.Errorf("Lock value was nil (not set)")
	}
	err = t.lock.Unlock()
	if err != nil {
		return err
	}
	return nil
}
