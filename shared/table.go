package shared

// Table metadata management functions.

import (
	"fmt"
	"github.com/araddon/qlbridge/value"
	"github.com/hashicorp/consul/api"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"strings"
)

// BasicTable - Table structure.
type BasicTable struct {
	Name             string                     `yaml:"tableName"`
	PrimaryKey       string                     `yaml:"primaryKey,omitempty"`
	SecondaryKeys    string                     `yaml:"secondaryKeys,omitempty"`
	DefaultPredicate string                     `yaml:"defaultPredicate,omitempty"`
	TimeQuantumType  string                     `yaml:"timeQuantumType,omitempty"`
	DisableDedup     bool                       `yaml:"disableDedup,omitempty"`
	Attributes       []BasicAttribute           `yaml:"attributes"`
	attributeNameMap map[string]*BasicAttribute `yaml:"-"`
	ConsulClient     *api.Client                `yaml:"-"`
}

// BasicAttribute - Field structure.
type BasicAttribute struct {
	Parent           *BasicTable       `yaml:"-" json:"-"`
	FieldName        string            `yaml:"fieldName"`
	SourceName       string            `yaml:"sourceName"`
	ChildTable       string            `yaml:"childTable,omitempty"`
	Type             string            `yaml:"type"`
	ForeignKey       string            `yaml:"foreignKey,omitempty"`
	MappingStrategy  string            `yaml:"mappingStrategy"`
	Size             int               `yaml:"maxLen,omitempty"`
	Ordinal          int               `yaml:"-"`
	Scale            int               `yaml:"scale,omitempty"`
	Values           []Value           `yaml:"values,omitempty"`
	MapperConfig     map[string]string `yaml:"configuration,omitempty"`
	Desc             string            `yaml:"desc,omitempty"`
	MinValue         int               `yaml:"minValue,omitempty"`
	MaxValue         int               `yaml:"maxValue,omitempty"`
	CallTransform    bool              `yaml:"callTransform,omitempty"`
	HighCard         bool              `yaml:"highCard,omitempty"`
	Required         bool              `yaml:"required,omitempty"`
	Searchable       bool              `yaml:"searchable,omitempty"`
	DefaultValue     string            `yaml:"defaultValue,omitempty"`
	ColumnID         bool              `yaml:"columnID,omitempty"`
	ColumnIDMSV      bool              `yaml:"columnIDMSV,omitempty"`
	IsTimeSeries     bool              `yaml:"isTimeSeries,omitempty"`
	TimeQuantumType  string            `yaml:"timeQuantumType,omitempty"`
	Exclusive        bool              `yaml:"exclusive,omitempty"`
	DelegationTarget string            `yaml:"delegationTarget,omitempty"`
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
	SEP = string(os.PathSeparator)
)

// LoadSchema - Load a new Table object from configuration.
func LoadSchema(path string, name string, consulClient *api.Client) (*BasicTable, error) {

	var table BasicTable
	if path != "" {
		b, err := ioutil.ReadFile(path + SEP + name + SEP + "schema.yaml")
		if err != nil {
			return nil, err
		}
		err2 := yaml.Unmarshal(b, &table)
		if err2 != nil {
			return nil, err2
		}
	} else { // load from Consul
		var err error
		table, err = UnmarshalConsul(consulClient, name)
		if err != nil {
			return nil, fmt.Errorf("Error UnmarshalConsul: %v", err)
		}
	}

	table.ConsulClient = consulClient
	table.attributeNameMap = make(map[string]*BasicAttribute)

	i := 1
	for j, v := range table.Attributes {

		table.Attributes[j].Parent = &table
		v.Parent = &table

		if v.SourceName == "" && v.FieldName == "" {
			return nil, fmt.Errorf("a valid attribute must have an input source name or field name.  Neither exists")
		}

		if v.MappingStrategy == "ParentRelation" {
			if v.ForeignKey == "" {
				return nil, fmt.Errorf("foreign key table name must be specified for %s", v.FieldName)
			}
			// Force field to be mapped by IntBSIMapper
			v.MappingStrategy = "IntBSI"
		}

		if v.FieldName != "" {
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
	return &table, nil
}

// GetAttribute - Get a tables attribute by name.
func (t *BasicTable) GetAttribute(name string) (*BasicAttribute, error) {

	if attr, ok := t.attributeNameMap[name]; ok {
		return attr, nil
	}
	return nil, fmt.Errorf("attribute '%s' not found", name)
}

// GetPrimaryKeyInfo - Return attributes for a given PK.
func (t *BasicTable) GetPrimaryKeyInfo() ([]*BasicAttribute, error) {
	s := strings.Split(t.PrimaryKey, "+")
	attrs := make([]*BasicAttribute, len(s))
	for i, v := range s {
		if attr, err := t.GetAttribute(strings.TrimSpace(v)); err == nil {
			attrs[i] = attr
		} else {
			return nil, err
		}
	}
	return attrs, nil
}

// IsBSI - Is this attribute a BSI?
func (a *BasicAttribute) IsBSI() bool {

	// TODO:  Add IsBSI() to Mapper interface and let mappers self describe
	switch a.MappingStrategy {
	case "IntBSI", "FloatScaleBSI", "SysMillisBSI", "SysMicroBSI", "SysSecBSI", "StringHashBSI", "CustomBSI", "ParentRelation":
		return true
	default:
		return false
	}
}
