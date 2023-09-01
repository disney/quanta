package shared

// Table metadata management functions.

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/disney/quanta/qlbridge/value"
	"github.com/hashicorp/consul/api"
	"gopkg.in/yaml.v2"
)

type TableInterface interface {
	// GetAttribute(name string) (AttributeInterface, error)
	// GetPrimaryKeyInfo() ([]AttributeInterface, error)
	Compare(other *BasicTable) (equal bool, warnings []string, err error)
	GetName() string
}

// BasicTable - Table structure.
type BasicTable struct {
	Name             string                     `yaml:"tableName"`
	PrimaryKey       string                     `yaml:"primaryKey,omitempty"`
	SecondaryKeys    string                     `yaml:"secondaryKeys,omitempty"`
	DefaultPredicate string                     `yaml:"defaultPredicate,omitempty"`
	TimeQuantumType  string                     `yaml:"timeQuantumType,omitempty"`
	TimeQuantumField string                     `yaml:"timeQuantumField,omitempty"`
	Selector         string                     `yaml:"selector,omitempty"`
	Attributes       []BasicAttribute           `yaml:"attributes"`
	attributeNameMap map[string]*BasicAttribute `yaml:"-"`
	ConsulClient     *api.Client                `yaml:"-"`
}

type AttributeInterface interface {
	GetParent() TableInterface
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

func (a *BasicAttribute) GetParent() TableInterface {
	return a.Parent
}
func (a *BasicTable) GetName() string {
	return a.Name
}

// func NewTableCacheStruct() *TableCacheStruct {
// 	tcs := &TableCacheStruct{}
// 	tcs.TableCache = make(map[string]TableInterface)
// 	return tcs
// }

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
				fmt.Errorf("A primary key field was defined but it is not valid field name(s) [%s] - %v",
					table.PrimaryKey, err)
		}
		timeQuantumField := strings.TrimSpace(table.TimeQuantumField)
		var timeQuantumAttr *BasicAttribute
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
	return &table, nil
}

// GetAttribute - Get a tables attribute by name.
func (t *BasicTable) GetAttribute(name string) (*BasicAttribute, error) {

	if t == nil {
		return nil, fmt.Errorf("assertion failure: receiver for table is nil for fieldname %s", name)
	}
	if t.attributeNameMap == nil {
		return nil, fmt.Errorf("assertion failure: attributeNameMap for table %s is nil for fieldname %s", t.Name, name)
	}
	if attr, ok := t.attributeNameMap[name]; ok {
		return attr, nil
	}
	return nil, fmt.Errorf("attribute '%s' not found", name)
}

// GetPrimaryKeyInfo - Return attributes for a given PK.
func (t *BasicTable) GetPrimaryKeyInfo() ([]*BasicAttribute, error) {

	s := strings.Split(t.PrimaryKey, "+")
	attrs := make([]*BasicAttribute, len(s))
	var v string
	i := 0
	if t.TimeQuantumField != "" {
		if len(s) > 0 {
			attrs = make([]*BasicAttribute, len(s)+1)
		} else {
			attrs = make([]*BasicAttribute, 1)
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

// Compare - This table's structure to another table.
func (t *BasicTable) Compare(other *BasicTable) (equal bool, warnings []string, err error) {

	warnings = make([]string, 0)

	if other == nil {
		return false, warnings, fmt.Errorf("comparison table must not be nil")
	}
	if t.Name != other.Name {
		return false, warnings, fmt.Errorf("table names differ existing = %s, new = %s", t.Name, other.Name)
	}
	if t.PrimaryKey != other.PrimaryKey {
		return false, warnings,
			fmt.Errorf("cannot alter PK existing = %s, other = %s", t.PrimaryKey, other.PrimaryKey)
	}
	if t.SecondaryKeys != other.SecondaryKeys {
		return false, warnings, fmt.Errorf("cannot alter SKs existing = %s, new = %s", t.SecondaryKeys,
			other.SecondaryKeys)
	}
	if t.TimeQuantumType != other.TimeQuantumType {
		return false, warnings,
			fmt.Errorf("Cannot alter time quantum existing = %s, new = %s", t.TimeQuantumType,
				other.TimeQuantumType)
	}
	if t.Selector != other.Selector {
		warnings = append(warnings, fmt.Sprintf("table selector changed existing = %v, new = %v",
			t.Selector, other.Selector))
	}

	// Compare these attributes against other attributes - drops not allowed.
	for _, v := range t.Attributes {
		otherAttr, err := other.GetAttribute(v.FieldName)
		if err != nil {
			return false, warnings, fmt.Errorf("attribute %s cannot be dropped", v.FieldName)
		}
		attrEqual, attrWarnings, attrErr := v.Compare(otherAttr)
		if attrErr != nil {
			return false, warnings, attrErr
		}
		if attrEqual {
			continue
		}
		warnings = append(warnings, attrWarnings...)
	}

	// Compare other attributes against these attributes - new adds allowed.
	for _, v := range other.Attributes {
		_, err := t.GetAttribute(v.FieldName)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("new attribute '%s', addition is allowable", v.FieldName))
		}
	}

	if len(warnings) == 0 {
		equal = true
	}
	return
}

// Compare - This attribute to another attribute.
func (a *BasicAttribute) Compare(other *BasicAttribute) (equal bool, warnings []string, err error) {

	warnings = make([]string, 0)

	// Check for error conditions first
	if a.Type != other.Type {
		return false, warnings, fmt.Errorf("attribute '%s' types differ existing = %s, new = %s", a.FieldName,
			a.Type, other.Type)
	}
	if a.ForeignKey != other.ForeignKey {
		if other.ForeignKey == "" {
			return false, warnings, fmt.Errorf("cannot drop foreign key constraint '%s' on attribute '%s'",
				a.ForeignKey, a.FieldName)
		}
		return false, warnings, fmt.Errorf("cannot add foreign key constraint '%s' on attribute '%s'",
			other.ForeignKey, a.FieldName)
	}
	if a.MappingStrategy != other.MappingStrategy {
		return false, warnings,
			fmt.Errorf("attribute '%s' mapping strategies differ existing = '%s', new = '%s'", a.FieldName,
				a.MappingStrategy, other.MappingStrategy)
	}
	if a.Scale != other.Scale {
		return false, warnings, fmt.Errorf("attribute '%s' scale differs existing = '%d', new = '%d'",
			a.FieldName, a.Scale, other.Scale)
	}
	if a.MinValue != other.MinValue {
		return false, warnings, fmt.Errorf("attribute '%s' min value differs existing = '%d', new = '%d'",
			a.FieldName, a.MinValue, other.MinValue)
	}
	if a.MaxValue != other.MaxValue {
		return false, warnings, fmt.Errorf("attribute '%s' max value differs existing = '%d', new = '%d'",
			a.FieldName, a.MaxValue, other.MaxValue)
	}
	if a.Searchable != other.Searchable {
		return false, warnings, fmt.Errorf("attribute '%s' searchability differs existing = '%v', new = '%v'",
			a.FieldName, a.Searchable, other.Searchable)
	}
	if a.Required != other.Required {
		return false, warnings, fmt.Errorf("attribute '%s' required differs existing = '%v', new = '%v'",
			a.FieldName, a.Required, other.Required)
	}
	if a.Exclusive != other.Exclusive {
		return false, warnings, fmt.Errorf("attribute '%s' exclusivity differs existing = '%v', new = '%v'",
			a.FieldName, a.Exclusive, other.Exclusive)
	}

	// Warning level comparisons for alters that are allowed.
	if a.SourceName != other.SourceName {
		warnings = append(warnings, fmt.Sprintf("attribute '%s' source name changed existing = '%v', new = '%v'",
			a.FieldName, a.SourceName, other.SourceName))
	}
	if a.Desc != other.Desc {
		warnings = append(warnings, fmt.Sprintf("attribute '%s' description changed existing = '%v', new = '%v'",
			a.FieldName, a.Desc, other.Desc))
	}
	if a.DefaultValue != other.DefaultValue {
		warnings = append(warnings, fmt.Sprintf("attribute '%s' default val changed existing = '%v', new = '%v'",
			a.FieldName, a.DefaultValue, other.DefaultValue))
	}
	if a.ChildTable != other.ChildTable {
		warnings = append(warnings, fmt.Sprintf("attribute '%s' child changed existing = '%v', new = '%v'",
			a.FieldName, a.ChildTable, other.ChildTable))
	}
	if len(a.Values) != len(other.Values) {
		warnings = append(warnings, fmt.Sprintf("attribute '%s' enum count changed existing = '%v', new = '%v'",
			a.FieldName, len(a.Values), len(other.Values)))
	}
	if len(a.MapperConfig) != len(other.MapperConfig) {
		warnings = append(warnings, fmt.Sprintf("attribute '%s' mapper conf changed existing = '%v', new = '%v'",
			a.FieldName, len(a.MapperConfig), len(other.MapperConfig)))
	}

	if len(warnings) == 0 {
		equal = true
	}
	return
}
