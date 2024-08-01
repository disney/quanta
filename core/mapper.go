package core

// Mapper interfaces

import (
	"fmt"
	hash "github.com/aviddiviner/go-murmur"
	"strings"
)

// MapperType - Mapper typedef
type MapperType int

// DefaultMapper - Base functionality
type DefaultMapper struct {
	MapperType
}

// Mapper - Mapper interface.   MapValue is the only required method.
type Mapper interface {
	Transform(attr *Attribute, val interface{}, c *Session) (newVal interface{}, err error)
	MapValue(attr *Attribute, val interface{}, c *Session, isUpdate bool) (result uint64, err error)
	MapValueReverse(attr *Attribute, id uint64, c *Session) (result interface{}, err error)
	GetMultiDelimiter() string
}

// MapperFactory for creating mapper instances.
type MapperFactory func(conf map[string]string) (Mapper, error)

var (
	mapperFactories = make(map[string]MapperFactory)
)

const (
	undefined = MapperType(iota)
	// BoolDirect - RowID false = 1, true = 2
	BoolDirect

	// IntToBoolDirect - Value is 0 or 1, Transform to boolean, Map to rowID 1 = false, 2 = true
	IntToBoolDirect

	// IntDirect - Value is rowID
	IntDirect

	// IntLinear - Linear buckets (Min, Max, Res)
	IntLinear

	// IntBuckets - Variable buckets ([]int) for logarithmic, etc.
	IntBuckets

	// IntBSI - BSI range encoding
	IntBSI

	// FloatScaleBSI - Converts float to int by scaling by fractional part -> BSI range encoding
	FloatScaleBSI

	// FloatLinear -  Linear buckets (Min, Max, Res)
	FloatLinear

	// FloatBuckets - Variable buckets ([]float64) for logarithmic, etc.
	FloatBuckets

	// YearToDay - Date/Datetime day granularity
	YearToDay

	// YearToMonth - Date/Datetime month granularity
	YearToMonth

	// Year = Date/Datetime year granularity
	Year

	// DayOfYear - Date/Datetime integer day in year granularity
	DayOfYear

	// DayOfMonth - Date/Datetime integer day in month granularity
	DayOfMonth

	// DayOfWeek - Date/Datetime integer day of week granularity
	DayOfWeek

	// TimeOfDay - Datetime time of day seconds granularity
	TimeOfDay

	// HourOfDay - Datetime hour in day
	HourOfDay

	// SysMillisBSI - Datetime to BSI range milliseconds granularity
	SysMillisBSI

	// SysMicroBSI - Datetime to BSI range microseconds granularity
	SysMicroBSI

	// SysSecBSI - Datetime to BSI range seconds granularity
	SysSecBSI

	// StringEnum - Direct mapping of set of enumerated strings to rowID (<500 cardinality)
	StringEnum

	// StringHashBSI - Hash string to rowID (>500 cardinality)
	StringHashBSI

	// StringToIntDirect - Parse string to int and use directly
	StringToIntDirect

	// BoolRegex - Map string to a boolean value via a pattern
	BoolRegex

	// Contains - Map string to an array of contained strings (non overlapping)
	Contains

	// Custom - Custom mapper plugin
	Custom

	// CustomBSI - Custom mapper plugin for BSI fields
	CustomBSI
)

// Transform - Default transformer just passes the input value as return argument
func (dm DefaultMapper) Transform(attr *Attribute, val interface{}, c *Session) (newVal interface{}, err error) {
	newVal = val
	return
}

// MapValueReverse - Default reverse mapper throws not implemented error
func (dm DefaultMapper) MapValueReverse(attr *Attribute, id uint64, c *Session) (result interface{}, err error) {
	err = fmt.Errorf("MapValueReverse - Not implemented for this mapper [%s]", dm.String())
	return
}

// GetMultiDelimiter - Default delimiter is an empty string
func (dm DefaultMapper) GetMultiDelimiter() string {
	return ""
}

// String - Returns a string representation of MapperType.
func (mt MapperType) String() string {

	switch mt {
	case BoolDirect:
		return "BoolDirect"
	case IntDirect:
		return "IntDirect"
	case IntToBoolDirect:
		return "IntToBoolDirect"
	case IntLinear:
		return "IntLinear"
	case IntBuckets:
		return "IntBuckets"
	case IntBSI:
		return "IntBSI"
	case FloatScaleBSI:
		return "FloatScaleBSI"
	case FloatLinear:
		return "FloatLinear"
	case FloatBuckets:
		return "FloatBuckets"
	case YearToDay:
		return "YearToDay"
	case YearToMonth:
		return "YearToMonth"
	case Year:
		return "Year"
	case DayOfYear:
		return "DayOfYear"
	case DayOfMonth:
		return "DayOfMonth"
	case DayOfWeek:
		return "DayOfWeek"
	case TimeOfDay:
		return "TimeOfDay"
	case HourOfDay:
		return "HourOfDay"
	case SysMillisBSI:
		return "SysMillisBSI"
	case SysMicroBSI:
		return "SysMicroBSI"
	case SysSecBSI:
		return "SysSecBSI"
	case StringEnum:
		return "StringEnum"
	case StringHashBSI:
		return "StringHashBSI"
	case StringToIntDirect:
		return "StringToIntDirect"
	case BoolRegex:
		return "BoolRegex"
	case Contains:
		return "Contains"
	case Custom:
		return "Custom"
	case CustomBSI:
		return "CustomBSI"
	default:
		return "Undefined"
	}
}

// MapperTypeFromString - Construct a MapperType from string representation.
func MapperTypeFromString(mt string) MapperType {

	switch mt {
	case "BoolDirect":
		return BoolDirect
	case "IntDirect":
		return IntDirect
	case "IntToBoolDirect":
		return IntToBoolDirect
	case "IntLinear":
		return IntLinear
	case "IntBuckets":
		return IntBuckets
	case "IntBSI":
		return IntBSI
	case "FloatScaleBSI":
		return FloatScaleBSI
	case "FloatLinear":
		return FloatLinear
	case "FloatBuckets":
		return FloatBuckets
	case "YearToDay":
		return YearToDay
	case "YearToMonth":
		return YearToMonth
	case "Year":
		return Year
	case "DayOfYear":
		return DayOfYear
	case "DayOfMonth":
		return DayOfMonth
	case "DayOfWeek":
		return DayOfWeek
	case "TimeOfDay":
		return TimeOfDay
	case "HourOfDay":
		return HourOfDay
	case "SysMillisBSI":
		return SysMillisBSI
	case "SysMicroBSI":
		return SysMicroBSI
	case "SysSecBSI":
		return SysSecBSI
	case "StringEnum":
		return StringEnum
	case "StringHashBSI":
		return StringHashBSI
	case "BoolRegex":
		return BoolRegex
	case "Contains":
		return Contains
	case "CustomBSI":
		return CustomBSI
	default:
		return undefined
	}
}

// MapValue - Map a value to a row id for standard bitmaps or an int64
func (mt MapperType) MapValue(attr *Attribute, val interface{}, c *Session, isUpdate bool) (result uint64, err error) {
	return attr.MapValue(val, c, isUpdate)
}

// IsBSI - Is this mapper for BSI types?
func (mt MapperType) IsBSI() bool {
	switch mt {
	case IntBSI, FloatScaleBSI, SysMillisBSI, SysMicroBSI, SysSecBSI, StringHashBSI, CustomBSI:
		return true
	default:
		return false
	}
}

// MutateBitmap - Mutate bitmap.
func (mt MapperType) MutateBitmap(c *Session, table, field string, mval interface{}, isUpdate bool) (err error) {

	//TIME_FMT := "2006-01-02T15"
	tbuf, ok := c.TableBuffers[table]
	if !ok {
		return fmt.Errorf("table %s invalid or not opened", table)
	}
	if !mt.IsBSI() {
		var val uint64
		switch mval.(type) {
		case uint64:
			val = mval.(uint64)
		case nil:
			err = c.BatchBuffer.ClearBit(table, field, tbuf.CurrentColumnID, val, tbuf.CurrentTimestamp)
			return
		default:
			return fmt.Errorf("MutateBitmap unknown type : %T for val %v", mval, mval)
		}
		//fmt.Printf("SETBIT %s [%s] COLID =  %d TS = %s\n", table, field, tbuf.CurrentColumnID,
		//    tbuf.CurrentTimestamp.Format(TIME_FMT))


		if at, err := tbuf.Table.GetAttribute(field); err == nil {
			if at.NonExclusive {
				isUpdate = false
			}
		}
		err = c.BatchBuffer.SetBit(table, field, tbuf.CurrentColumnID, val, tbuf.CurrentTimestamp, isUpdate)
	} else {
		var val int64
		switch mval.(type) {
		case uint64:
			val = int64(mval.(uint64))
		case int64:
			val = mval.(int64)
		case nil:
			err = c.BatchBuffer.ClearValue(table, field, tbuf.CurrentColumnID, tbuf.CurrentTimestamp)
			return
		default:
			err = fmt.Errorf("MutateBitmap unknown type : %T for val %v", mval, mval)
			return
		}
		//fmt.Printf("SETVALUE %s [%s] COLID =  %d TS = %s\n", table, field, tbuf.CurrentColumnID,
		//    tbuf.CurrentTimestamp.Format(TIME_FMT))
		err = c.BatchBuffer.SetValue(table, field, tbuf.CurrentColumnID, val, tbuf.CurrentTimestamp)
	}
	return
}

// ResolveMapper - Resolve the mapper type for an attribute
func ResolveMapper(attr *Attribute) (mapper Mapper, err error) {

	if attr.MappingStrategy == "" {
		return nil, fmt.Errorf("MappingStrategy is nil for '%s'", attr.FieldName)
	}

	if attr.MappingStrategy == "Custom" || attr.MappingStrategy == "CustomBSI" {
		if attr.MapperConfig != nil {
			if name, ok := attr.MapperConfig["name"]; ok {
				mapper, err = lookupMapper(name, attr.MapperConfig)
			}
		}
	} else if attr.MappingStrategy == "ParentRelation" {
		mapper, err = lookupMapper("IntBSI", attr.MapperConfig)
	} else {
		mapper, err = lookupMapper(attr.MappingStrategy, attr.MapperConfig)
	}
	return
}

func lookupMapper(mapperName string, conf map[string]string) (Mapper, error) {

	mapperFactory, ok := mapperFactories[mapperName]
	if !ok {
		// Factory has not been registered.
		// Make a list of all available mapper factories for logging.
		availableMapperTypes := make([]string, len(mapperFactories))
		for k := range mapperFactories {
			availableMapperTypes = append(availableMapperTypes, k)
		}
		return nil, fmt.Errorf("invalid MapperType '%s'. Must be one of: %s", mapperName, strings.Join(availableMapperTypes, ", "))
	}

	// Run the factory with the configuration.
	mapper, err := mapperFactory(conf)
	if err != nil {
		return nil, err
	}
	return mapper, nil

}

// Register a mapper factory.
func Register(name string, factory MapperFactory) {

	if factory == nil {
		panic(fmt.Sprintf("MapperType factory %s does not exist.", name))
	}
	_, registered := mapperFactories[name]
	if registered {
		//log.Printf("MapperType factory '%s' already registered. Ignoring.", name)
		return
	}
	mapperFactories[name] = factory
}

// Register builtins
func init() {
	Register(StringHashBSI.String(), NewStringHashBSIMapper)
	Register(IntDirect.String(), NewIntDirectMapper)
	Register(IntToBoolDirect.String(), NewIntToBoolDirectMapper)
	Register(StringToIntDirect.String(), NewStringToIntDirectMapper)
	Register(BoolDirect.String(), NewBoolDirectMapper)
	Register(FloatScaleBSI.String(), NewFloatScaleBSIMapper)
	Register(IntBSI.String(), NewIntBSIMapper)
	Register(StringEnum.String(), NewStringEnumMapper)
	Register(BoolRegex.String(), NewBoolRegexMapper)
	Register(SysMillisBSI.String(), NewSysMillisBSIMapper)
	Register(SysMicroBSI.String(), NewSysMicroBSIMapper)
	Register(SysSecBSI.String(), NewSysSecBSIMapper)
}

// Get64BitHash - Hash a string.
func Get64BitHash(s string) uint64 {
	//return hash.MurmurHash64A([]byte(s), 0)
	return uint64(hash.MurmurHash2([]byte(s), 0))
}
