package core

//
// This file defines all of the built in mapping functions.
//

import (
	"fmt"
	"github.com/araddon/dateparse"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// StringHashBSIMapper - High cardinality string mapping.
type StringHashBSIMapper struct {
	DefaultMapper
}

// NewStringHashBSIMapper - Construct a High cardinality, partitioned string mapping.  Strings are stored on the same
// node as their corresponding BSI.
func NewStringHashBSIMapper(conf map[string]string) (Mapper, error) {
	return StringHashBSIMapper{DefaultMapper: DefaultMapper{StringHashBSI}}, nil
}

// MapValue - Map a string value to an int64
func (m StringHashBSIMapper) MapValue(attr *Attribute, val interface{}, c *Session) (result uint64, err error) {

	var strVal string
	switch val.(type) {
	case string:
		strVal = val.(string)
		if strVal != "" {
			result = Get64BitHash(strVal)
		}
	default:
		err = fmt.Errorf("StringHashBSIMapper not expecting a '%T' for '%s'", val, attr.FieldName)
		return
	}

	if c != nil {
		if tbuf, ok := c.TableBuffers[attr.Parent.Name]; ok {
			if attr.Searchable {
				err = c.StringIndex.Index(strVal)
			}
			stringPath := indexPath(tbuf, attr.FieldName, "strings")    // indexPath() is in core/session.go
			c.BatchBuffer.SetPartitionedString(stringPath, tbuf.CurrentColumnID, val)
			err = m.UpdateBitmap(c, attr.Parent.Name, attr.FieldName, result, attr.IsTimeSeries)
		} else {
			err = fmt.Errorf("table %s not open for this connection", attr.Parent.Name)
		}
	}
	return
}

// BoolDirectMapper - Map values 0/1 to true/false rowID 0 = false, rowID 1 = true
type BoolDirectMapper struct {
	DefaultMapper
}

// NewBoolDirectMapper - Construct a new BoolDirectMapper
func NewBoolDirectMapper(conf map[string]string) (Mapper, error) {
	return BoolDirectMapper{DefaultMapper{BoolDirect}}, nil
}

// MapValue - Map boolean values true/false to rowid = 0 false rowid = 1 true
func (m BoolDirectMapper) MapValue(attr *Attribute, val interface{},
	c *Session) (result uint64, err error) {

	result = uint64(0)
	switch val.(type) {
	case bool:
		if val.(bool) == true {
			result = uint64(1)
		}
	case string:
		str := strings.TrimSpace(val.(string))
		if str == "" || strings.ContainsAny(str, `Nn`) {
			result = uint64(0)
			break
		}
		if strings.ContainsAny(str, `Yy`) {
			result = uint64(1)
			break
		}
		v, e := strconv.ParseBool(str)
		if e != nil {
			err = e
			result = uint64(0)
			return
		}
		if v {
			result = uint64(1)
		}
	case int:
		if val.(int) == 1 {
			result = uint64(1)
		}
	case int64:
		if val.(int64) == 1 {
			result = uint64(1)
		}
	default:
		err = fmt.Errorf("%v: No handling for type '%T'", val, val)
		return
	}
	if c != nil {
		err = m.UpdateBitmap(c, attr.Parent.Name, attr.FieldName, result, attr.IsTimeSeries)
	}
	return
}

// MapValueReverse - Map a row ID back to original value (true/false)
func (m BoolDirectMapper) MapValueReverse(attr *Attribute, id uint64, c *Session) (result interface{}, err error) {
	result = false
	if id == 1 {
		result = true
	}
	return
}

// IntDirectMapper - Take the input integer value and use it as the row ID.
type IntDirectMapper struct {
	DefaultMapper
}

// NewIntDirectMapper - Construct a new IntDirectMapper
func NewIntDirectMapper(conf map[string]string) (Mapper, error) {
	return IntDirectMapper{DefaultMapper{IntDirect}}, nil
}

// MapValue - Map a value to a row ID.
func (m IntDirectMapper) MapValue(attr *Attribute, val interface{},
	c *Session) (result uint64, err error) {

	switch val.(type) {
	case uint64:
		result = uint64(val.(uint64))
	case int64:
		result = uint64(val.(int64))
	case uint32:
		result = uint64(val.(uint32))
	case int32:
		result = uint64(val.(int32))
	case int:
		result = uint64(val.(int))
	case string:
		str := strings.TrimSpace(val.(string))
		if str == "" {
			str = "0"
		}
		v, e := strconv.ParseInt(str, 10, 64)
		if e != nil {
			err = e
			result = uint64(0)
			return
		}
		if v <= 0 {
			err = fmt.Errorf("cannot map %d as a positive non-zero value", v)
			return
		}
		result = uint64(v)
	default:
		return
	}
	if c != nil {
		err = m.UpdateBitmap(c, attr.Parent.Name, attr.FieldName, result, attr.IsTimeSeries)
	}
	return
}

// MapValueReverse - Map a row ID back to original value (row ID value taken literally)
func (m IntDirectMapper) MapValueReverse(attr *Attribute, id uint64, c *Session) (result interface{}, err error) {
	result = int64(id)
	return
}

// StringToIntDirectMapper - Maps a string containing a number directly as a row ID.
type StringToIntDirectMapper struct {
	DefaultMapper
}

// NewStringToIntDirectMapper - Construct a new mapper
func NewStringToIntDirectMapper(conf map[string]string) (Mapper, error) {
	return StringToIntDirectMapper{DefaultMapper{IntDirect}}, nil
}

// MapValue - Map a value to a row ID.
func (m StringToIntDirectMapper) MapValue(attr *Attribute, val interface{},
	c *Session) (result uint64, err error) {

	var v int64
	v, err = strconv.ParseInt(strings.TrimSpace(val.(string)), 10, 64)
	if err == nil && c != nil {
		err = m.UpdateBitmap(c, attr.Parent.Name, attr.FieldName, result, attr.IsTimeSeries)
	}
	if v <= 0 {
		err = fmt.Errorf("cannot map %d as a positive non-zero value", v)
		return
	}
	result = uint64(v)
	return
}

// FloatScaleBSIMapper - Maps floating point values by scaling an integer BSI.
type FloatScaleBSIMapper struct {
	DefaultMapper
}

// NewFloatScaleBSIMapper - Constructs a floating point mapper.
func NewFloatScaleBSIMapper(conf map[string]string) (Mapper, error) {
	return FloatScaleBSIMapper{DefaultMapper{FloatScaleBSI}}, nil
}

// MapValue - Map a value to an int64.
func (m FloatScaleBSIMapper) MapValue(attr *Attribute, val interface{},
	c *Session) (result uint64, err error) {

	var floatVal float64
	switch val.(type) {
	case float64:
		floatVal = val.(float64)
	case float32:
		floatVal = float64(val.(float32))
	case uint64:
		floatVal = float64(val.(uint64))
	case int64:
		floatVal = float64(val.(int64))
	case string:
		str := strings.TrimSpace(val.(string))
		if str == "" {
			str = "0.0"
		}
		floatVal, err = strconv.ParseFloat(str, 64)
		if err != nil {
			return
		}
	default:
		err = fmt.Errorf("type passed for '%s' is of type '%T' which in unsupported", attr.FieldName, val)
		return
	}
	result = uint64(floatVal * float64(math.Pow10(attr.Scale)))
	if c != nil {
		err = m.UpdateBitmap(c, attr.Parent.Name, attr.FieldName, result, attr.IsTimeSeries)
	}
	return
}

// IntBSIMapper - Maps integer values to a BSI.
type IntBSIMapper struct {
	DefaultMapper
}

// NewIntBSIMapper - Construct a NewIntBSIMapper
func NewIntBSIMapper(conf map[string]string) (Mapper, error) {
	return IntBSIMapper{DefaultMapper{IntBSI}}, nil
}

// MapValue - Map a value to an int64.
func (m IntBSIMapper) MapValue(attr *Attribute, val interface{},
	c *Session) (result uint64, err error) {

	switch val.(type) {
	case int64:
		result = uint64(val.(int64))
	case int32:
		result = uint64(val.(int32))
	case uint32:
		result = uint64(val.(uint32))
	case uint64:
		result = uint64(val.(uint64))
	case int:
		result = uint64(val.(int))
	case uint:
		result = uint64(val.(uint))
	case string:
		str := strings.TrimSpace(val.(string))
		if str == "" {
			str = "0"
		}
		v, e := strconv.ParseInt(str, 10, 64)
		if e != nil {
			err = e
			result = uint64(0)
			return
		}
		result = uint64(v)
	default:
		err = fmt.Errorf("%s: No handling for type '%T'", m.String(), val)
	}
	if c != nil {
		err = m.UpdateBitmap(c, attr.Parent.Name, attr.FieldName, result, attr.IsTimeSeries)
	}
	return
}

// StringEnumMapper - Maps low cardinality strings to standard bitmaps using the metadata db to assign row ids.
type StringEnumMapper struct {
	DefaultMapper
	delim string
}

// NewStringEnumMapper - Construct a NewStringEnumMapper.
func NewStringEnumMapper(conf map[string]string) (Mapper, error) {
	if conf != nil {
		if d, ok := conf["delim"]; ok {
			return StringEnumMapper{DefaultMapper: DefaultMapper{StringEnum}, delim: d}, nil
		}
		return nil, fmt.Errorf("'delim' config param must be supplied for StringEnumMapper")
	}
	return StringEnumMapper{DefaultMapper: DefaultMapper{StringEnum}, delim: ""}, nil
}

// MapValue - Map a value to a row id.
func (m StringEnumMapper) MapValue(attr *Attribute, val interface{},
	c *Session) (result uint64, err error) {

	var multi []string
	switch val.(type) {
	case string:
		strVal := val.(string)
		if strVal == "" {
			return
		}
		if m.delim != "" {
			multi = strings.Split(strVal, m.delim)
		} else {
			multi = []string{strVal}
		}
	case []string:
		multi = val.([]string)
	case int32:
		strVal := fmt.Sprintf("%d", val.(int32))
		multi = []string{strVal}
	default:
		return 0, fmt.Errorf("cannot cast '%s' from '%T' to a string", attr.FieldName, val)
	}

	if c != nil && err == nil {
		for _, v := range multi {
			val := strings.TrimSpace(v)
			if val == "" {
				continue
			}
			if result, err = attr.GetValue(val); err != nil {
				return
			}
			if err = m.UpdateBitmap(c, attr.Parent.Name, attr.FieldName, result, attr.IsTimeSeries); err != nil {
				return
			}
		}
	} else {
		result, err = attr.GetValue(multi[0])
	}
	return
}

// MapValueReverse - Return the original value given a row id.
func (m StringEnumMapper) MapValueReverse(attr *Attribute, id uint64, c *Session) (result interface{}, err error) {
	result, err = attr.GetValueForID(id)
	return
}

// GetMultiDelimiter - Return the delimiter used for multiple value support.
func (m StringEnumMapper) GetMultiDelimiter() string {
	return m.delim
}

// BoolRegexMapper - Maps a string pattern to a boolean value.
type BoolRegexMapper struct {
	DefaultMapper
	regex *regexp.Regexp
}

// NewBoolRegexMapper - Construct a NewBoolRegexMapper
func NewBoolRegexMapper(conf map[string]string) (Mapper, error) {

	if conf != nil {
		if pattern, ok := conf["regex"]; ok {
			r, err := regexp.Compile(pattern)
			if err == nil {
				return BoolRegexMapper{DefaultMapper: DefaultMapper{BoolRegex}, regex: r}, nil
			}
			return nil, err
		}
	}
	return nil, fmt.Errorf("'regex' config param must be supplied for BoolRegexMapper")
}

// MapValue - Map a value to a row id.
func (m BoolRegexMapper) MapValue(attr *Attribute, val interface{},
	c *Session) (result uint64, err error) {

	switch val.(type) {
	case bool:
		result = uint64(0)
		if val.(bool) {
			result = uint64(1)
		}
	default:
		return 0, fmt.Errorf("cannot cast '%s' from '%T' to a string", attr.FieldName, val)
	}

	if c != nil && err == nil {
		err = m.UpdateBitmap(c, attr.Parent.Name, attr.FieldName, result, attr.IsTimeSeries)
	}
	return
}

// Transform - Perform a transformation on a value.
func (m BoolRegexMapper) Transform(attr *Attribute, val interface{}, c *Session) (newVal interface{}, err error) {

	var value string
	switch val.(type) {
	case []byte:
		value = string(val.([]byte))
	default:
		return 0, fmt.Errorf("cannot cast '%s' from '%T' to a string", attr.FieldName, val)
	}

	newVal = m.regex.MatchString(value)
	return
}

// SysMillisBSIMapper - Maps millisecond granularity timestamps to a BSI.
type SysMillisBSIMapper struct {
	DefaultMapper
}

// NewSysMillisBSIMapper - Construct a NewSysMillisBSIMapper
func NewSysMillisBSIMapper(conf map[string]string) (Mapper, error) {
	return SysMillisBSIMapper{DefaultMapper{SysMillisBSI}}, nil
}

// MapValue - Maps a value to an int64
func (m SysMillisBSIMapper) MapValue(attr *Attribute, val interface{},
	c *Session) (result uint64, err error) {

	switch val.(type) {
	case string:
		strVal := val.(string)
		if strVal == "" || strVal == "NULL" {
			result = uint64(0)
			return
		}
		loc, _ := time.LoadLocation("Local")
		var t time.Time
		t, err = dateparse.ParseIn(strVal, loc)
		if err == nil {
			result = uint64(t.UnixNano() / 1000000)
		}
	case []byte:
		t := time.Now()
		err = t.UnmarshalBinary(val.([]byte))
		if err == nil {
			result = uint64(t.UnixNano() / 1000000)
		}
	case time.Time:
		result = uint64(val.(time.Time).UnixNano() / 1000000)
	case int64:
		result = uint64(val.(int64))
	case float64:
		result = uint64(val.(float64))
	default:
		err = fmt.Errorf("%s: No handling for type '%T'", m.String(), val)
	}
	if c != nil && err == nil {
		err = m.UpdateBitmap(c, attr.Parent.Name, attr.FieldName, result, attr.IsTimeSeries)
	}
	return
}

// SysMicroBSIMapper - Maps microsecond granularity timestamps to a BSI.
type SysMicroBSIMapper struct {
	DefaultMapper
}

// NewSysMicroBSIMapper - Construct a NewSysMicroBSIMapper
func NewSysMicroBSIMapper(conf map[string]string) (Mapper, error) {
	return SysMicroBSIMapper{DefaultMapper{SysMicroBSI}}, nil
}

// MapValue - Maps a value to an int64.
func (m SysMicroBSIMapper) MapValue(attr *Attribute, val interface{},
	c *Session) (result uint64, err error) {

	switch val.(type) {
	case string:
		strVal := val.(string)
		if strVal == "" || strVal == "NULL" {
			result = uint64(0)
			return
		}
		loc, _ := time.LoadLocation("Local")
		var t time.Time
		t, err = dateparse.ParseIn(strVal, loc)
		if err == nil {
			result = uint64(t.UnixNano() / 1000)
		}
	case []byte:
		t := time.Now()
		err = t.UnmarshalBinary(val.([]byte))
		if err == nil {
			result = uint64(t.UnixNano() / 1000)
		}
	case time.Time:
		result = uint64(val.(time.Time).UnixNano() / 1000)
	case int64:
		result = uint64(val.(int64))
	case float64:
		result = uint64(val.(float64))
	default:
		err = fmt.Errorf("%s: No handling for type '%T'", m.String(), val)
	}
	if c != nil && err == nil {
		err = m.UpdateBitmap(c, attr.Parent.Name, attr.FieldName, result, attr.IsTimeSeries)
	}
	return
}

// SysSecBSIMapper - Maps Unix (second granularity) timestamps to a BSI.
type SysSecBSIMapper struct {
	DefaultMapper
}

// NewSysSecBSIMapper - Construct a NewSysSecBSIMapper
func NewSysSecBSIMapper(conf map[string]string) (Mapper, error) {
	return SysSecBSIMapper{DefaultMapper{SysSecBSI}}, nil
}

// MapValue - Maps a value to an int64.
func (m SysSecBSIMapper) MapValue(attr *Attribute, val interface{},
	c *Session) (result uint64, err error) {

	switch val.(type) {
	case string:
		strVal := val.(string)
		if strVal == "" || strVal == "NULL" {
			result = uint64(0)
			return
		}
		loc, _ := time.LoadLocation("Local")
		var t time.Time
		t, err = dateparse.ParseIn(strVal, loc)
		result = uint64(t.Unix())
	case []byte:
		t := time.Now()
		err = t.UnmarshalBinary(val.([]byte))
		result = uint64(t.Unix())
	case time.Time:
		result = uint64(val.(time.Time).Unix())
	case int64:
		result = uint64(val.(int64))
	case int32:
		result = uint64(val.(int32))
	default:
		err = fmt.Errorf("%s: No handling for type '%T'", m.String(), val)
	}
	if c != nil && err == nil {
		err = m.UpdateBitmap(c, attr.Parent.Name, attr.FieldName, result, attr.IsTimeSeries)
	}
	return
}

// IntToBoolDirectMapper - Maps 0/1 integer to boolean
type IntToBoolDirectMapper struct {
	DefaultMapper
}

// NewIntToBoolDirectMapper - Construct a NewIntToBoolDirectMapper
func NewIntToBoolDirectMapper(conf map[string]string) (Mapper, error) {
	return IntToBoolDirectMapper{DefaultMapper{IntToBoolDirect}}, nil
}

// MapValue - Map a value to a row id.
func (m IntToBoolDirectMapper) MapValue(attr *Attribute, val interface{},
	c *Session) (result uint64, err error) {

	switch val.(type) {
	case int:
		result = uint64(0)
		if val.(int) != 0 {
			result = uint64(1)
		}
	case bool:
		result = uint64(0)
		if val.(bool) {
			result = uint64(1)
		}
	case string:
		result = uint64(0)
		if val.(string) == "true" {
			result = uint64(1)
		}
	default:
		return 0, fmt.Errorf("cannot cast '%s' from '%T' to a boolean", attr.FieldName, val)
	}

	if c != nil && err == nil {
		err = m.UpdateBitmap(c, attr.Parent.Name, attr.FieldName, result, attr.IsTimeSeries)
	}
	return
}

// Transform - Perform a transformation on a value.
func (m IntToBoolDirectMapper) Transform(attr *Attribute, val interface{}, c *Session) (newVal interface{}, err error) {

	switch val.(type) {
	case int:
		newVal = false
		if val.(int) != 0 {
			newVal = true
		}
	case string:
		newVal = false
		if val.(string) != "0" {
			newVal = true
		}
	default:
		return 0, fmt.Errorf("cannot cast '%s' from '%T' to a boolean", attr.FieldName, val)
	}

	return
}
