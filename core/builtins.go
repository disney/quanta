package core

//
// This file defines all of the built in mapping functions.
//

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/google/uuid"
	endian "github.com/dnaeon/go-uuid-endianness/uuid"

	"github.com/disney/quanta/shared"
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
func (m StringHashBSIMapper) MapValue(attr *Attribute, val interface{}, c *Session, 
		isUpdate bool) (result *big.Int, err error) {

	var strVal string
	switch val.(type) {
	case string:
		strVal = val.(string)
		if strVal != "" {
			result = Get64BitHash(strVal)
		}
	case int64:
		val = val.(int64)
		strVal = fmt.Sprintf("%d", val)
		result = Get64BitHash(strVal)
	case float64:
		val = val.(float64)
		strVal = fmt.Sprintf("%.2f", val)
		result = Get64BitHash(strVal)
	case map[string]interface{}:
		x := val.(map[string]interface{})
		if len(x) == 0 {
			return
		}
		b, errx := json.Marshal(x)
		if errx != nil {
			err = errx
			return
		}
		strVal = string(b)
		val = strVal
		result = Get64BitHash(strVal)
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, false)
			if tbuf, ok := c.TableBuffers[attr.Parent.Name]; ok {
				stringPath := indexPath(tbuf, attr.FieldName, "strings") // indexPath() is in core/session.go
				c.BatchBuffer.SetPartitionedString(stringPath, tbuf.CurrentColumnID, strVal)
			}
		}
		return
	default:
		err = fmt.Errorf("StringHashBSIMapper not expecting a '%T' for '%s'", val, attr.FieldName)
		return
	}

	if c != nil {
		if tbuf, ok := c.TableBuffers[attr.Parent.Name]; ok {
			if attr.Searchable {
				err = c.StringIndex.Index(strVal)
			}
			stringPath := indexPath(tbuf, attr.FieldName, "strings") // indexPath() is in core/session.go
			c.BatchBuffer.SetPartitionedString(stringPath, tbuf.CurrentColumnID, strVal)
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, false)
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
		c *Session, isUpdate bool) (result *big.Int, err error) {

	result = big.NewInt(0)
	switch val.(type) {
	case bool:
		if val.(bool) == true {
			result = big.NewInt(1)
		}
	case string:
		str := strings.TrimSpace(val.(string))
		if str == "" || strings.ContainsAny(str, `Nn`) {
			break
		}
		if strings.ContainsAny(str, `Yy`) {
			result = big.NewInt(1)
			break
		}
		v, e := strconv.ParseBool(str)
		if e != nil {
			err = e
			return
		}
		if v {
			result = big.NewInt(1)
		}
	case int:
		if val.(int) == 1 {
			result = big.NewInt(1)
		}
	case int64:
		if val.(int64) == 1 {
			result = big.NewInt(1)
		}
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, false)
			return
		}
	default:
		err = fmt.Errorf("%v: No handling for type '%T'", val, val)
		return
	}
	if c != nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, isUpdate)
	}
	return
}

// MapValueReverse - Map a row ID back to original value (true/false)
func (m BoolDirectMapper) MapValueReverse(attr *Attribute, id uint64, c *Session) (result interface{}, err error) {
	result = false
	if id == uint64(1) {
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
		c *Session, isUpdate bool) (result *big.Int, err error) {

	switch val.(type) {
	case uint64:
		result = big.NewInt(int64(val.(uint64)))
	case int64:
		result = big.NewInt(val.(int64))
	case uint32:
		result = big.NewInt(int64(val.(uint32)))
	case int32:
		result = big.NewInt(int64(val.(int32)))
	case int:
		result = big.NewInt(int64(val.(int)))
	case string:
		str := strings.TrimSpace(val.(string))
		if str == "" {
			str = "0"
		}
		v, e := strconv.ParseInt(str, 10, 64)
		if e != nil {
			err = e
			result = big.NewInt(0)
			return
		}
		if v <= 0 {
			err = fmt.Errorf("cannot map %d as a positive non-zero value", v)
			return
		}
		result = big.NewInt(v)
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, isUpdate)
			return
		}
	default:
		err = fmt.Errorf("%v: No handling for type '%T'", val, val)
		return
	}
	if c != nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, isUpdate)
	}
	return
}

// MapValueReverse - Map a row ID back to original value (row ID value taken literally)
func (m IntDirectMapper) MapValueReverse(attr *Attribute, id uint64, c *Session) (result interface{}, err error) {
	result = id
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
		c *Session, isUpdate bool) (result *big.Int, err error) {

	if val == nil &&  c != nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, isUpdate)
	}

	var v int64
	v, err = strconv.ParseInt(strings.TrimSpace(val.(string)), 10, 64)
	if v <= 0 {
		err = fmt.Errorf("cannot map %d as a positive non-zero value", v)
		return
	}
	result = big.NewInt(v)
	if err == nil && c != nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, isUpdate)
	}
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

// MapValue - Map a value to an big.Int
func (m FloatScaleBSIMapper) MapValue(attr *Attribute, val interface{},
	c *Session, isUpdate bool) (result *big.Int, err error) {

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
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, false)
		}
		return
	default:
		err = fmt.Errorf("type passed for '%s' is of type '%T' which in unsupported", attr.FieldName, val)
		return
	}
	if floatVal != 0 {
		// don't let 7509.999999999999 become 75.09 instead of 75.1
		scaled := floatVal * float64(math.Pow10(attr.Scale)) // this will change 75.1 to 7509.999999999999
		// adjustment := .000000000001 * math.Log10(scaled) // the slow way
		// adjusted := scaled + adjustment
		// read math.Nextafter. It's a hoot. They operate on the int64 representation of the float !
		var adjusted float64
		if floatVal > 0 {
			adjusted = math.Nextafter(scaled, float64(math.MaxFloat64)) // this will change 7509.999999999999 to 7510.00000000000
		} else {
			adjusted = math.Nextafter(scaled, float64(-math.MaxFloat64))
		}
		result = big.NewInt(int64(adjusted))
		checkRound := float64(result.Int64()) / float64(math.Pow10(attr.Scale))
		if checkRound != floatVal {
			err = fmt.Errorf("this would result in rounding error for field '%s', value should have %d decimal places",
				attr.FieldName, attr.Scale)
		}
	} else {
		result = big.NewInt(0)
	}
	if c != nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, false)
	}
	return
}

func (m FloatScaleBSIMapper) Render(attr *Attribute, value interface{}) string {
	if val, ok := value.(*big.Int); ok {
		switch shared.TypeFromString(attr.Type) {
		case shared.Float:
			f := fmt.Sprintf("%%10.%df", attr.Scale)
			return fmt.Sprintf(f, float64(val.Int64())/math.Pow10(attr.Scale))
		}
	}
	return "???"
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
		c *Session, isUpdate bool) (result *big.Int, err error) {

	switch val.(type) {
	case int64:
		result = big.NewInt(val.(int64))
	case int32:
		result = big.NewInt(int64(val.(int32)))
	case uint32:
		result = big.NewInt(int64(val.(uint32)))
	case uint64:
		result = big.NewInt(int64(val.(uint64)))
	case int:
		result = big.NewInt(int64(val.(int)))
	case uint:
		result = big.NewInt(int64(val.(uint)))
	case string:
		str := strings.TrimSpace(val.(string))
		if str == "" {
			str = "0"
		}
		v, e := strconv.ParseInt(str, 10, 64)
		if e != nil {
			err = e
			result = big.NewInt(0)
			return
		}
		result = big.NewInt(v)
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, false)
		}
		return
	default:
		err = fmt.Errorf("%s: No handling for type '%T'", m.String(), val)
	}
	if c != nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, false)
	}
	return
}

func (m IntBSIMapper) Render(attr *Attribute, value interface{}) string {
	if val, ok := value.(*big.Int); ok {
		switch shared.TypeFromString(attr.Type) {
		case shared.Integer:
			return fmt.Sprintf("%10d", val.Int64())
		}
	}
	return "???"
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
		c *Session, isUpdate bool) (result *big.Int, err error) {

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
	case int64:
		strVal := fmt.Sprintf("%d", val.(int64))
		multi = []string{strVal}
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, isUpdate)
		}
		return
	default:
		return nil, fmt.Errorf("cannot cast '%s' from '%T' to a string", attr.FieldName, val)
	}

	var rv uint64
	if c != nil && err == nil {
		for _, v := range multi {
			val := strings.TrimSpace(v)
			if val == "" {
				continue
			}
			if rv, err = attr.GetValue(val); err != nil {
				return
			}
			if err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, rv, isUpdate); err != nil {
				return
			}
		}
	} else {
		rv, err = attr.GetValue(multi[0])
	}
	result = big.NewInt(int64(rv))
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
		c *Session, isUpdate bool) (result *big.Int, err error) {

	switch val.(type) {
	case bool:
		result = big.NewInt(0)
		if val.(bool) {
			result = big.NewInt(1)
		}
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, isUpdate)
		}
		return
	default:
		return nil, fmt.Errorf("cannot cast '%s' from '%T' to a string", attr.FieldName, val)
	}

	if c != nil && err == nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, isUpdate)
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

func extractSecondsAndNanosToBigInt(mapv map[string]interface{}, secsSource, 
		nanoSource string) (*big.Int, error) {

	secsVal, sok := mapv[secsSource]
	nanoVal, nok := mapv[nanoSource]
	if !sok {
		return nil, fmt.Errorf("'seconds' is missing from source")
	}
	var seconds, nanos int64
	var err error
	if v, ok := secsVal.(float64); ok {
		seconds = int64(v)
	} else { // Gotta be a string
		seconds, err = strconv.ParseInt(secsVal.(string), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("extractSecondsAndNanos parsing 'seconds' - %v", err)
		}
	}
	if nok {
		if v, ok := nanoVal.(float64); ok {
			nanos = int64(v)
		} else { // String
			nanos, _ = strconv.ParseInt(nanoVal.(string), 10, 64)
		}
	}
	bigTime := big.NewInt(int64(seconds*1000000000 + nanos))
	return bigTime, nil
}

func bigIntToSecondsAndNanos(big *big.Int) (seconds, nanos int64) {
	buf := make([]byte, 12)
	big.FillBytes(buf)
	seconds = int64(binary.BigEndian.Uint64(buf[:8]))
	nanos = int64(int32(binary.BigEndian.Uint32(buf[8:])))
	return
}

// SysMillisBSIMapper - Maps millisecond granularity timestamps to a BSI.
type SysMillisBSIMapper struct {
	DefaultMapper
	secondsSource string
	nanosSource  string
}

// NewSysMillisBSIMapper - Construct a NewSysMillisBSIMapper
func NewSysMillisBSIMapper(conf map[string]string) (Mapper, error) {
	if conf != nil {
		if v, ok := conf["seconds"]; ok {
			return SysMillisBSIMapper{DefaultMapper: DefaultMapper{SysMillisBSI}, secondsSource: v}, nil
		}
		return nil, fmt.Errorf("'seconds' config param must be supplied for SysMillisBSIMapper")
		if v, ok := conf["nanos"]; ok {
			return SysMillisBSIMapper{DefaultMapper: DefaultMapper{SysMillisBSI}, nanosSource: v}, nil
		}
		return nil, fmt.Errorf("'nanos' config param must be supplied for SysMillisBSIMapper")
	}
	return SysMillisBSIMapper{DefaultMapper: DefaultMapper{SysMillisBSI}, secondsSource: "seconds", 
		nanosSource: "nanos" }, nil
}

// MapValue - Maps a value to an millisecond granularity timestamp
func (m SysMillisBSIMapper) MapValue(attr *Attribute, val interface{},
		c *Session, isUpdate bool) (result *big.Int, err error) {

	switch val.(type) {
	case map[string]interface{}:   // composite seconds/nanos
		if m.secondsSource == "" || m.nanosSource == "" {
			err = fmt.Errorf("'seconds' or 'nanos' configuration not specified")
			return
		}
		mapv := val.(map[string]interface{})
		result, err = extractSecondsAndNanosToBigInt(mapv, m.secondsSource, m.nanosSource)
		if err == nil {
			result.Div(result, big.NewInt(1000000))
		}
	case string:
		strVal := val.(string)
		if strVal == "" || strVal == "NULL" {
			result = big.NewInt(0)
			return
		}
		loc, _ := time.LoadLocation("Local")
		var t time.Time
		t, err = dateparse.ParseIn(strVal, loc)
		if err == nil {
			result = big.NewInt(t.UnixNano() / 1000000)
		}
	case []byte:
		t := time.Now()
		err = t.UnmarshalBinary(val.([]byte))
		if err == nil {
			result = big.NewInt(t.UnixNano() / 1000000)
		}
	case time.Time:
		result = big.NewInt((val.(time.Time).UnixNano() / 1000000))
	case int64:
		result = big.NewInt(val.(int64))
	case float64:
		result = big.NewInt(int64(val.(float64)))
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, false)
		}
		return
	default:
		err = fmt.Errorf("%s: No handling for type '%T'", m.String(), val)
	}
	if c != nil && err == nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, false)
	}
	return
}

func (m SysMillisBSIMapper) Render(attr *Attribute, value interface{}) string {
	if val, ok := value.(*big.Int); ok {
		switch shared.TypeFromString(attr.Type) {
		case shared.DateTime, shared.Date:
			t := time.Unix(0, val.Int64()*1000000).UTC()
			if val.BitLen() > 64 {
				seconds, nanos := bigIntToSecondsAndNanos(val)
				t = time.Unix(seconds, nanos)
			}
			if shared.TypeFromString(attr.Type) == shared.DateTime {
				return t.Format("2006-01-02T15:04:05.000Z")
			}
			if shared.TypeFromString(attr.Type) == shared.Date {
				return t.Format("2006-01-02")
			}
		
		}
	}
	return "???"
}

// SysMicroBSIMapper - Maps microsecond granularity timestamps to a BSI.
type SysMicroBSIMapper struct {
	DefaultMapper
	secondsSource string
	nanosSource  string
}

// NewSysMicroBSIMapper - Construct a NewSysMicroBSIMapper
func NewSysMicroBSIMapper(conf map[string]string) (Mapper, error) {
	if conf != nil {
		if v, ok := conf["seconds"]; ok {
			return SysMicroBSIMapper{DefaultMapper: DefaultMapper{SysMicroBSI}, secondsSource: v}, nil
		}
		return nil, fmt.Errorf("'seconds' config param must be supplied for SysMicroBSIMapper")
		if v, ok := conf["nanos"]; ok {
			return SysMicroBSIMapper{DefaultMapper: DefaultMapper{SysMicroBSI}, nanosSource: v}, nil
		}
		return nil, fmt.Errorf("'nanos' config param must be supplied for SysMicroBSIMapper")
	}
	return SysMicroBSIMapper{DefaultMapper: DefaultMapper{SysMicroBSI}, secondsSource: "seconds", 
		nanosSource: "nanos" }, nil
}

// MapValue - Maps a value to an int64.
func (m SysMicroBSIMapper) MapValue(attr *Attribute, val interface{},
		c *Session, isUpdate bool) (result *big.Int, err error) {

	switch val.(type) {
	case map[string]interface{}:   // composite seconds/nanos
		if m.secondsSource == "" || m.nanosSource == "" {
			err = fmt.Errorf("'seconds' or 'nanos' configuration not specified")
			return
		}
		mapv := val.(map[string]interface{})
		result, err = extractSecondsAndNanosToBigInt(mapv, m.secondsSource, m.nanosSource)
		if err == nil {
			result.Div(result, big.NewInt(1000))
		}
	case string:
		strVal := val.(string)
		if strVal == "" || strVal == "NULL" {
			result = big.NewInt(0)
			return
		}
		loc, _ := time.LoadLocation("Local")
		var t time.Time
		t, err = dateparse.ParseIn(strVal, loc)
		if err == nil {
			result = big.NewInt(t.UnixNano() / 1000)
		}
	case []byte:
		t := time.Now()
		err = t.UnmarshalBinary(val.([]byte))
		if err == nil {
			result = big.NewInt(t.UnixNano() / 1000)
		}
	case time.Time:
		result = big.NewInt(val.(time.Time).UnixNano() / 1000)
	case int64:
		result = big.NewInt(val.(int64))
	case float64:
		result = big.NewInt(int64(val.(float64)))
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, false)
		}
		return
	default:
		err = fmt.Errorf("%s: No handling for type '%T'", m.String(), val)
	}
	if c != nil && err == nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, false)
	}
	return
}

func (m SysMicroBSIMapper) Render(attr *Attribute, value interface{}) string {
	if val, ok := value.(*big.Int); ok {
		switch shared.TypeFromString(attr.Type) {
		case shared.DateTime, shared.Date:
			t := time.Unix(0, val.Int64()*1000).UTC()
			if val.BitLen() > 64 {
				seconds, nanos := bigIntToSecondsAndNanos(val)
				t = time.Unix(seconds, nanos)
			}
			if shared.TypeFromString(attr.Type) == shared.DateTime {
				return t.Format("2006-01-02T15:04:05.000000Z")
			}
			if shared.TypeFromString(attr.Type) == shared.Date {
				return t.Format("2006-01-02")
		 	}
		}
	}
	return "???"
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
		c *Session, isUpdate bool) (result *big.Int, err error) {

	switch val.(type) {
	case string:
		strVal := val.(string)
		if strVal == "" || strVal == "NULL" {
			result = big.NewInt(0)
			return
		}
		loc, _ := time.LoadLocation("Local")
		var t time.Time
		t, err = dateparse.ParseIn(strVal, loc)
		result = big.NewInt(t.Unix())
	case []byte:
		t := time.Now()
		err = t.UnmarshalBinary(val.([]byte))
		result = big.NewInt(t.Unix())
	case time.Time:
		result = big.NewInt(val.(time.Time).Unix())
	case int64:
		result = big.NewInt(val.(int64))
	case int32:
		result = big.NewInt(int64(val.(int32)))
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil,  false)
		}
		return
	default:
		err = fmt.Errorf("%s: No handling for type '%T'", m.String(), val)
	}
	if c != nil && err == nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, false)
	}
	return
}

func (m SysSecBSIMapper) Render(attr *Attribute, value interface{}) string {
	if val, ok := value.(*big.Int); ok {
		switch shared.TypeFromString(attr.Type) {
		case shared.DateTime, shared.Date:
			t := time.Unix(val.Int64(), 0).UTC()
			if val.BitLen() > 64 {
				seconds, nanos := bigIntToSecondsAndNanos(val)
				t = time.Unix(seconds, nanos)
			}
			if shared.TypeFromString(attr.Type) == shared.DateTime {
				return t.Format("2006-01-02T15:04:05Z")
			}
			if shared.TypeFromString(attr.Type) == shared.Date{
				return t.Format("2006-01-02")
			}
		}
	}
	return "???"
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
		c *Session, isUpdate bool) (result *big.Int, err error) {

	switch val.(type) {
	case int:
		result = big.NewInt(0)
		if val.(int) != 0 {
			result = big.NewInt(1)
		}
	case bool:
		result = big.NewInt(0)
		if val.(bool) {
			result = big.NewInt(1)
		}
	case string:
		result = big.NewInt(0)
		if val.(string) == "true" {
			result = big.NewInt(1)
		}
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, isUpdate)
		}
		return
	default:
		return nil, fmt.Errorf("cannot cast '%s' from '%T' to a boolean", attr.FieldName, val)
	}

	if c != nil && err == nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, isUpdate)
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


func extractUpperAndLowerBitsToBigInt(mapv map[string]interface{}, upperSrc, lowerSrc string) *big.Int {

	upperVal, uok := mapv[upperSrc]
	lowerVal, lok := mapv[lowerSrc]
	if !uok || !lok {
		return nil
	}
	upperBits, err := strconv.ParseInt(upperVal.(string), 10, 64)
	if err != nil {
		return nil
	}
	lowerBits, err := strconv.ParseInt(lowerVal.(string), 10, 64)
	if err != nil {
		return nil
	}
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[:8], uint64(upperBits))
	binary.BigEndian.PutUint64(b[8:], uint64(lowerBits))

	uuid, _ := endian.FromBytes(b)
	bigUUID := new(big.Int)
	middleEndian, _ :=  uuid.MiddleEndianBytes()
	bigUUID.SetBytes(middleEndian)
	return bigUUID
}



// UUIDBSIMapper - Maps millisecond granularity timestamps to a BSI.
type UUIDBSIMapper struct {
	DefaultMapper
	upperSrc string
	lowerSrc string
}

// NewUUIDBSIMapper - Construct a NewUUIDBSIMapper
func NewUUIDBSIMapper(conf map[string]string) (Mapper, error) {
	if conf != nil {
		if v, ok := conf["upperSource"]; ok {
			return UUIDBSIMapper{DefaultMapper: DefaultMapper{UUIDBSI}, upperSrc: v}, nil
		}
		return nil, fmt.Errorf("'upperSource' config param must be supplied for UUIDBSIMapper")
		if v, ok := conf["lowerSource"]; ok {
			return UUIDBSIMapper{DefaultMapper: DefaultMapper{UUIDBSI}, lowerSrc: v}, nil
		}
		return nil, fmt.Errorf("'lowerSource' config param must be supplied for UUIDBSIMapper")
	}
	return UUIDBSIMapper{DefaultMapper: DefaultMapper{UUIDBSI}, upperSrc: "upperBits", 
		lowerSrc: "lowerBits" }, nil
}

// MapValue - Maps a value to an millisecond granularity timestamp
func (m UUIDBSIMapper) MapValue(attr *Attribute, val interface{},
		c *Session, isUpdate bool) (result *big.Int, err error) {

	switch val.(type) {
	case map[string]interface{}:   // composite upper/lower bits
		if m.upperSrc == "" || m.lowerSrc == "" {
			err = fmt.Errorf("'upperSource' or 'lowerSource' configuration not specified")
			return
		}
		mapv := val.(map[string]interface{})
		result = extractUpperAndLowerBitsToBigInt(mapv, m.upperSrc, m.lowerSrc)
	case string:

/*
			if uuidVal, errx := endian.FromBytes([]byte(val.(string))); errx == nil {
				b, _ := uuidVal.BigEndianBytes()
				result = new(big.Int).SetBytes(b)
			} else {
				err = errx
			}
*?
/*
			nuuid, _ := endian.FromBytes(val.Bytes())
			middleEndian, _ :=  nuuid.MiddleEndianBytes()
			if newUUID, err := uuid.FromBytes(middleEndian); err == nil {
				return newUUID.String()
			}
*/
			if uuidVal, errx := uuid.Parse(val.(string)); errx == nil {
				b, _ := uuidVal.MarshalBinary()
				nuuid, _ := endian.FromBytes(b)
				middleEndian, _ :=  nuuid.MiddleEndianBytes()
				result = new(big.Int).SetBytes(middleEndian)
			} else {
				err = errx
			}
	case nil:
		if c != nil {
			err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, nil, false)
		}
		return
	default:
		err = fmt.Errorf("%s: No handling for type '%T'", m.String(), val)
	}
	if c != nil && err == nil {
		err = m.MutateBitmap(c, attr.Parent.Name, attr.FieldName, result, false)
	}
	return
}

func (m UUIDBSIMapper) Render(attr *Attribute, value interface{}) string {
	if val, ok := value.(*big.Int); ok {
		switch shared.TypeFromString(attr.Type) {
		case shared.String:
			nuuid, _ := endian.FromBytes(val.Bytes())
			middleEndian, _ :=  nuuid.MiddleEndianBytes()
			if newUUID, err := uuid.FromBytes(middleEndian); err == nil {
				return newUUID.String()
			}
/*
			if newUUID, err := uuid.FromBytes(val.Bytes()); err == nil {
				return newUUID.String()
			} else {
				return fmt.Sprintf("%v", err)
			}
*/
		}
	}
	return "???"
}
