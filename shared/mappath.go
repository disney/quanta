package shared

// map path - functions to retrieve values from map dags

import (
	"fmt"
	//"reflect"
	"strconv"
	"strings"
	"github.com/xitongsys/parquet-go/reader"
)

// GetPath - Recurse and retrieve a value at a given path
func GetPath(path string, s interface{}, ignoreSourcePath, useNerdCapitalization bool) (value interface{}, err error) {

	if ignoreSourcePath{
		path = GetBasePath(path, useNerdCapitalization)
	}

	keys := strings.Split(path, "/")
	value = s
	for _, key := range keys {
		if key == ""{
			continue
		}
		if value, err = get(key, value); err != nil {
			break
		}
	}
	return
}

func get(key string, s interface{}) (v interface{}, err error) {

	var (
		i  int64
		ok bool
	)
	switch s.(type) {
	case map[string]interface{}:
		if v, ok = s.(map[string]interface{})[key]; !ok {
			err = fmt.Errorf("Key not present. [Key:%s]", key)
		}
	case []interface{}:
		if i, err = strconv.ParseInt(key, 10, 64); err == nil {
			array := s.([]interface{})
			if int(i) < len(array) {
				v = array[i]
			} else {
				err = fmt.Errorf("Index out of bounds. [Index:%d] [Array:%v]", i, array)
			}
		}
	case (*reader.ParquetReader):
		v = fmt.Sprintf("%s.%s",s.(*reader.ParquetReader).SchemaHandler.GetRootExName(), key )
	}
	/*
		case Signature:
		    r := reflect.ValueOf(s)
		    v = reflect.Indirect(r).FieldByName(key)
	*/

	return v, err
}

func GetBasePath(source string, useNerdCapitalization bool) string {
	if useNerdCapitalization {
		source = strings.Title(source)
	}
	//if strings.Count(source, "/") > 1 || !strings.HasPrefix(source, "/") {
	if strings.Count(source, "/") > 1 {
		idx := strings.LastIndex(source, "/")
		source = source[idx:]
	}
	return source
}

func CreateNestedMapFromPath(mapPath string, value interface{}) map[string]interface{} {

	leaf := make(map[string]interface{})
	s := strings.Split(mapPath, "/")
	if s[0] == "" && len(s) > 1 {  // Path started with a "/" so adjust
		s = append(s[:0], s[1:]...)
	}

	leaf[s[len(s)-1]] = value

	for i := len(s) - 2; i >= 0; i-- {
		v := s[i]
		m := make(map[string]interface{})
		m[v] = leaf
		leaf = m
	}
	return leaf
}

func MergeMaps(m1, m2 map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for k, v := range m1 {
		result[k] = v
	}

	for k, v2 := range m2 {
		v1, ok := result[k]
		if !ok {
			result[k] = v2
		} else if m1, ok1 := v1.(map[string]interface{}); ok1 {
			if m2, ok2 := v2.(map[string]interface{}); ok2 {
				result[k] = MergeMaps(m1, m2)
			} else {
				result[k] = v2
			}
		} else {
			result[k] = v2
		}
	}

	return result
}

