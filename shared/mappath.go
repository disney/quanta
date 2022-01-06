// map path - functions to retrieve values from map dags
package shared

import (
	"fmt"
	//"reflect"
	"strconv"
	"strings"
)

// GetPath - Recurse and retrieve a value at a given path
func GetPath(path string, s interface{}) (value interface{}, err error) {

	keys := strings.Split(path, "/")
	value = s
	for _, key := range keys {
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
		/*
		   case Signature:
		       r := reflect.ValueOf(s)
		       v = reflect.Indirect(r).FieldByName(key)
		*/
	}
	//fmt.Println("Value:", v, " Key:", key, "Error:", err)
	return v, err
}
