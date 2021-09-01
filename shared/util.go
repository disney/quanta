package shared

// Utility functions for shared package

import (
	"encoding/binary"
	"fmt"
	"github.com/hashicorp/consul/api"
	filepath "path"
	"reflect"
	"strconv"
	"strings"
)

// ToString - Interface type to string
func ToString(v interface{}) string {

	switch v.(type) {
	case string:
		return v.(string)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ToBytes - Helper function to serialize data for GRPC.
func ToBytes(v interface{}) []byte {

	switch v.(type) {
	case string:
		return []byte(v.(string))
	case uint64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, v.(uint64))
		return b
	case int64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(v.(int64)))
		return b
	}
	msg := fmt.Sprintf("Unsupported type %T", v)
	panic(msg)
}

// UnmarshalValue - Unmarshal GRPC value from bytes.
func UnmarshalValue(kind reflect.Kind, buf []byte) interface{} {

	switch kind {
	case reflect.String:
		return string(buf)
	case reflect.Uint64:
		return binary.LittleEndian.Uint64(buf)

	}
	msg := fmt.Sprintf("Should not be here for kind [%s]!", kind.String())
	panic(msg)
}

// MarshalConsul - Marshal the contents of a Table struct to Consul
func MarshalConsul(in *Table, consul *api.Client) error {

	table := *in
	return putRecursive(reflect.TypeOf(table), reflect.ValueOf(table), consul, "schema/"+table.Name)
}

func putRecursive(typ reflect.Type, value reflect.Value, consul *api.Client, root string) error {

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		var omit bool
		var tagName string
		if tag, ok := field.Tag.Lookup("yaml"); ok {
			if tag == "-" || tag == "" {
				continue
			}
			s := strings.Split(tag, ",")
			tagName = s[0]
			if len(s) > 1 && s[1] == "omitempty" {
				omit = true
			}
		}
		if field.Type.Kind() == reflect.Slice {
			for j := 0; j < value.Field(i).Len(); j++ {
				path := root + "/" + tagName
				putRecursive(field.Type.Elem(), value.Field(i).Index(j), consul, path)
			}
			continue
		}
		if omit && value.Field(i).IsZero() {
			continue
		}
		if !value.Field(i).CanInterface() {
			continue
		}
		if tagName == "tableName" {
			continue
		}
		fv := value.Field(i).Interface()
		var kvPair api.KVPair
		if tagName == "fieldName" || tagName == "value" {
			root = root + "/" + fv.(string)
		}
		if field.Type.Kind() == reflect.Map {
			if tagName == "configuration" {
				for _, k := range value.Field(i).MapKeys() {
					v := value.Field(i).MapIndex(k)
					kvPair.Key = root + "/" + tagName + "/" + k.String()
					kvPair.Value = ToBytes(v.String())
					if _, err := consul.KV().Put(&kvPair, nil); err != nil {
						return err
					}
				}
			}
			continue
		}
		if value.Field(i).Kind() == reflect.Bool {
			if fv.(bool) == true {
				fv = "true"
			} else {
				fv = "false"
			}
		}
		if value.Field(i).Kind() == reflect.Int {
			fv = fmt.Sprintf("%d", fv.(int))
		}
		if value.Field(i).Kind() == reflect.Int64 {
			fv = fmt.Sprintf("%d", fv.(int64))
		}
		kvPair.Key = root + "/" + tagName
		kvPair.Value = ToBytes(fv)
		if _, err := consul.KV().Put(&kvPair, nil); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalConsul - Populate the contents of the Table struct from Consul
func UnmarshalConsul(consul *api.Client, name string) (Table, error) {

	table := Table{Name: name}
	ps := reflect.ValueOf(&table)
	err := getRecursive(reflect.TypeOf(table), ps.Elem(), consul, "schema/"+name)
	for i := range table.Attributes {
		table.Attributes[i].Parent = &table
	}
	return table, err
}

func getRecursive(typ reflect.Type, value reflect.Value, consul *api.Client, root string) error {

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		var tagName string
		if tag, ok := field.Tag.Lookup("yaml"); ok {
			if tag == "-" || tag == "" {
				continue
			}
			s := strings.Split(tag, ",")
			tagName = s[0]
		}
		if field.Type.Kind() == reflect.Slice {
			path := root + "/" + tagName
			keys, _, err := consul.KV().Keys(path, "", nil)
			if err != nil {
				return err
			}
			if len(keys) == 0 {
				continue
			}
			slice := reflect.MakeSlice(field.Type, 0, 0)
			for j := 0; j < len(keys); j++ {
				if strings.HasSuffix(path, "values") && strings.HasSuffix(keys[j], "value") {
					slicePath := keys[j][:len(keys[j])-6] //length of "value" - 1
					newVal := reflect.New(field.Type.Elem())
					getRecursive(field.Type.Elem(), reflect.Indirect(newVal), consul, slicePath)
					slice = reflect.Append(slice, newVal.Elem())
				}
				if strings.HasSuffix(keys[j], "fieldName") {
					slicePath := keys[j][:len(keys[j])-10] //length of "fieldName" - 1
					newVal := reflect.New(field.Type.Elem())
					getRecursive(field.Type.Elem(), reflect.Indirect(newVal), consul, slicePath)
					slice = reflect.Append(slice, newVal.Elem())
				}
			}
			value.Field(i).Set(slice)
			continue
		}
		if field.Type.Kind() == reflect.Map {
			if tagName == "configuration" {
				path := root + "/" + tagName
				pairs, _, err := consul.KV().List(path, nil)
				if err != nil {
					return err
				}
				if len(pairs) == 0 {
					continue
				}
				configMap := reflect.MakeMap(field.Type)
				for _, v := range pairs {
					configMap.SetMapIndex(reflect.ValueOf(filepath.Base(v.Key)), reflect.ValueOf(string(v.Value)))
				}
				value.Field(i).Set(configMap)
			}
			continue
		}
		if tagName == "tableName" {
			continue
		}
		kvPair, _, err := consul.KV().Get(root+"/"+tagName, nil)
		if err != nil {
			return err
		}
		if kvPair == nil {
			continue
		}
		switch value.Field(i).Kind() {
		case reflect.Interface:
			value.Field(i).Set(reflect.ValueOf(string(kvPair.Value)))
		case reflect.String:
			value.Field(i).SetString(string(kvPair.Value))
		case reflect.Int, reflect.Int64:
			if x, err := strconv.ParseInt(string(kvPair.Value), 10, 64); err == nil {
				value.Field(i).SetInt(x)
			}
		case reflect.Bool:
			value.Field(i).SetBool(false)
			if string(kvPair.Value) == "true" {
				value.Field(i).SetBool(true)
			}
		}
	}
	return nil
}
