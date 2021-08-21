package shared

// Utility functions for shared package

import (
	"encoding/binary"
	"fmt"
	"reflect"
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
