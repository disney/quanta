package shared

// Utility functions for shared package

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/signal"
	filepath "path"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/exec"
	"github.com/hashicorp/consul/api"
	"golang.org/x/sync/errgroup"
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
	case int:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(v.(int)))
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
	case reflect.Int:
		return int(binary.LittleEndian.Uint64(buf))
	}
	msg := fmt.Sprintf("Should not be here for kind [%s]!", kind.String())
	panic(msg)
}

// MarshalConsul - Marshal the contents of a Table struct to Consul
func MarshalConsul(in *BasicTable, consul *api.Client) error {

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
		if value.Field(i).Kind() == reflect.Uint64 {
			fv = fmt.Sprintf("%d", fv.(uint64))
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
func UnmarshalConsul(consul *api.Client, name string) (BasicTable, error) {

	table := BasicTable{Name: name}
	keys, _, _ := consul.KV().Keys("schema/" + name, "", nil)
	if len(keys) == 0 {
		return table, fmt.Errorf("Table %s not found.", name)
	}
	ps := reflect.ValueOf(&table)
	err := getRecursive(reflect.TypeOf(table), ps.Elem(), consul, "schema/"+name)
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
		case reflect.Uint, reflect.Uint64:
			if x, err := strconv.ParseInt(string(kvPair.Value), 10, 64); err == nil {
				value.Field(i).SetUint(uint64(x))
			} else {
				valx := UnmarshalValue(value.Field(i).Kind(), kvPair.Value)
				value.Field(i).SetUint(valx.(uint64))
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

// TableExists - Check for the existence of the table in Consul
func TableExists(consul *api.Client, name string) (bool, error) {

	if name == "" {
		return false, fmt.Errorf("table name must not be empty")
	}

	path := fmt.Sprintf("schema/%s/primaryKey", name)
	kvPair, _, err := consul.KV().Get(path, nil)
	if err != nil {
		return false, fmt.Errorf("TableExists: %v", err)
	}
	if kvPair == nil {
		return false, nil
	}
	return true, nil
}

// DeleteTable - Delete the table data from Consul.
func DeleteTable(consul *api.Client, name string) error {

	if name == "" {
		return fmt.Errorf("table name must not be empty")
	}
	path := fmt.Sprintf("schema/%s", name)
	_, err := consul.KV().DeleteTree(path, nil)
	if err != nil {
		return fmt.Errorf("DeleteTable: %v", err)
	}
	return nil
}

// CheckParentRelation - Returns true if there are no foreign keys or the referenced tables exist.
func CheckParentRelation(consul *api.Client, table *BasicTable) (bool, error) {

	if table == nil {
		return false, fmt.Errorf("table must not be nil")
	}

	ok := true
	var err error
	for _, v := range table.Attributes {
		if v.ForeignKey != "" {
			ok, err = TableExists(consul, v.ForeignKey)
			if err != nil {
				err = fmt.Errorf("CheckParentRelation error: %v", err)
				ok = false
			}
			if !ok {
				break
			}
		}
	}
	return ok, err
}

// GetTables - Return a list of deployed tables.
func GetTables(consul *api.Client) ([]string, error) {

	results := make([]string, 0)
	keys := make(map[string]struct{}, 0)
	pairs, _, err := consul.KV().List("schema", nil)
	if err != nil {
		return results, err
	}
	for _, v := range pairs {
		s := strings.Split(v.Key, SEP)
		keys[s[1]] = struct{}{}
	}
	for v := range keys {
		results = append(results, v)
	}
	return results, nil
}

// UpdateModTimeForTable - Updates the tables modificiation timestamp
func UpdateModTimeForTable(consul *api.Client, tableName string) error {

	current := time.Now().UTC().Format(time.RFC3339)
	var kvPair api.KVPair
	kvPair.Key = "schema" + SEP + tableName + SEP + "modificationTime"
	kvPair.Value = ToBytes(current)
	if _, err := consul.KV().Put(&kvPair, nil); err != nil {
		return err
	}
	return nil
}

func getDeployedFKReferenceMap(consul *api.Client) (map[string][]string, error) {

	results := make(map[string][]string)
	pairs, _, err := consul.KV().List("schema", nil)
	if err != nil {
		return results, err
	}
	for _, v := range pairs {
		if filepath.Base(v.Key) == "foreignKey" {
			referencedTable := string(v.Value)
			s := strings.Split(v.Key, SEP)
			references := results[referencedTable]
			if references == nil {
				references = make([]string, 0)
			}
			references = append(references, s[1])
			results[referencedTable] = references
		}
	}
	return results, nil
}

// CheckChildRelation - Returns list of dependent references to this table.
func CheckChildRelation(consul *api.Client, tableName string) ([]string, error) {

	results, err := getDeployedFKReferenceMap(consul)
	if err != nil {
		return nil, err
	}
	if ret, ok := results[tableName]; ok {
		return ret, err
	}
	return []string{}, nil
}

// Lock - Distributed lock
func Lock(consul *api.Client, lockName, processName string) (*api.Lock, error) {

	// If Consul client is not set then we are not running in distributed mode.  Use local mutex.
	if consul == nil {
		return nil, fmt.Errorf("lock: Consul client not set")
	}

	// create lock key
	opts := &api.LockOptions{
		Key:        lockName + "/1",
		Value:      []byte("lock set by " + processName),
		SessionTTL: "10s",
		/*
			   		SessionOpts: &api.SessionEntry{
				 	Checks:   []string{"check1", "check2"},
				 	Behavior: "release",
			   		},
		*/
	}

	lock, err := consul.LockOpts(opts)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			u.Infof("Interrupted...")
			err = lock.Unlock()
			if err != nil {
				return
			}
		}
	}()

	// acquire lock
	u.Debugf("Acquiring lock ...")
	stopCh := make(chan struct{})
	lockCh, err := lock.Lock(stopCh)
	if err != nil {
		return lock, err
	}
	if lockCh == nil {
		return nil, fmt.Errorf("lock already held")
	}
	return lock, nil
}

// Unlock - Unlock distributed lock
func Unlock(consul *api.Client, lock *api.Lock) error {

	u.Debugf("Releasing lock ...")
	if consul == nil {
		return fmt.Errorf("unlock: Consul client not set")
	}
	var err error
	if lock == nil {
		return fmt.Errorf("lock value was nil (not set)")
	}
	err = lock.Unlock()
	if err != nil {
		return err
	}
	return nil
}

// Retry - Generalized retry function - Use in closure
func Retry(attempts int, sleep time.Duration, f func() error) (err error) {
	for i := 0; ; i++ {
		err = f()
		if err == nil {
			return
		}

		if i >= (attempts - 1) {
			break
		}

		time.Sleep(sleep)

		u.Errorf("retrying after error: %v", err)
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}

// GetLocalHostIP - return the hosts IP address.
func GetLocalHostIP() (net.IP, error) {

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip.IsLoopback() {
				continue
			}
			// process IP address
			if ip != nil {
				return ip, nil
			}
		}
	}
	return nil, fmt.Errorf("local IP address could not be determined")
}

// ToTQTimestamp - Return a partition key timestamp as time.Time and string
func ToTQTimestamp(tqType, timestamp string) (time.Time, string, error) {

	ts := time.Unix(0, 0)
	var err error
	if tqType != "" {
		loc, _ := time.LoadLocation("Local")
		ts, err = dateparse.ParseIn(timestamp, loc)
		if err != nil {
			return ts, "", err
		}
	}
	tFormat := YMDTimeFmt
	if tqType == "YMDH" {
		tFormat = YMDHTimeFmt
	}
	sf := ts.Format(tFormat)
	tq, _ := time.Parse(tFormat, sf)
	return tq, ts.Format(YMDHTimeFmt), nil
}

// GetClusterSizeTarget - Get the target cluster size.
func GetClusterSizeTarget(consul *api.Client) (int, error) {

	if consul == nil {
		return -1, fmt.Errorf("consul client is not provided")
	}

	path := "config/clusterSizeTarget"
	kvPair, _, err := consul.KV().Get(path, nil)
	if err != nil {
		return -1, err
	}
	if kvPair == nil {
		return 0, nil
	}
	v := UnmarshalValue(reflect.Int, kvPair.Value)
	return v.(int), nil
}

// SetClusterSizeTarget - Set the target cluster size.
func SetClusterSizeTarget(consul *api.Client, size int) error {

	var kvPair api.KVPair
	kvPair.Key = "config/clusterSizeTarget"
	kvPair.Value = ToBytes(size)
	if _, err := consul.KV().Put(&kvPair, nil); err != nil {
		return err
	}
	return nil
}

// GetIntParam retrieves an int value from a parameter map
func GetIntParam(params map[string]interface{}, key string) (val int, err error) {

	if params != nil {
		sParam := params[key]
		if sParam != nil {
			switch v := sParam.(type) {
			case int64:
				val = int(sParam.(int64))
			case string:
				var valx int64
				valx, err = strconv.ParseInt(sParam.(string), 0, 32)
				if err != nil {
					err = fmt.Errorf("error parsing %s - %v", key, err)
					return
				}
				val = int(valx)
			default:
				err = fmt.Errorf("unknown type %T for timeout", v)
			}
		}
	}
	return
}

// GetBoolParam retrieves an boolean value from a parameter map
func GetBoolParam(params map[string]interface{}, key string) (val bool, err error) {

	if params != nil {
		sParam := params[key]
		if sParam != nil {
			switch v := sParam.(type) {
			case bool:
				val = sParam.(bool)
			case string:
				val, err = strconv.ParseBool(sParam.(string))
				if err != nil {
					err = fmt.Errorf("error parsing %s - %v", key, err)
					return
				}
			default:
				err = fmt.Errorf("unknown type %T for timeout", v)
			}
		}
	}
	return
}

// WaitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func WaitTimeout(wg *errgroup.Group, timeout time.Duration, sigChan exec.SigChan) (error, bool) {

	c := make(chan error, 1)
	go func() {
		defer close(c)
		c <- wg.Wait()
	}()
	select {
	case err := <-c:
		return err, false // completed normally
	case <-time.After(timeout):
		sigChan <- true
		close(sigChan)
		return nil, true // timed out
	}
}
