package main

import (
	"context"
	"fmt"

	"github.com/disney/quanta/shared"
	"github.com/golang/protobuf/ptypes/empty"

	//"time"
	"reflect"
)

// VerifyEnumCmd - Verify command
type VerifyEnumCmd struct {
	Table     string `arg:"" name:"table" help:"Table name."`
	Field     string `arg:"" name:"field" help:"Field name."`
	RowID     uint64 `help:"Row id. (Omit for BSI)"`
	Timestamp string `help:"Time quantum value. (Omit for no quantum)"`
}

// Run - VerifyEnum implementation
func (f *VerifyEnumCmd) Run(ctx *Context) error {

	conn := getClientConnection(ctx.ConsulAddr, ctx.Port)
	table, err := shared.LoadSchema("", f.Table, conn.Consul)
	if err != nil {
		return fmt.Errorf("Error loading table %s - %v", f.Table, err)
	}
	field, err2 := table.GetAttribute(f.Field)
	if err2 != nil {
		return fmt.Errorf("Error getting field  %s - %v", f.Field, err2)
	}
	_ = field
	/*
		if f.RowID > 0 && field.IsBSI() {
			return fmt.Errorf("Field is a BSI and Rowid was specified, ignoring Rowid")
		}
		if f.Timestamp != "" && table.TimeQuantumType == "" {
			return fmt.Errorf("Table does not have time quantum, ignoring timestamp")
		}
		if table.TimeQuantumType != "" && f.Timestamp == "" {
			return fmt.Errorf("Table has time quantum type %s but timestamp not provided", table.TimeQuantumType)
		}
		ts, tf, err3 := shared.ToTQTimestamp(table.TimeQuantumType, f.Timestamp)
		if err3 != nil {
			return fmt.Errorf("Error ToTQTimestamp %v - TQType = %s, Timestamp = %s", err3, table.TimeQuantumType, f.Timestamp)
		}
	*/
	if f.RowID == 0 {
		f.RowID = 1
	}
	//key := fmt.Sprintf("%s/%s/%d/%s", f.Table, f.Field, f.RowID, tf)

	fmt.Println("")
	cx, cancel := context.WithTimeout(context.Background(), shared.Deadline)
	defer cancel()

	indices, err4 := conn.SelectNodes("", shared.Admin)
	if err4 != nil {
		return err4
	}
	fmt.Printf("SelectNodes returned %d node Active state for reading.\n", len(indices))

	intersectMap := make(map[interface{}]interface{}, 0)

	errCount := 0
	for _, i := range indices {
		var ip, nodeState string
		if result, err := conn.Admin[i].Status(cx, &empty.Empty{}); err != nil {
			fmt.Printf(fmt.Sprintf("%v.Status(_) = _, %v, node = %s\n", conn.Admin[i], err,
				conn.ClientConnections()[i].Target()))
		} else {
			nodeState = result.NodeState
			ip = result.LocalIP
			enums, err := getAllEnumsForField(conn, fmt.Sprintf("%s/%s.StringEnum", f.Table, f.Field), i)
			if err != nil {
				return err
			}
			for k, v := range enums {
				if val, found := intersectMap[k]; !found {
					intersectMap[k] = v
				} else {
					if val != v {
						fmt.Printf("NODE: %s, FOR KEY: %s, MISMATCHED ROWIDS EXISTING: %d, THISNODE: %d\n", ip, k, val, v)
						errCount++
					}
				}
			}
			fmt.Printf("NODE: %s, STATE: %s, has %d items\n", ip, nodeState, len(enums))
		}
	}
	fmt.Println("")
	if errCount == 0 {
		fmt.Printf("No errors found!\n")
	} else {
		fmt.Printf("%d errors found!\n", errCount)
	}
	fmt.Println("")
	return nil
}

func getAllEnumsForField(conn *shared.Conn, key string, index int) (map[interface{}]interface{}, error) {

	kvStore := shared.NewKVStore(conn)
	kvClient := kvStore.Client(index)

	getBatch, err := kvStore.NodeItems(kvClient, key, reflect.String, reflect.Uint64)
	if err != nil {
		return nil, fmt.Errorf("kvStore.NodeItems failed for %s - %v", key, err)
	}

	return getBatch, nil
}
