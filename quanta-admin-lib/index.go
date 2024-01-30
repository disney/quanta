package admin

import (
	"context"
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring/roaring64"
	pb "github.com/disney/quanta/grpc"
	"github.com/disney/quanta/shared"
	"github.com/golang/protobuf/ptypes/empty"
	//"time"
)

// VerifyIndexCmd - VerifyIndex command
type VerifyIndexCmd struct {
	Table     string `arg:"" name:"table" help:"Table name."`
	Timestamp string `help:"Time quantum value. (Omit for no quantum)"`
}

// Run - VerifyIndex implementation
func (f *VerifyIndexCmd) Run(ctx *Context) error {

	conn := GetClientConnection(ctx.ConsulAddr, ctx.Port, "VerifyIndexCmd")
	defer conn.Disconnect()
	table, err := shared.LoadSchema("", f.Table, conn.Consul)
	if err != nil {
		return fmt.Errorf("Error loading table %s - %v", f.Table, err)
	}

	info, err2 := table.GetPrimaryKeyInfo()
	if err2 != nil {
		return fmt.Errorf("Error getting primary key info - %v", err2)
	}
	field := info[0]
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
	var key string
	if field.IsBSI() {
		key = fmt.Sprintf("%s/%s/%s", f.Table, field.FieldName, tf)
	} else {
		return fmt.Errorf("Field must be BSI type")
	}

	fmt.Println("")
	fmt.Printf("KEY = %s\n", key)
	cx, cancel := context.WithTimeout(context.Background(), shared.Deadline)
	defer cancel()

	indices, err4 := conn.SelectNodes(key, shared.ReadIntent)
	if err4 != nil {
		return err4
	}
	bitClient := shared.NewBitmapIndex(conn)
	req := &pb.SyncStatusRequest{
		SendData: true,
		ModTime:  int64(-1),
		Index:    f.Table,
		Field:    field.FieldName,
		Time:     ts.UnixNano(),
	}

	fmt.Printf("SelectNodes returned %d node Active state for reading.\n", len(indices))

	if len(indices) >= 1 {
		var ip, nodeState string
		if result, err := conn.Admin[indices[0]].Status(cx, &empty.Empty{}); err != nil {
			fmt.Printf(fmt.Sprintf("%v.Status(_) = _, %v, node = %s\n", conn.Admin[0], err,
				conn.ClientConnections()[indices[0]].Target()))
		} else {
			nodeState = result.NodeState
			ip = result.LocalIP
		}
		res, err2 := bitClient.Client(indices[0]).SyncStatus(cx, req)
		if err2 != nil {
			fmt.Printf(fmt.Sprintf("%v.SyncStatus(_) = _, %v, node = %s\n", bitClient.Client(indices[0]), err2,
				conn.ClientConnections()[indices[0]].Target()))
		}

		// Deserialize
		// Unmarshal data from response
		resBsi := roaring64.NewBSI(int64(field.MaxValue), int64(field.MinValue))
		if len(res.Data) == 0 && res.Cardinality > 0 {
			return fmt.Errorf("deserialize sync response - BSI index out of range %d, Index = %s, Field = %s",
				len(res.Data), f.Table, field.FieldName)
		}
		if res.Cardinality > 0 {
			if err := resBsi.UnmarshalBinary(res.Data); err != nil {
				return fmt.Errorf("deserialize sync reponse - BSI UnmarshalBinary error - %v", err)
			}
		}

		index := key + "/" + table.PrimaryKey + ".PK"
		fmt.Printf("INDEX = %s\n", index)
		kvStore := shared.NewKVStore(conn)
		lookup, err3 := kvStore.NodeItems(kvStore.Client(indices[0]), index, reflect.String, reflect.Uint64)
		if err3 != nil {
			return err3
		}

		revMap := make(map[interface{}]interface{}, len(lookup))
		for k, v := range lookup {
			revMap[v] = k
		}

		foundCount := 0
		colIDs := resBsi.GetExistenceBitmap().ToArray()
		for _, v := range colIDs {
			if _, found := revMap[v]; found {
				foundCount++
			}
		}

		fmt.Printf("NODE: %s   STATE: %s\n", ip, nodeState)
		fmt.Printf("BSI CARDINALITY: %d\n", resBsi.GetCardinality())
		fmt.Printf("INDEX LEN: %d, FOUND COUNT: %d\n", len(lookup), foundCount)
		if resBsi.GetCardinality() != uint64(foundCount) {
			fmt.Printf("INDEX HAS %d FEWER ENTRIES.\n", resBsi.GetCardinality()-uint64(foundCount))
		}
	}
	fmt.Println("")
	return nil
}
