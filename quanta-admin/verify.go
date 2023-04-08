package main

import (
	"context"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	pb "github.com/disney/quanta/grpc"
	proxy "github.com/disney/quanta/quanta-proxy-lib"
	"github.com/disney/quanta/shared"
	"github.com/golang/protobuf/ptypes/empty"
	//"time"
)

// VerifyCmd - Verify command
type VerifyCmd struct {
	Table     string `arg:"" name:"table" help:"Table name."`
	Field     string `arg:"" name:"field" help:"Field name."`
	RowID     uint64 `help:"Row id. (Omit for BSI)"`
	Timestamp string `help:"Time quantum value. (Omit for no quantum)"`
}

// Run - Verify implementation
func (f *VerifyCmd) Run(ctx *proxy.Context) error {

	conn := getClientConnection(ctx.ConsulAddr, ctx.Port)
	table, err := shared.LoadSchema("", f.Table, conn.Consul)
	if err != nil {
		return fmt.Errorf("loading table %s - %v", f.Table, err)
	}
	field, err2 := table.GetAttribute(f.Field)
	if err2 != nil {
		return fmt.Errorf("getting field  %s - %v", f.Field, err2)
	}
	if f.RowID > 0 && field.IsBSI() {
		return fmt.Errorf("field is a BSI and Rowid was specified, ignoring Rowid")
	}
	if f.Timestamp != "" && table.TimeQuantumType == "" {
		return fmt.Errorf("table does not have time quantum, ignoring timestamp")
	}
	if table.TimeQuantumType != "" && f.Timestamp == "" {
		return fmt.Errorf("table has time quantum type %s but timestamp not provided", table.TimeQuantumType)
	}
	ts, tf, err3 := shared.ToTQTimestamp(table.TimeQuantumType, f.Timestamp)
	if err3 != nil {
		return fmt.Errorf("ToTQTimestamp %v - TQType = %s, Timestamp = %s", err3, table.TimeQuantumType, f.Timestamp)
	}
	if f.RowID == 0 {
		f.RowID = 1
	}
	var key string
	if field.IsBSI() {
		key = fmt.Sprintf("%s/%s/%s", f.Table, f.Field, tf)
	} else {
		key = fmt.Sprintf("%s/%s/%d/%s", f.Table, f.Field, f.RowID, tf)
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
		Field:    f.Field,
		//RowId: f.RowID,
		Time: ts.UnixNano(),
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
				len(res.Data), f.Table, f.Field)
		}
		if res.Cardinality > 0 {
			if err := resBsi.UnmarshalBinary(res.Data); err != nil {
				return fmt.Errorf("deserialize sync reponse - BSI UnmarshalBinary error - %v", err)
			}
		}
		lookup, err3 := getStringBackingStore(conn, key, resBsi.GetExistenceBitmap())
		if err3 != nil {
			return err3
		}
		responseCount := 0
		for _, v := range lookup {
			if v != nil {
				responseCount++
			}
		}
		fmt.Printf("NODE: %s   STATE: %s\n", ip, nodeState)
		fmt.Printf("BSI CARDINALITY: %d\n", resBsi.GetCardinality())
		fmt.Printf("LOOKUP RESPONSE LEN: %d, NON-NIL COUNT: %d\n", len(lookup), responseCount)
	}
	fmt.Println("")
	return nil
}

func getStringBackingStore(conn *shared.Conn, key string, bm *roaring64.Bitmap) (map[interface{}]interface{}, error) {

	kvStore := shared.NewKVStore(conn)

	getBatch := make(map[interface{}]interface{}, bm.GetCardinality())
	for _, v := range bm.ToArray() {
		getBatch[v] = ""
	}
	var err error
	getBatch, err = kvStore.BatchLookup(key, getBatch, true)
	if err != nil {
		return nil, fmt.Errorf("kvStore.BatchLookup failed for %s - %v", key, err)
	}

	return getBatch, nil
}
