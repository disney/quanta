package admin

import (
	"context"
	"fmt"
	"time"

	pb "github.com/disney/quanta/grpc"
	"github.com/disney/quanta/shared"
	"github.com/golang/protobuf/ptypes/empty"
)

// FindKeyCmd - Find key command
type FindKeyCmd struct {
	Table     string `arg:"" name:"table" help:"Table name."`
	Field     string `arg:"" name:"field" help:"Field name."`
	RowID     uint64 `help:"Row id. (Omit for BSI)"`
	Timestamp string `help:"Time quantum value. (Omit for no quantum)"`
}

// Run - FindKey command implementation
func (f *FindKeyCmd) Run(ctx *Context) error {

	conn := GetClientConnection(ctx.ConsulAddr, ctx.Port)
	table, err := shared.LoadSchema("", f.Table, conn.Consul)
	if err != nil {
		return fmt.Errorf("Error loading table %s - %v", f.Table, err)
	}
	field, err2 := table.GetAttribute(f.Field)
	if err2 != nil {
		return fmt.Errorf("Error getting field  %s - %v", f.Field, err2)
	}
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
	// Not realy intending to write here but we want all qualifying nodes.
	indices, err4 := conn.SelectNodes(key, shared.WriteIntent)
	if err4 != nil {
		return err4
	}
	bitClient := shared.NewBitmapIndex(conn)
	req := &pb.SyncStatusRequest{Index: f.Table, Field: f.Field, RowId: f.RowID, Time: ts.UnixNano()}
	fmt.Println("")
	fmt.Println("REPLICA   ADDRESS            STATE          STATUS        CARDINALITY   MODTIME")
	fmt.Println("=======   ================   =========      ===========   ===========   ====================")
	for i, index := range indices {
		var status, ip, modTime, nodeState string
		var card uint64
		if result, err := conn.Admin[index].Status(cx, &empty.Empty{}); err != nil {
			fmt.Printf(fmt.Sprintf("%v.Status(_) = _, %v, node = %s\n", conn.Admin[index], err,
				conn.ClientConnections()[index].Target()))
		} else {
			status = result.NodeState
			nodeState = result.NodeState
			ip = result.LocalIP
		}
		if res, err2 := bitClient.Client(index).SyncStatus(cx, req); err2 != nil {
			fmt.Printf(fmt.Sprintf("%v.SyncStatus(_) = _, %v, node = %s\n", bitClient.Client(index), err2,
				conn.ClientConnections()[index].Target()))
		} else {
			if res.Cardinality == 0 && res.ModTime == 0 {
				status = "Missing"
			} else {
				status = "OK"
				card = res.Cardinality
				ts := time.Unix(0, res.ModTime)
				modTime = ts.Format(time.RFC3339)
			}
		}
		fmt.Printf("%-7d   %-16s   %-11s    %-8s      %11d   %s\n", i+1, ip, nodeState, status, card, modTime)
	}
	fmt.Println("")
	return nil
}
