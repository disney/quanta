package admin

import (
	"fmt"
	"time"

	"github.com/disney/quanta/core"
	"github.com/disney/quanta/shared"
)

// PartitionInfoCmd - Find key command
type PartitionInfoCmd struct {
	Timestamp string `arg:"" name:"time" help:"Ending time quantum value to show (shards before this date/time)."`
	Table     string `name:"table" help:"Table name (optional)."`
}

// Run - PartitionInfo command implementation
func (f *PartitionInfoCmd) Run(ctx *Context) error {

	ts, tf, err := shared.ToTQTimestamp("YMDH", f.Timestamp)
	if err != nil {
		return err
	}

	fmt.Printf("\nTimestamp = %v, Partitions before and including = %v\n", ts, tf)

	conn := shared.GetClientConnection(ctx.ConsulAddr, ctx.Port, "shardInfo")
	defer conn.Disconnect()

	fmt.Println("")
	bitClient := shared.NewBitmapIndex(conn)
	res, err := bitClient.PartitionInfo(ts, f.Table)
	if  err != nil {
		return err
	}
	fmt.Println("")

	if len(res) == 0 {
		fmt.Println("No partitions selected.")
		return nil
	}

	fmt.Println("TABLE                            PARTITION          MODTIME                  MEMORY     SHARDS")
	fmt.Println("==============================   ================   ====================   ========   ========")
	var totMemory uint32
	var totShards int
	for _, v := range res {
		var quantum string
		if v.TQType == "YMDH" {
			quantum = v.Quantum.Format(shared.YMDHTimeFmt)
		} else {
			quantum = v.Quantum.Format(shared.YMDTimeFmt)
		}
		modTime := v.ModTime.Format(time.RFC3339)
		fmt.Printf("%-30s   %-16v   %-20v   %8v   %8d\n", v.Table, quantum, modTime, 
			core.Bytes(v.MemoryUsed), v.Shards)
		totMemory += v.MemoryUsed
		totShards += v.Shards
	}
	fmt.Println("                                                                           ========   ========")
	fmt.Printf("                                                                           %8v   %8d\n", 
		core.Bytes(totMemory), totShards)
	fmt.Println("")
	return nil
}
