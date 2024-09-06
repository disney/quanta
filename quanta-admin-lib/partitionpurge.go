package admin

import (
	"fmt"
	"strings"

	"github.com/disney/quanta/shared"
)

// OfflinePartitionsCmd - Offline partitions command.
type OfflinePartitionsCmd struct {
	Timestamp string `arg:"" name:"time" help:"Ending time quantum value to show (shards before this date/time)."`
	Table     string `name:"table" help:"Table name (optional)."`
}

// Run - Offline command implementation
func (f *OfflinePartitionsCmd) Run(ctx *Context) error {

	if len(f.Timestamp) == 13 && strings.Contains(f.Timestamp, "T") {
		f.Timestamp = strings.ReplaceAll(f.Timestamp, "T", " ") + ":00"
	}

	ts, tf, err := shared.ToTQTimestamp("YMDH", f.Timestamp)
	if err != nil {
		return err
	}

	if ctx.Debug {
		fmt.Printf("\nTimestamp = %v, Partitions before and including = %v\n", ts, tf)
	}

	conn := shared.GetClientConnection(ctx.ConsulAddr, ctx.Port, "offlinePartitions")
	defer conn.Disconnect()

	bitClient := shared.NewBitmapIndex(conn)
	err = bitClient.OfflinePartitions(ts, f.Table)
	if  err != nil {
		return err
	}
	return nil
}
