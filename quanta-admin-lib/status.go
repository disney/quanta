package admin

import (
	"fmt"

	"github.com/disney/quanta/core"
)

// StatusCmd - Status command
type StatusCmd struct {
}

// Run - Status command implementation
func (s *StatusCmd) Run(ctx *Context) error {

	conn := GetClientConnection(ctx.ConsulAddr, ctx.Port, "admin-status")
	defer conn.Disconnect()

	fmt.Print("admin StatusCmd top")
	for _, v := range conn.Nodes() {
		fmt.Print(" ", v.Service.ID)
	}
	fmt.Println()

	fmt.Println()
	fmt.Println("ADDRESS            STATUS    DATA CENTER                              SHARDS   MEMORY   VERSION")
	fmt.Println("================   ======    ==================================   ==========   =======  =========================")
	for _, node := range conn.Nodes() {
		status := "Left"
		version := ""
		var shards uint32
		var memory uint32
		if node.Checks[0].Status == "passing" {
			status = "Crashed"
			if node.Checks[1].Status == "passing" {
				// Invoke Status API
				if result, err := conn.GetNodeStatusForID(node.Service.ID); err != nil {
					fmt.Printf("Error: %v\n", err)
					continue
				} else {
					status = result.NodeState
					version = result.Version
					shards = result.ShardCount
					memory = result.MemoryUsed
				}
			}
		}
		fmt.Printf("%-16s   %-8s  %-34s   %10d   %-7s  %s\n", node.Service.Address, status, node.Node.Datacenter, shards,
			core.Bytes(memory), version)
	}
	fmt.Println()
	status, active, size := conn.GetClusterState()
	if active == 0 {
		fmt.Printf("Cluster is DOWN,  Target Cluster Size = %d\n", size)
	} else {
		fmt.Printf("Cluster State = %s, Active nodes = %d, Target Cluster Size = %d\n", status.String(), active, size)
	}
	fmt.Println()
	return nil
}
