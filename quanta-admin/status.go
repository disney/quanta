package main

import (
	"fmt"
	"github.com/disney/quanta/shared"
)

// StatusCmd - Status command
type StatusCmd struct {
}

// Run - Status command implementation
func (s *StatusCmd) Run(ctx *Context) error {

	conn := getClientConnection(ctx.ConsulAddr, ctx.Port)

	fmt.Println("ADDRESS            STATUS   DATA CENTER      CONSUL NODE ID                        VERSION")
	fmt.Println("================   ======   ==============   ====================================  =========================")
	for _, node := range conn.Nodes() {
		status := "Left"
		version := ""
		if node.Checks[0].Status == "passing" {
			status = "Crashed"
			if node.Checks[1].Status == "passing" {
				// Invoke Status API
				if result, err := shared.GetNodeStatusForID(conn, node.Service.ID); err != nil {
					fmt.Printf("Error: %v\n", err)
					continue
				} else {
					status = result.NodeState
					version = result.Version
				}
			}
		}
		fmt.Printf("%-16s   %-7s  %-14s   %-25s  %s\n", node.Node.Address, status, node.Node.Datacenter, node.Node.ID, version)
	}
	return nil
}
