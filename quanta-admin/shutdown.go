package main

import (
	"context"
	"fmt"
	"github.com/disney/quanta/shared"
	"github.com/golang/protobuf/ptypes/empty"
)

// ShutdownCmd - Shutdown command
type ShutdownCmd struct {
	NodeIP string `arg name:"node-ip" help:"IP address of node to shutdown or ALL."`
}

// Run - Shutdown command implementation
func (s *ShutdownCmd) Run(ctx *Context) error {

	conn := getClientConnection(ctx.ConsulAddr, ctx.Port)
	cx, cancel := context.WithTimeout(context.Background(), shared.Deadline)
	defer cancel()
	for i, v := range conn.Admin {
		if _, err := v.Shutdown(cx, &empty.Empty{}); err != nil {
			fmt.Printf(fmt.Sprintf("%v.Shutdown(_) = _, %v, node = %s\n", v, err, conn.ClientConnections()[i].Target()))
		} else {
			fmt.Printf("Node %s shutdown triggered.\n", conn.ClientConnections()[i].Target())
		}
	}
	return nil
}
