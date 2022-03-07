package main

import (
	"context"
	"fmt"
	"github.com/disney/quanta/shared"
	"github.com/golang/protobuf/ptypes/empty"
	"strings"
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
	indices, err := conn.SelectNodes("", shared.Admin)
	if err != nil {
		return fmt.Errorf("admin tool - Shutdown failed: %v", err)
	}
	if len(indices) != len(conn.Admin) {
		return fmt.Errorf("SelectNodes returned %d indices, not %d", len(indices), len(conn.Admin))
	}
	shutCount := 0
	for i, v := range conn.Admin {
		if s.NodeIP != "all" && !strings.HasPrefix(conn.ClientConnections()[i].Target(), s.NodeIP) {
			continue
		}
		_, _ = v.Shutdown(cx, &empty.Empty{})
		fmt.Printf("Node %s shutdown triggered.\n", conn.ClientConnections()[i].Target())
		shutCount++
	}
	if shutCount > 0 {
		fmt.Printf("%d nodes shut down.\n", shutCount)
	} else {
		fmt.Printf("No nodes matched %s.\n", s.NodeIP)
	}
	return nil
}
