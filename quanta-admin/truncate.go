package main

import (
	"fmt"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
	"time"
)

// TruncateCmd - Truncate command
type TruncateCmd struct {
	Table       string `arg name:"table" help:"Table name."`
	DropEnums   bool   `help:"Drop enumeration data for StringEnum types."`
	Force       bool   `help:"Force override of constraints."`
}

// Run - Truncate command implementation
func (c *TruncateCmd) Run(ctx *Context) error {

	fmt.Printf("Connecting to Consul at: [%s] ...\n", ctx.ConsulAddr)
	consulClient, err := api.NewClient(&api.Config{Address: ctx.ConsulAddr})
	if err != nil {
		fmt.Println("Is the consul agent running?")
		return fmt.Errorf("Error connecting to consul %v", err)
	}

	if err = checkForChildDependencies(consulClient, c.Table, "truncate"); err != nil && !c.Force {
		return err
	}

	lock, errx := shared.Lock(consulClient, "admin-tool", "admin-tool")
	if errx != nil {
		return errx
	}
	defer shared.Unlock(consulClient, lock)

	err = shared.UpdateModTimeForTable(consulClient, c.Table)
	if err != nil {
		return fmt.Errorf("updateModTimeForTable  error %v", err)
	}

	// Give consumers time to flush and restart.
	time.Sleep(time.Second * 5)

	err = nukeData(consulClient, ctx.Port, c.Table, "truncate", c.DropEnums)
	if err != nil {
		return err
	}

	fmt.Printf("Successfully truncated table %s\n", c.Table)
	return nil
}
