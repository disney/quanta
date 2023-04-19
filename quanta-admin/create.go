package main

import (
	"fmt"
	"log"

	proxy "github.com/disney/quanta/quanta-proxy-lib"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
)

// CreateCmd - Create command
type CreateCmd struct {
	Table     string `arg:"" name:"table" help:"Table name."`
	SchemaDir string `help:"Base directory containing schema files." default:"./config"`
	Confirm   bool   `help:"Confirm deployment."`
}

// Run - Create command implementation
func (c *CreateCmd) Run(ctx *proxy.Context) error {

	fmt.Printf("Configuration directory = %s\n", c.SchemaDir)
	fmt.Printf("Connecting to Consul at: [%s] ...\n", ctx.ConsulAddr)
	consulClient, err := api.NewClient(&api.Config{Address: ctx.ConsulAddr})
	if err != nil {
		fmt.Println("Is the consul agent running?")
		return fmt.Errorf("Error connecting to consul %v", err)
	}
	table, err3 := shared.LoadSchema(c.SchemaDir, c.Table, consulClient)
	if err3 != nil {
		return fmt.Errorf("Error loading schema %v", err3)
	}

	// Check if the table already exists, if not deploy and verify.  Else, compare and verify.
	ok, _ := shared.TableExists(consulClient, table.Name)
	if !ok {
		// Simulate create table where parent of FK does not exist
		ok, err := shared.CheckParentRelation(consulClient, table)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("cannot create table due to missing parent FK constraint dependency")
		}

		err = performCreate(consulClient, table, ctx.Port)
		if err != nil {
			return fmt.Errorf("errors during performCreate: %v", err)
		}

		fmt.Printf("Successfully created table %s\n", table.Name)
		return nil
	}

	// If here then table already exists.  Perform compare
	table2, err5 := shared.LoadSchema("", table.Name, consulClient)
	if err5 != nil {
		return fmt.Errorf("Error loading schema from consul %v", err5)
	}
	ok2, warnings, err6 := table2.Compare(table)
	if err6 != nil {
		return fmt.Errorf("error comparing deployed table %v", err6)
	}
	if ok2 {
		fmt.Printf("Table already exists.  No differences detected.\n")
		return nil
	}

	// If --confirm flag not set then print warnings and exit.
	if !c.Confirm {
		fmt.Printf("Warnings:\n")
		for _, warning := range warnings {
			fmt.Printf("    -> %v\n", warning)
		}
		return fmt.Errorf("if you wish to deploy the changes then re-run with --confirm flag")
	}
	err = performCreate(consulClient, table, ctx.Port)
	if err != nil {
		return fmt.Errorf("errors during performCreate: %v", err)
	}

	fmt.Printf("Successfully deployed modifications to table %s\n", table.Name)
	return nil
}

func performCreate(consul *api.Client, table *shared.BasicTable, port int) error {

	lock, errx := shared.Lock(consul, "admin-tool", "admin-tool")
	if errx != nil {
		return errx
	}
	defer shared.Unlock(consul, lock)

	fmt.Printf("Connecting to Quanta services at port: [%d] ...\n", port)
	conn := shared.NewDefaultConnection()
	conn.ServicePort = port
	conn.Quorum = 3
	if err := conn.Connect(consul); err != nil {
		log.Fatal(err)
	}
	services := shared.NewBitmapIndex(conn)

	err := shared.DeleteTable(consul, table.Name)
	if err != nil {
		return fmt.Errorf("DeleteTable error %v", err)
	}

	// Go ahead and update Consul
	err = shared.UpdateModTimeForTable(consul, table.Name)
	if err != nil {
		return fmt.Errorf("updateModTimeForTable  error %v", err)
	}
	err = shared.MarshalConsul(table, consul)
	if err != nil {
		return fmt.Errorf("Error marshalling table %v", err)
	}

	// Verify table persistence.  Read schema back from Consul and compare.
	table2, err1 := shared.LoadSchema("", table.Name, consul)
	if err1 != nil {
		return fmt.Errorf("Error loading schema from consul %v", err1)
	}
	ok, _, err2 := table2.Compare(table)
	if err2 != nil {
		return fmt.Errorf("error comparing deployed table %v", err2)
	}
	if !ok {
		return fmt.Errorf("differences detected with deployed table %v", table.Name)
	}

	return services.TableOperation(table.Name, "deploy")
}
