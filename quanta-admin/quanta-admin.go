// Quanta admin cli tool
package main

import (
	"fmt"
	"github.com/alecthomas/kong"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
	"log"
)

// Variables to identify the build
var (
	Version string
	Build   string
)

// Context - Global command line variables
type Context struct {
	ConsulAddr string `help:"Consul agent address/port." default:"127.0.0.1:8500"`
}

// StatusCmd - Status command
type StatusCmd struct {
}

// CreateCmd - Create command
type CreateCmd struct {
	Table     string `arg name:"table" help:"Table name."`
	SchemaDir string `help:"Base directory containing schema files." default:"./config"`
	Confirm   bool   `help:"Confirm deployment."`
}

// VersionCmd - Version command
type VersionCmd struct {
}

// DropCmd - Drop command
type DropCmd struct {
	Table string `arg name:"table" help:"Table name."`
}

// TruncateCmd - Truncate command
type TruncateCmd struct {
	Table string `arg name:"table" help:"Table name."`
}

// TablesCmd - Show tables command
type TablesCmd struct {
}

var cli struct {
	ConsulAddr string      `default:"127.0.0.1:8500"`
	Create     CreateCmd   `cmd help:"Create table."`
	Drop       DropCmd     `cmd help:"Drop table."`
	Truncate   TruncateCmd `cmd help:"Truncate table."`
	Status     StatusCmd   `cmd help:"Show status."`
	Version    VersionCmd  `cmd help:"Show version."`
	Tables     TablesCmd   `cmd help:"Show tables."`
}

func main() {

	ctx := kong.Parse(&cli)
	err := ctx.Run(&Context{ConsulAddr: cli.ConsulAddr})
	ctx.FatalIfErrorf(err)
}

// Run - Create command implementation
func (c *CreateCmd) Run(ctx *Context) error {

	log.Printf("Configuration directory = %s\n", c.SchemaDir)
	log.Printf("Connecting to Consul at: [%s] ...\n", ctx.ConsulAddr)
	consulClient, err := api.NewClient(&api.Config{Address: ctx.ConsulAddr})
	if err != nil {
		log.Printf("Is the consul agent running?")
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

		err = performCreate(consulClient, table)
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
	err = performCreate(consulClient, table)
	if err != nil {
		return fmt.Errorf("errors during performCreate: %v", err)
	}

	fmt.Printf("Successfully deployed modifications to table %s\n", table.Name)
	return nil
}

func performCreate(consul *api.Client, table *shared.BasicTable) error {

	// TODO: Synchronously invoke backend table re-load inside distributed lock

	err := shared.DeleteTable(consul, table.Name)
	if err != nil {
		return fmt.Errorf("DeleteTable error %v", err)
	}

	// Go ahead and update Consul
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
	return nil
}

// Run - Version command implementation
func (v *VersionCmd) Run(ctx *Context) error {

	fmt.Printf("Version: %s\n  Build: %s\n", Version, Build)
	return nil
}

// Run - Status command implementation
func (s *StatusCmd) Run(ctx *Context) error {

	log.Printf("Not implemented yet.")
	return nil
}

// Run - Drop command implementation
func (c *DropCmd) Run(ctx *Context) error {

	log.Printf("Connecting to Consul at: [%s] ...\n", ctx.ConsulAddr)
	consulClient, err := api.NewClient(&api.Config{Address: ctx.ConsulAddr})
	if err != nil {
		log.Printf("Is the consul agent running?")
		return fmt.Errorf("Error connecting to consul %v", err)
	}

	if err = checkForChildDependencies(consulClient, c.Table, "drop"); err != nil {
		return err
	}

	// TODO: Obtain distributed lock
	// TODO: Nuke the data

	err = shared.DeleteTable(consulClient, c.Table)
	if err != nil {
		return fmt.Errorf("DeleteTable error %v", err)
	}

	// TODO: Release the lock
	fmt.Printf("Successfully dropped table %s\n", c.Table)
	return nil
}

func checkForChildDependencies(consul *api.Client, tableName, operation string) error {

	ok, errx := shared.TableExists(consul, tableName)
	if errx != nil {
		return fmt.Errorf("tableExists error %v", errx)
	}
	if !ok {
		return fmt.Errorf("table %s doesn't exist", tableName)
	}
	dependencies, err := shared.CheckChildRelation(consul, tableName)
	if err != nil {
		return fmt.Errorf("checkChildRelation  error %v", err)
	}
	if len(dependencies) > 0 {
		fmt.Printf("Dependencies:\n")
		for _, dep := range dependencies {
			fmt.Printf("    -> %v\n", dep)
		}
		return fmt.Errorf("cannot %s table with dependencies", operation)
	}
	return nil
}

// Run - Truncate command implementation
func (c *TruncateCmd) Run(ctx *Context) error {

	log.Printf("Connecting to Consul at: [%s] ...\n", ctx.ConsulAddr)
	consulClient, err := api.NewClient(&api.Config{Address: ctx.ConsulAddr})
	if err != nil {
		log.Printf("Is the consul agent running?")
		return fmt.Errorf("Error connecting to consul %v", err)
	}

	if err = checkForChildDependencies(consulClient, c.Table, "truncate"); err != nil {
		return err
	}

	// TODO: Obtain distributed lock
	// TODO: Nuke the data only
	// TODO: Release the lock

	fmt.Printf("Successfully truncated table %s\n", c.Table)
	return nil
}

// Run - Show tables implementation
func (t *TablesCmd) Run(ctx *Context) error {

	log.Printf("Connecting to Consul at: [%s] ...\n", ctx.ConsulAddr)
	consulClient, err := api.NewClient(&api.Config{Address: ctx.ConsulAddr})
	if err != nil {
		log.Printf("Is the consul agent running?")
		return fmt.Errorf("Error connecting to consul %v", err)
	}

	tables, errx := shared.GetTables(consulClient)
	if errx != nil {
		return errx
	}
	if len(tables) == 0 {
		fmt.Printf("No Tables deployed.\n")
		return nil
	}
	fmt.Printf("Tables deployed:\n")
	for _, v := range tables {
		fmt.Printf("    -> %v\n", v)
	}
	return nil
}
