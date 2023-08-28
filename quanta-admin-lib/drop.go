package admin

import (
	"fmt"
	"log"
	"time"

	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
)

// DropCmd - Drop command
type DropCmd struct {
	Table string `arg:"" name:"table" help:"Table name."`
}

// Run - Drop command implementation
func (c *DropCmd) Run(ctx *Context) error {

	fmt.Printf("Connecting to Consul at: [%s] ...\n", ctx.ConsulAddr)
	consulClient, err := api.NewClient(&api.Config{Address: ctx.ConsulAddr})
	if err != nil {
		fmt.Println("Is the consul agent running?")
		return fmt.Errorf("Error connecting to consul %v", err)
	}

	if ctx.Debug {
		fmt.Println("Checking for child dependencies.")
	}
	if err = checkForChildDependencies(consulClient, c.Table, "drop"); err != nil {
		return err
	}

	if ctx.Debug {
		fmt.Println("Acquiring distributed lock.")
	}
	lock, errx := shared.Lock(consulClient, "admin-tool", "admin-tool")
	if errx != nil {
		return errx
	}
	defer shared.Unlock(consulClient, lock)

	if ctx.Debug {
		fmt.Println("Calling DeleteTable to remove table from consul and notify upstream clients.")
	}

	err = shared.DeleteTable(consulClient, c.Table)
	if err != nil {
		return fmt.Errorf("DeleteTable error %v", err)
	}

	// Give consumers time to flush and restart.
	time.Sleep(time.Second * 5)

	if ctx.Debug {
		fmt.Println("Calling nukeData.")
	}

	err = nukeData(consulClient, ctx.Port, c.Table, "drop", false)
	if err != nil {
		return err
	}

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

func nukeData(consul *api.Client, port int, tableName, operation string, dropEnums bool) error {

	fmt.Printf("Connecting to Quanta services at port: [%d] ...\n", port)
	conn := shared.NewDefaultConnection()
	conn.ServicePort = port
	conn.Quorum = 3
	if err := conn.Connect(consul); err != nil {
		log.Fatal(err)
	}
	services := shared.NewBitmapIndex(conn)
	kvStore := shared.NewKVStore(conn)
	err := services.TableOperation(tableName, operation)
	if err != nil {
		return fmt.Errorf("TableOperation error %v", err)
	}
	retainEnums := true
	if dropEnums || operation == "drop" {
		retainEnums = false
	}
	err = kvStore.DeleteIndicesWithPrefix(tableName, retainEnums)
	if err != nil {
		return fmt.Errorf("DeleteIndicesWithPrefix error %v", err)
	}
	return nil
}
