// Quanta admin cli tool
package main

import (
	"context"
	"fmt"
	"github.com/alecthomas/kong"
	pb "github.com/disney/quanta/grpc"
	"github.com/disney/quanta/shared"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/hashicorp/consul/api"
	"log"
	"strconv"
	"time"
)

// Variables to identify the build
var (
	Version string
	Build   string
)

// Context - Global command line variables
type Context struct {
	ConsulAddr string `help:"Consul agent address/port." default:"127.0.0.1:8500"`
	Port       int    `help:"Port number for Quanta service." default:"4000"`
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
	Table       string `arg name:"table" help:"Table name."`
	RetainEnums bool   `help:"Retain enumeration data for StringEnum types."`
	Force       bool   `help:"Force override of constraints."`
}

// TablesCmd - Show tables command
type TablesCmd struct {
}

// ShutdownCmd - Shutdown command
type ShutdownCmd struct {
	NodeIP string `arg name:"node-ip" help:"IP address of node to shutdown or ALL."`
}

// FindKeyCmd - Find key command
type FindKeyCmd struct {
	Table     string `arg name:"table" help:"Table name."`
	Field     string `arg name:"field" help:"Field name."`
	RowID     uint64 `help:"Row id. (Omit for BSI)"`
	Timestamp string `help:"Time quantum value. (Omit for no quantum)"`
}

// ConfigCmd - Configuration  command
type ConfigCmd struct {
	Key   string `help:"Parameter name."`
	Value string `help:"Parameter value."`
}

var cli struct {
	ConsulAddr string      `default:"127.0.0.1:8500"`
	Port       int         `default:"4000"`
	Create     CreateCmd   `cmd help:"Create table."`
	Drop       DropCmd     `cmd help:"Drop table."`
	Truncate   TruncateCmd `cmd help:"Truncate table."`
	Status     StatusCmd   `cmd help:"Show status."`
	Version    VersionCmd  `cmd help:"Show version."`
	Tables     TablesCmd   `cmd help:"Show tables."`
	Shutdown   ShutdownCmd `cmd help:"Shutdown cluster or one node."`
	FindKey    FindKeyCmd  `cmd help:"Find nodes for key debug tool."`
	Config     ConfigCmd   `cmd help:"Configuration key/value pair."`
}

func main() {

	ctx := kong.Parse(&cli)
	err := ctx.Run(&Context{ConsulAddr: cli.ConsulAddr, Port: cli.Port})
	ctx.FatalIfErrorf(err)
}

// Run - Create command implementation
func (c *CreateCmd) Run(ctx *Context) error {

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
	services := shared.NewBitmapIndex(conn, 3000000)

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

// Run - Version command implementation
func (v *VersionCmd) Run(ctx *Context) error {

	fmt.Printf("Version: %s\n  Build: %s\n", Version, Build)
	return nil
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

// Run - Drop command implementation
func (c *DropCmd) Run(ctx *Context) error {

	fmt.Printf("Connecting to Consul at: [%s] ...\n", ctx.ConsulAddr)
	consulClient, err := api.NewClient(&api.Config{Address: ctx.ConsulAddr})
	if err != nil {
		fmt.Println("Is the consul agent running?")
		return fmt.Errorf("Error connecting to consul %v", err)
	}

	if err = checkForChildDependencies(consulClient, c.Table, "drop"); err != nil {
		return err
	}

	lock, errx := shared.Lock(consulClient, "admin-tool", "admin-tool")
	if errx != nil {
		return errx
	}
	defer shared.Unlock(consulClient, lock)
	err = nukeData(consulClient, ctx.Port, c.Table, "drop", false)
	if err != nil {
		return err
	}
	err = shared.DeleteTable(consulClient, c.Table)
	if err != nil {
		return fmt.Errorf("DeleteTable error %v", err)
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

func nukeData(consul *api.Client, port int, tableName, operation string, retainEnums bool) error {

	fmt.Printf("Connecting to Quanta services at port: [%d] ...\n", port)
	conn := shared.NewDefaultConnection()
	conn.ServicePort = port
	conn.Quorum = 3
	if err := conn.Connect(consul); err != nil {
		log.Fatal(err)
	}
	services := shared.NewBitmapIndex(conn, 3000000)
	kvStore := shared.NewKVStore(conn)
	err := services.TableOperation(tableName, operation)
	if err != nil {
		return fmt.Errorf("TableOperation error %v", err)
	}
	err = kvStore.DeleteIndicesWithPrefix(tableName, retainEnums)
	if err != nil {
		return fmt.Errorf("DeleteIndicesWithPrefix error %v", err)
	}
	return nil
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

	err = nukeData(consulClient, ctx.Port, c.Table, "truncate", c.RetainEnums)
	if err != nil {
		return err
	}

	fmt.Printf("Successfully truncated table %s\n", c.Table)
	return nil
}

// Run - Show tables implementation
func (t *TablesCmd) Run(ctx *Context) error {

	fmt.Printf("Connecting to Consul at: [%s] ...\n", ctx.ConsulAddr)
	consulClient, err := api.NewClient(&api.Config{Address: ctx.ConsulAddr})
	if err != nil {
		fmt.Println("Is the consul agent running?")
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

func getClientConnection(consulAddr string, port int) *shared.Conn {

	fmt.Printf("Connecting to Consul at: [%s] ...\n", consulAddr)
	consulClient, err := api.NewClient(&api.Config{Address: consulAddr})
	if err != nil {
		fmt.Println("Is the consul agent running?")
		log.Fatal(err)
	}
	fmt.Printf("Connecting to Quanta services at port: [%d] ...\n", port)
	conn := shared.NewDefaultConnection()
	conn.ServicePort = port
	conn.Quorum = 0
	if err := conn.Connect(consulClient); err != nil {
		log.Fatal(err)
	}
	return conn
}

// Run - Shutdown command implementation
func (f *FindKeyCmd) Run(ctx *Context) error {

	conn := getClientConnection(ctx.ConsulAddr, ctx.Port)
	table, err := shared.LoadSchema("", f.Table, conn.Consul)
	if err != nil {
		return fmt.Errorf("Error loading table %s - %v", f.Table, err)
	}
	field, err2 := table.GetAttribute(f.Field)
	if err2 != nil {
		return fmt.Errorf("Error getting field  %s - %v", f.Field, err2)
	}
	if f.RowID > 0 && field.IsBSI() {
		return fmt.Errorf("Field is a BSI and Rowid was specified, ignoring Rowid")
	}
	if f.Timestamp != "" && table.TimeQuantumType == "" {
		return fmt.Errorf("Table does not have time quantum, ignoring timestamp")
	}
	if table.TimeQuantumType != "" && f.Timestamp == "" {
		return fmt.Errorf("Table has time quantum type %s but timestamp not provided", table.TimeQuantumType)
	}
	ts, tf, err3 := shared.ToTQTimestamp(table.TimeQuantumType, f.Timestamp)
	if err3 != nil {
		return fmt.Errorf("Error ToTQTimestamp %v - TQType = %s, Timestamp = %s", err3, table.TimeQuantumType, f.Timestamp)
	}
	if f.RowID == 0 {
		f.RowID = 1
	}
	var key string
	if field.IsBSI() {
		key = fmt.Sprintf("%s/%s/%s", f.Table, f.Field, tf)
	} else {
		key = fmt.Sprintf("%s/%s/%d/%s", f.Table, f.Field, f.RowID, tf)
	}

	fmt.Println("")
	fmt.Printf("KEY = %s\n", key)
	cx, cancel := context.WithTimeout(context.Background(), shared.Deadline)
	defer cancel()
	indices := conn.SelectNodes(key, false, false)
	bitClient := shared.NewBitmapIndex(conn, 0)
	req := &pb.SyncStatusRequest{Index: f.Table, Field: f.Field, RowId: f.RowID, Time: ts.UnixNano()}
	fmt.Println("")
	fmt.Println("REPLICA   ADDRESS            STATUS   CARDINALITY   MODTIME")
	fmt.Println("=======   ================   ======   ===========   =======")
	for i, index := range indices {
		var status, ip, modTime string
		var card uint64
		if result, err := conn.Admin[index].Status(cx, &empty.Empty{}); err != nil {
			fmt.Printf(fmt.Sprintf("%v.Status(_) = _, %v, node = %s\n", conn.Admin[index], err,
				conn.ClientConnections()[index].Target()))
		} else {
			status = result.NodeState
			ip = result.LocalIP
		}
		if res, err2 := bitClient.Client(index).SyncStatus(cx, req); err2 != nil {
			fmt.Printf(fmt.Sprintf("%v.SyncStatus(_) = _, %v, node = %s\n", bitClient.Client(index), err2,
				conn.ClientConnections()[index].Target()))
		} else {
			if res.Cardinality == 0 && res.ModTime == 0 {
				status = "Missing"
			} else {
				status = "OK"
				card = res.Cardinality
				ts := time.Unix(0, res.ModTime)
				modTime = ts.Format(time.RFC3339)
			}
		}
		fmt.Printf("%-7d   %-16s   %-7s  %-11d   %s\n", i+1, ip, status, card, modTime)
	}
	return nil
}

// Run - Config command implemetation.
func (c *ConfigCmd) Run(ctx *Context) error {

	fmt.Printf("Connecting to Consul at: [%s] ...\n", ctx.ConsulAddr)
	consulClient, err := api.NewClient(&api.Config{Address: ctx.ConsulAddr})
	if err != nil {
		fmt.Println("Is the consul agent running?")
		return fmt.Errorf("Error connecting to consul %v", err)
	}
	if c.Key == "" {
		return fmt.Errorf("--key should be one of cluster-size-target, etc")
	}
	if c.Key == "cluster-size-target" {
		if c.Value != "" {
			val, err := strconv.Atoi(c.Value)
			if err != nil {
				return fmt.Errorf("cant parse config value %s for key %s - %v", c.Value, c.Key, err)
			}
			err = shared.SetClusterSizeTarget(consulClient, val)
			if err != nil {
				return fmt.Errorf("error setting value %s for key %s - %v", c.Value, c.Key, err)
			}
		}
		val, err := shared.GetClusterSizeTarget(consulClient)
		if err != nil {
			return fmt.Errorf("Error getting configuration value %v - %v", c.Key, err)
		}
		if c.Value == "" {
			fmt.Printf("cluster-size-target = %v\n", val)
		} else {
			fmt.Printf("cluster-size-target is now set = %v\n", val)
		}
	}
	return nil
}
