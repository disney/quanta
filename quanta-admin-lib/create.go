package admin

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
)

// CreateCmd - Create command
type CreateCmd struct {
	Table     string `arg:"" name:"table" help:"Table name."`
	SchemaDir string `help:"Base directory containing schema files." default:"config"`
	Confirm   bool   `help:"Confirm deployment."`
}

const DropAttributesAllowed = false

// Run - Create command implementation
func (c *CreateCmd) Run(ctx *Context) error {

	u.Infof("Configuration directory = %s\n", c.SchemaDir)
	u.Infof("Connecting to Consul at: [%s] ...\n", ctx.ConsulAddr)
	consulClient, err := api.NewClient(&api.Config{Address: ctx.ConsulAddr})
	if err != nil {
		u.Info("Is the consul agent running?")
		return fmt.Errorf("Error connecting to consul %v", err)
	}
	table, err3 := shared.LoadSchema(c.SchemaDir, c.Table, consulClient)
	if err3 != nil {
		return fmt.Errorf("Error loading schema %v", err3)
	}
	// let's check the types of the fields
	for i := 0; i < len(table.Attributes); i++ {
		u.Info(table.Attributes[i].FieldName, " ", table.Attributes[i].Type)
		typ := shared.TypeFromString(table.Attributes[i].Type)
		if typ == shared.NotDefined && table.Attributes[i].MappingStrategy != "ChildRelation" {
			return fmt.Errorf("unknown type %s for field %s", table.Attributes[i].Type, table.Attributes[i].FieldName)
		}
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

		err = performCreate(consulClient, table, ctx.Port) // this is the create
		if err != nil {
			return fmt.Errorf("errors during performCreate: %v", err)
		}

		u.Infof("Successfully created table %s\n", table.Name)
		return nil
	} else {
		u.Debugf("Table already exists. Comparing.\n")
		// If here then table already exists.  Perform compare
		table2, err5 := shared.LoadSchema("", table.Name, consulClient)
		if err5 != nil {
			return fmt.Errorf("Error loading schema from consul %v", err5)
		}
		ok2, warnings, err6 := table2.Compare(table)
		if err6 != nil {
			if DropAttributesAllowed { // if drop attributes is allowed
				str := fmt.Sprintf("%v", err6)
				if strings.HasPrefix(str, "attribute cannot be dropped:") {
					u.Infof("Warnings: %v\n", err6)
					// do reverse compare to get dropped attributes TODO:
					ok2, warnings, err6 = table.Compare(table2)
					if err6 != nil {
						return fmt.Errorf("error comparing deployed table reverse %v", err6)
					}
				} else {
					return fmt.Errorf("error comparing deployed table %v", err6)
				}
			} else {
				return fmt.Errorf("error comparing deployed table %v", err6)
			}
		}
		if ok2 {
			u.Infof("Table already exists.  No differences detected.\n")
			return nil
		}

		// If --confirm flag not set then print warnings and exit.
		if !c.Confirm {
			u.Infof("Warnings:\n")
			for _, warning := range warnings {
				u.Infof("    -> %v\n", warning)
			}
			return fmt.Errorf("if you wish to deploy the changes then re-run with --confirm flag")
		}
		// delete attributes dropped ?

		err = performCreate(consulClient, table, ctx.Port)
		if err != nil {
			return fmt.Errorf("errors during performCreate (table exists): %v", err)
		}
		u.Infof("Successfully deployed modifications to table %s\n", table.Name)
	}
	return nil
}

func performCreate(consul *api.Client, table *shared.BasicTable, port int) error {

	lock, errx := shared.Lock(consul, "admin-tool", "admin-tool")
	if errx != nil {
		return errx
	}
	defer shared.Unlock(consul, lock)

	u.Infof("Connecting to Quanta services at port: [%d] ...\n", port)
	conn := shared.NewDefaultConnection("performCreate")
	defer conn.Disconnect()
	conn.ServicePort = port
	conn.Quorum = 3
	if err := conn.Connect(consul); err != nil {
		u.Log(u.FATAL, err)
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
	ok, warnings, err2 := table2.Compare(table)
	if err2 != nil {
		return fmt.Errorf("error comparing deployed table %v", err2)
	}
	if !ok {
		fmt.Printf("HERE Warnings:\n")
		for _, warning := range warnings {
			fmt.Printf("    -> %v\n", warning)
		}
		return fmt.Errorf("differences detected with deployed table %v", table.Name)
	}

	return services.TableOperation(table.Name, "deploy")
}
