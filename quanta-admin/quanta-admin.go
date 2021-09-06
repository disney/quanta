// Quanta admin cli tool
package main

import (
	"fmt"
	"github.com/alecthomas/kong"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
	"gopkg.in/yaml.v2"
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

// DeployCmd - Deploy command
type DeployCmd struct {
	Table     string `arg name:"table" help:"Table name."`
	SchemaDir string `help:"Base directory containing schema files." default:"./config"`
}

// VersionCmd - Version command
type VersionCmd struct {
}

var cli struct {
	ConsulAddr string
	Deploy     DeployCmd  `cmd help:"Deploy table schema."`
	Status     StatusCmd  `cmd help:"Show status."`
	Version    VersionCmd `cmd help:"Show version."`
}

func main() {

	ctx := kong.Parse(&cli)
	err := ctx.Run(&Context{ConsulAddr: cli.ConsulAddr})
	ctx.FatalIfErrorf(err)
}

// Run - Deploy command implementation
func (c *DeployCmd) Run(ctx *Context) error {

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
	err4 := shared.MarshalConsul(table, consulClient)
	if err4 != nil {
		return fmt.Errorf("Error marshalling table %v", err4)
	}

	table2, err5 := shared.LoadSchema("", c.Table, consulClient)
	if err5 != nil {
		return fmt.Errorf("Error loading schema from consul %v", err5)
	}
	data, err := yaml.Marshal(table2)
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", string(data))
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
