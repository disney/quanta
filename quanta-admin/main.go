// Quanta admin cli tool
package main

import (
	"fmt"
	"log"

	"github.com/alecthomas/kong"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
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
	Debug      bool   `help:"Print Debug messages."`
}

// VersionCmd - Version command
type VersionCmd struct {
}

// StatusCmd - Status command
type StatusCmd struct {
}

var cli struct {
	ConsulAddr  string         `default:"127.0.0.1:8500"`
	Port        int            `default:"4000"`
	Debug       bool           `default:"false"`
	Create      CreateCmd      `cmd:"" help:"Create table."`
	Drop        DropCmd        `cmd:"" help:"Drop table."`
	Truncate    TruncateCmd    `cmd:"" help:"Truncate table."`
	Status      StatusCmd      `cmd:"" help:"Show status."`
	Version     VersionCmd     `cmd:"" help:"Show version."`
	Tables      TablesCmd      `cmd:"" help:"Show tables."`
	Shutdown    ShutdownCmd    `cmd:"" help:"Shutdown cluster or one node."`
	FindKey     FindKeyCmd     `cmd:"" help:"Find nodes for key debug tool."`
	Config      ConfigCmd      `cmd:"" help:"Configuration key/value pair."`
	Verify      VerifyCmd      `cmd:"" help:"Verify data for key debug tool."`
	VerifyEnum  VerifyEnumCmd  `cmd:"" help:"Verify a string enum for key debug tool."`
	VerifyIndex VerifyIndexCmd `cmd:"" help:"Verify indices debug tool."`
}

func main() {

	ctx := kong.Parse(&cli)
	err := ctx.Run(&Context{ConsulAddr: cli.ConsulAddr, Port: cli.Port, Debug: cli.Debug})
	ctx.FatalIfErrorf(err)
}

// Run - Version command implementation
func (v *VersionCmd) Run(ctx *Context) error {

	fmt.Printf("Version: %s\n  Build: %s\n", Version, Build)
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
