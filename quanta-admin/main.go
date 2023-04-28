// Quanta admin cli tool
package main

import (
	"fmt"

	"github.com/alecthomas/kong"
	proxy "github.com/disney/quanta/quanta-proxy-lib"

	admin "github.com/disney/quanta/quanta-admin-lib"
)

var cli struct {
	ConsulAddr  string               `default:"127.0.0.1:8500"`
	Port        int                  `default:"4000"`
	Debug       bool                 `default:"false"`
	Create      admin.CreateCmd      `cmd:"" help:"Create table."`
	Drop        admin.DropCmd        `cmd:"" help:"Drop table."`
	Truncate    admin.TruncateCmd    `cmd:"" help:"Truncate table."`
	Status      admin.StatusCmd      `cmd:"" help:"Show status."`
	Version     VersionCmd           `cmd:"" help:"Show version."`
	Tables      admin.TablesCmd      `cmd:"" help:"Show tables."`
	Shutdown    admin.ShutdownCmd    `cmd:"" help:"Shutdown cluster or one node."`
	FindKey     admin.FindKeyCmd     `cmd:"" help:"Find nodes for key debug tool."`
	Config      admin.ConfigCmd      `cmd:"" help:"Configuration key/value pair."`
	Verify      admin.VerifyCmd      `cmd:"" help:"Verify data for key debug tool."`
	VerifyEnum  admin.VerifyEnumCmd  `cmd:"" help:"Verify a string enum for key debug tool."`
	VerifyIndex admin.VerifyIndexCmd `cmd:"" help:"Verify indices debug tool."`
}

func main() {

	ctx := kong.Parse(&cli)
	err := ctx.Run(&proxy.Context{ConsulAddr: cli.ConsulAddr, Port: cli.Port, Debug: cli.Debug})
	ctx.FatalIfErrorf(err)
}

// VersionCmd - Version command
type VersionCmd struct {
}

// Run - Version command implementation
func (v *VersionCmd) Run(ctx *proxy.Context) error {

	fmt.Printf("Version: %s\n  Build: %s\n", proxy.Version, proxy.Build)
	return nil
}
