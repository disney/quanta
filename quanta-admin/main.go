// Quanta admin cli tool
package main

import (
	"github.com/alecthomas/kong"
	admin "github.com/disney/quanta/quanta-admin-lib"
)

func main() {

	ctx := kong.Parse(&admin.Cli)
	err := ctx.Run(&admin.Context{ConsulAddr: admin.Cli.ConsulAddr, Port: admin.Cli.Port, Debug: admin.Cli.Debug})
	ctx.FatalIfErrorf(err)
}
