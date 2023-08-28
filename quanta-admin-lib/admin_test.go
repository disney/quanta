package admin

import (
	"strings"
	"testing"

	"github.com/alecthomas/kong"
)

// Demo of how to call admin directly w/o any command line.

// invoke admin w/o command line interface
func TestVersion(t *testing.T) {

	cmd := "version"
	cmds := strings.Split(cmd, " ")

	parser, err := kong.New(&Cli)

	if err != nil {
		t.Error(err)
	}
	ctx, err := parser.Parse(cmds) // os.Args[1:])
	parser.FatalIfErrorf(err)

	err = ctx.Run(&Context{ConsulAddr: Cli.ConsulAddr, Port: Cli.Port, Debug: Cli.Debug})
	if err != nil {
		t.Error(err)
	}

}
