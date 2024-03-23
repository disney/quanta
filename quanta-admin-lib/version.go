package admin

import (
	"fmt"
)

// Context - Global command line variables
type Context struct {
	ConsulAddr string `help:"Consul agent address/port." default:"127.0.0.1:8500"`
	Port       int    `help:"Port number for Quanta service." default:"4000"`
	Debug      bool   `help:"Print Debug messages."`
}

// Variables to identify the build
var (
	Version string
	Build   string
)

// VersionCmd - Version command
type VersionCmd struct {
}

// Run - Version command implementation
func (v *VersionCmd) Run(ctx *Context) error {

	fmt.Printf("Version: %s\n  Build: %s\n", Version, Build)
	return nil
}
