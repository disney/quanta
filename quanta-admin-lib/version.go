package admin

import (
	"fmt"
	"log"

	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
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

// GetClientConnection - Create a connection to the Quanta service
// The 'owner' is used for debugging.
func GetClientConnection(consulAddr string, port int, owner string) *shared.Conn {

	fmt.Printf("Connecting to Consul at: [%s] ...\n", consulAddr)
	consulClient, err := api.NewClient(&api.Config{Address: consulAddr})
	if err != nil {
		fmt.Println("Is the consul agent running?")
		log.Fatal(err)
	}
	fmt.Printf("Connecting to Quanta services at port: [%d] ...\n", port)
	conn := shared.NewDefaultConnection(owner)
	conn.ServicePort = port
	conn.Quorum = 0
	if err := conn.Connect(consulClient); err != nil {
		log.Fatal(err)
	}
	return conn
}
