package admin

import (
	"fmt"

	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
)

// TablesCmd - Show tables command
type TablesCmd struct {
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

func GetTableNames(ctx *Context) ([]string, error) {
	consulClient, err := api.NewClient(&api.Config{Address: ctx.ConsulAddr})
	if err != nil {
		fmt.Println("Is the consul agent running?")
		return nil, fmt.Errorf("connecting to consul %v", err)
	}

	tables, errx := shared.GetTables(consulClient)
	if errx != nil {
		return nil, errx
	}
	return tables, nil
}
