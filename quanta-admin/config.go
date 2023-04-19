package main

import (
	"fmt"
	"strconv"

	proxy "github.com/disney/quanta/quanta-proxy-lib"
	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
)

// ConfigCmd - Configuration  command
type ConfigCmd struct {
	Key   string `help:"Parameter name."`
	Value string `help:"Parameter value."`
}

// Run - Config command implementation.
func (c *ConfigCmd) Run(ctx *proxy.Context) error {

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
