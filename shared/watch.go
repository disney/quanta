package shared

import (
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"log"
	"strings"
)

// Inspired by https://github.com/dwijnand/abactis

// EventType - Schema change event types
type EventType int

// Constant defines for data type.
const (
	Create = EventType(iota)
	Modify
	Drop
)

type SchemaChangeEvent struct {
	Table string
	Event EventType
}

type SchemaChangeListener func(event SchemaChangeEvent)

func RegisterSchemaChangeListener(conf *api.Config, cb SchemaChangeListener) error {

	watchParams := make(map[string]interface{})
	watchParams["type"] = "keyprefix"
	watchParams["prefix"] = "schema"

	watch, err := watch.Parse(watchParams)
	if err != nil {
		return err
	}

	watch.Handler = makeKvPairsHandler(conf, cb)

	go func() {
		err = watch.Run(conf.Address)
		if err != nil {
			log.Fatal(err)
		}
	}()
	return nil
}

func makeKvPairsHandler(conf *api.Config, cb SchemaChangeListener) watch.HandlerFunc {

	client, err := api.NewClient(conf)
	if err != nil {
		log.Fatal(err)
	}

	kv := client.KV()
	oldKvPairs, _, err := kv.List("schema", nil)
	if err != nil {
		log.Fatal(err)
	}
	oldUMap := makeUniquesMap(oldKvPairs)

	return func(index uint64, result interface{}) {

		newKvPairs := result.(api.KVPairs)
		newUMap := makeUniquesMap(newKvPairs)

		for k := range oldUMap {
			if _, found := newUMap[k]; !found {
				cb(SchemaChangeEvent{Table: k, Event: Drop})
			}
		}
		for k := range newUMap {
			if _, found := oldUMap[k]; !found {
				cb(SchemaChangeEvent{Table: k, Event: Create})
			}
		}
		oldKvPairs = newKvPairs
		oldUMap = newUMap
	}
}

func makeUniquesMap(kvPairs api.KVPairs) map[string]struct{} {
	uMap := make(map[string]struct{})
	for _, kvPair := range kvPairs {
		s := strings.Split(kvPair.Key, SEP)
		uMap[s[1]] = struct{}{}
	}
	return uMap
}
