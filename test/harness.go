// Package test - This code creates an in-memory "stack" and loads test data.
package test

import (
	"os"
	"path/filepath"

	"github.com/disney/quanta/server"
)

// Setup - Initialize test harness
func Setup() (*server.Node, error) {

	os.Mkdir("./testdata/bitmap", 0755)
	// Enable in memory instance
	node, err := server.NewNode("TEST", 0, "", "./testdata", nil)
	if err != nil {
		return nil, err
	}
	kvStore := server.NewKVStore(node)
	node.AddNodeService(kvStore)
	search := server.NewStringSearch(node)
	node.AddNodeService(search)
	bitmapIndex := server.NewBitmapIndex(node, 0)
	node.AddNodeService(bitmapIndex)
	go func() {
		node.Start()
	}()
	err = node.InitServices()
	if err != nil {
		return nil, err
	}
	return node, nil
}

// RemoveContents - Remove local data files.
func RemoveContents(path string) error {
	files, err := filepath.Glob(path)
	if err != nil {
		return err
	}
	for _, file := range files {
		err = os.RemoveAll(file)
		if err != nil {
			return err
		}
	}
	return nil
}
