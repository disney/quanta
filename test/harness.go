// Package test - This code creates an in-memory "stack" and loads test data.
package test

import (
	"os"
	"path/filepath"

	"github.com/disney/quanta/server"
)

// Setup - Initialize test harness
func Setup() (*server.Node, error) {

	// Enable in memory instance
	node, err := server.NewNode("TEST", 0, "", "./testdata", nil)
	if err != nil {
		return nil, err
	}
	bitmapIndex := server.NewBitmapIndex(node, 0)
	err = bitmapIndex.Init()
	if err != nil {
		return nil, err
	}
	_, err = server.NewStringSearch(node)
	if err != nil {
		return nil, err
	}
	_, err = server.NewKVStore(node)
	if err != nil {
		return nil, err
	}
	go func() {
		node.Start()
	}()
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
