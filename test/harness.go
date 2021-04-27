/**
 * This code creates an in-memory "stack" and loads test data.
 */
package test

import (
	"os"
	"path/filepath"

	"github.com/disney/quanta/server"
)

func Setup() (*server.EndPoint,  error) {

	endpoint, err := server.NewEndPoint("./testdata")
	if err != nil {
		return nil, err
	}
	endpoint.Port = 0 // Enable in memory instance
	endpoint.SetNode(server.NewDummyNode(endpoint))
	bitmapIndex := server.NewBitmapIndex(endpoint, 0)
	err = bitmapIndex.Init()
	if err != nil {
		return nil, err
	}
	_, err = server.NewStringSearch(endpoint)
	if err != nil {
		return nil, err
	}
	_, err = server.NewKVStore(endpoint)
	if err != nil {
		return nil, err
	}
	go func() {
		endpoint.Start()
	}()
	return endpoint, nil
}

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
