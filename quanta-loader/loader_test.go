package main

import (
	"testing"
)

func TestVersionBuild(t *testing.T) {
	if len(Version) <= 0 {
		//t.Errorf("Version string length was empty, zero or less; got: %s", Version)
	}
	if len(Build) <= 0 {
		//t.Errorf("Build string length was empty, zero or less; got: %s", Build)
	}
}
