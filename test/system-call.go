package test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"time"
)

// Quiet for when we don't like the echo
var Quiet = true

// SuperQuiet = Don't even show the stdout
var SuperQuiet = false

// Shell is really just another shell gadget.
// Returns what the command outputs and not before it's done.
// If input is not empty, it will be piped to the command
func Shell(command string, input string) (string, error) {
	if !Quiet {
		fmt.Println(">" + command)
	}
	cmd := exec.Command("bash", "-c", command)
	if input != "" {
		stdin, err := cmd.StdinPipe()
		if err != nil {
			check(err)
		}
		go func() {
			defer stdin.Close()
			n, err := io.WriteString(stdin, input)
			fmt.Println("io.WriteString> ", n, " ", err)
		}()
	}

	var timer *time.Timer
	timer = time.AfterFunc(120*time.Second, func() {
		timer.Stop()
		cmd.Process.Kill()
	})

	out, err := myCombinedOutput(cmd)
	return string(out), err
}

type buffWriter struct {
	b bytes.Buffer
}

func (bw *buffWriter) Write(barr []byte) (int, error) {
	if !SuperQuiet {
		fmt.Println("Shell>>", string(barr))
	}
	return bw.b.Write(barr)
}

func myCombinedOutput(c *exec.Cmd) ([]byte, error) {
	if c.Stdout != nil {
		return nil, errors.New("exec: Stdout already set")
	}
	if c.Stderr != nil {
		return nil, errors.New("exec: Stderr already set")
	}
	var b bytes.Buffer
	bw := buffWriter{b}
	c.Stdout = &bw
	c.Stderr = &bw
	err := c.Run()
	return bw.b.Bytes(), err
}

// Sh - a shorter version of shell
func Sh(command string) {
	if !Quiet {
		fmt.Println(">" + command)
	}
	out, err := Shell(command, "")
	if err != nil {
		fmt.Println(">ERROR:", err, out)
	}
	if !Quiet {
		fmt.Println("", out)
	}
}
