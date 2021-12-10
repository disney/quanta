package shared

import (
	"encoding/json"
	"fmt"
	u "github.com/araddon/gou"
	"log"
	"os"
	"time"
)

/*
const (
    RFC3339     = "2006-01-02T15:04:05Z07:00"
)
*/

var (
	_ u.LoggerCustom = (*jsonLogger)(nil)

	logPrefix = map[int]string{
		u.FATAL: "FATAL",
		u.ERROR: "ERROR",
		u.WARN:  "WARN",
		u.INFO:  "INFO",
		u.DEBUG: "DEBUG",
	}
)

type jsonLogger struct {
	Level       string
	Environment string
	SvcName     string
	Version     string
	Product     string
}

// LogMessage - Logger message struct
type LogMessage struct {
	Environment string `json:"env"`
	Level       string `json:"level"`
	SvcName     string `json:"svc_name"`
	Version     string `json:"svc_ver"`
	Product     string `json:"product"`
	Timestamp   string `json:"timestamp"`
	Message     string `json:"msg"`
}

// Log - Log a message.
func (l *jsonLogger) Log(depth, logLevel int, msg string, fields map[string]interface{}) {
	m := &LogMessage{Environment: l.Environment, SvcName: l.SvcName, Version: l.Version, Product: l.Product}
	m.Timestamp = time.Now().UTC().Format(time.RFC3339)
	m.Message = msg
	m.Level = logPrefix[logLevel]
	o, _ := json.Marshal(m)

	switch logLevel {
	case u.NOLOGGING:
		return
	case u.FATAL, u.ERROR:
		fmt.Printf("%v\n", string(o))
		if logLevel == u.FATAL {
			os.Exit(0)
		}
	default:
		fmt.Printf("%v\n", string(o))
	}
}

// Write log output.
func (l *jsonLogger) Write(bytes []byte) (int, error) {

	l.Log(0, u.WARN, string(bytes), nil)
	return len(bytes), nil
}

// InitLogging - Initialize logging.  Call this function as early as possible in your code
func InitLogging(level string, environment string, svcName string, version string, product string) {

	c := &jsonLogger{level, environment, svcName, version, product}
	u.SetupLogging(level)
	u.SetColorOutput()
	u.SetCustomLogger(c)
	log.SetFlags(0)  // Disable standard Go logger formatting
	log.SetOutput(c) // Redirect output stream
}
