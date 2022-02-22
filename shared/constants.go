package shared

import (
	"time"
)

var (
    // Deadline - gRPC calls should complete within 500 seconds.
    Deadline time.Duration = 500 * time.Second
    SyncDeadline time.Duration = 20 * time.Minute
	DefaultPollInterval time.Duration = 5 * time.Second
)

type Service interface {
	MemberJoined(nodeId, ipAddress string, index int)
	MemberLeft(nodeId string, index int)
}
