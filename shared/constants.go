package shared

import (
	"time"
)

var (
	// Deadline - gRPC calls should complete within 500 seconds.
	Deadline time.Duration = 500 * time.Second
	// SyncDeadline - synchronize call should complete with 20 minutes
	SyncDeadline time.Duration = 20 * time.Minute
	// DefaultPollInterval - 5 seconds between poll operations
	DefaultPollInterval time.Duration = 5 * time.Second
	// SyncRetryInterval - 1 second between sync attempts
	SyncRetryInterval   time.Duration = time.Second
)

// Service - Client side services.
type Service interface {
	MemberJoined(nodeID, ipAddress string, index int)
	MemberLeft(nodeID string, index int)
}
