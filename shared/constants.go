package shared

import (
	"time"
)

var (
	// OpDeadline - Table operation gRPC calls should complete within 20 seconds.
	OpDeadline time.Duration = 60 * time.Second
	// Deadline - gRPC calls should complete within 500 seconds.
	Deadline time.Duration = 60 * time.Minute
	// SyncDeadline - synchronize call should complete with 20 minutes
	SyncDeadline time.Duration = 60 * time.Minute
	// DefaultPollInterval - 5 seconds between poll operations
	DefaultPollInterval time.Duration = 5 * time.Second
	// SyncRetryInterval - 1 second between sync attempts
	SyncRetryInterval time.Duration = time.Second
)

// Service - Client side services.
type Service interface {
	MemberJoined(nodeID, ipAddress string, index int)
	MemberLeft(nodeID string, index int)
}
