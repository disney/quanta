package shared
// Shared buffer for in-memory stack testing

import (
	"google.golang.org/grpc/test/bufconn"
	"net"
	"time"
)

var (
	//GRPCRecvBufsize = 1024*1024*156
	GRPCRecvBufsize = 1024 * 1024 * 1024 * 3
	//GRPCSendBufsize = 1024*1024*64
	GRPCSendBufsize = 1024 * 1024 * 1024 * 3
    // TestListener endpoint
	TestListener      *bufconn.Listener
)

func init() {
	TestListener = bufconn.Listen(GRPCRecvBufsize)
}

// TestDialer - Wrapper around connection for simulating network.
func TestDialer(string, time.Duration) (net.Conn, error) {
	return TestListener.Dial()
}
