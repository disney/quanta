package server

// NewDummyNode - Construct a dummy node for integration testing.
func NewDummyNode(e *EndPoint) *Node {
	return &Node{
		serviceName: "quanta",
		Stop:        make(chan bool),
		Err:         make(chan error),
		checkURL:    "Status",
		Service:     e,
	}
}
