package server

//
// This code manages the abstractions necessary for a server node.  This includes code
// to register node membership with Consul.  It is the "base class" containg common node level
// functions to support business APIs.
//

import (
	"context"
	"fmt"
	u "github.com/araddon/gou"
    pb "github.com/disney/quanta/grpc"
	"github.com/disney/quanta/shared"
    "github.com/golang/protobuf/ptypes/empty"
	"github.com/hashicorp/consul/api"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health/grpc_health_v1"
    "google.golang.org/grpc/testdata"
    "net"
    "os"
    "path"
	"time"
)

const (
	checkInterval = 5 * time.Second
	pollWait      = time.Second
    sep 		  = string(os.PathSeparator)
)

// Node is a single node in a distributed hash table, coordinated using
// services registered in Consul. Key membership is determined using rendezvous
// hashing to ensure even distribution of keys and minimal key membership
// changes when a Node fails or otherwise leaves the hash table.
//
// Errors encountered when making blocking GET requests to the Consul agent API
// are logged using the log package.
type Node struct {
	// Outbound connections to peer nodes
	*shared.Conn

	BindAddr 			string
	serviceName 		string
    dataDir  			string
    server   			*grpc.Server
    consul   			*api.Client
	hashKey				string

	// TLS options
    tls      			bool
    certFile 			string
    keyFile  			string

	// Health check endpoint
	checkURL 			string

	// Shutdown channels
	Stop 				chan bool
	Err  				chan error
}

func NewNode(port int, bindAddr, dataDir string, consul *api.Client) (*Node, error) {

	conn := shared.NewDefaultConnection()
    m := &Node{Conn: conn}
	m.ServicePort = port
    m.hashKey = path.Base(dataDir) // leaf directory name is consistent hash key
    if m.hashKey == "" || m.hashKey == "/" {
        return nil, fmt.Errorf("data dir must not be root")
    }   
    
	m.BindAddr = bindAddr
    m.dataDir = dataDir
    m.consul = consul
    var opts []grpc.ServerOption
    opts = append(opts, grpc.MaxRecvMsgSize(shared.GRPCRecvBufsize),
        grpc.MaxSendMsgSize(shared.GRPCSendBufsize))
        
    if m.tls {
        if m.certFile == "" {
            m.certFile = testdata.Path("server1.pem")
        }   
        if m.keyFile == "" {
            m.keyFile = testdata.Path("server1.key")
        }   
        creds, err := credentials.NewServerTLSFromFile(m.certFile, m.keyFile)
        if err != nil {
            return nil, fmt.Errorf("Failed to generate credentials %v", err)
        }   
        opts = append(opts, grpc.Creds(creds))
    }   
    
    m.server = grpc.NewServer(opts...)
    pb.RegisterClusterAdminServer(m.server, m)
    grpc_health_v1.RegisterHealthServer(m.server, &HealthImpl{})
    
    return m, nil
}


// Join creates a new Node and adds it to the distributed hash table specified
// by the given name. This name should be unique among all Nodes in the hash
// table.
func (n *Node) Join(name string) error {

	n.serviceName = name
	n.checkURL = "Status"
	n.Stop = make(chan bool)
	n.Err = make(chan error)

	go func() {
		defer close(n.Stop)
		n.Err <- n.Start()
	}()

	err := n.register()
	if err != nil {
		return fmt.Errorf("node: can't register %s service: %s", n.serviceName, err)
	}

	err = n.Update()
	if err != nil {
		return fmt.Errorf("node: can't fetch %s services list: %s", n.serviceName, err)
	}

	// create and start connection as client

	go n.Poll()

	return nil
}

func (n *Node) register() (err error) {

	err = n.consul.Agent().ServiceRegister(&api.AgentServiceRegistration{
		Name: n.serviceName,
		ID:   n.hashKey,
		Check: &api.AgentServiceCheck{
			GRPC:     fmt.Sprintf("%v:%v/%v", n.BindAddr, n.ServicePort, n.checkURL),
			Interval: checkInterval.String(),
		},
	})
	return err
}

// Start the node endpoint.
func (n *Node) Start() error {

    if n.ServicePort > 0 {
        lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", n.BindAddr, n.ServicePort))
        if err != nil {
            return err
        }
        n.server.Serve(lis)
    } else {
        n.server.Serve(shared.TestListener)
    }

    return nil
}

// Member returns true if the given key belongs to this Node in the distributed
// hash table.
func (n *Node) Member(key string) bool {

	if n.consul == nil {
		return true // for testing
	}
	return n.HashTable.Get(key) == n.hashKey
}

// Leave removes the Node from the distributed hash table by de-registering it
// from Consul. Once Leave is called, the Node should be discarded. An error is
// returned if the Node is unable to successfully deregister itself from
// Consul. In that case, Consul's health check for the Node will fail
func (n *Node) Leave() (err error) {
	close(n.Stop) // stop polling for state
	err = n.consul.Agent().ServiceDeregister(n.hashKey)
	//close(n.stop) FIXME: stop the server
	return err
}

// HealthImpl - Health check implementation.
type HealthImpl struct{}

// Check implements the health check interface, which directly returns to health status. There are also more complex health check strategies, such as returning based on server load.
func (h *HealthImpl) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	//u.Errorf("Health checking ...\n")
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

// Watch - Health check.
func (h *HealthImpl) Watch(req *grpc_health_v1.HealthCheckRequest, w grpc_health_v1.Health_WatchServer) error {
	return nil
}

// Status - Status API.
func (n *Node) Status(ctx context.Context, e *empty.Empty) (*pb.StatusMessage, error) {
    u.Info("Status ping returning OK.\n")
    return &pb.StatusMessage{Status: "OK"}, nil
}
