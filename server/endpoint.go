package server

//
// This code manages server side connection listening endpoint.
//

import (
	"context"
	"fmt"
	pb "github.com/disney/quanta/grpc"
	"github.com/disney/quanta/shared"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/testdata"
	"log"
	"net"
	"os"
	"path"
)

const (
	sep = string(os.PathSeparator)
)

// EndPoint encapsulates state for a data node endpoint.
type EndPoint struct {
	*Node
	Port     uint
	BindAddr string
	tls      bool
	certFile string
	keyFile  string
	dataDir  string
	hashKey  string
	server   *grpc.Server
	consul   *api.Client
}

// NewEndPoint construct a new data node endpoint.
func NewEndPoint(dataDir string, consul *api.Client) (*EndPoint, error) {

	m := &EndPoint{}
	m.hashKey = path.Base(dataDir) // leaf directory name is consisten hash key
	if m.hashKey == "" || m.hashKey == "/" {
		return nil, fmt.Errorf("data dir must not be root")
	}

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

// SetNode - Sets the node instance.
func (m *EndPoint) SetNode(node *Node) {
	m.Node = node
}

// Start the node endpoint.
func (m *EndPoint) Start() error {

	if m.Port > 0 {
		lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", m.BindAddr, m.Port))
		if err != nil {
			return err
		}
		m.server.Serve(lis)
	} else {
		m.server.Serve(shared.TestListener)
	}

	return nil
}

// Status - Status API.
func (m *EndPoint) Status(ctx context.Context, e *empty.Empty) (*pb.StatusMessage, error) {
	log.Printf("Status ping returning OK.\n")
	return &pb.StatusMessage{Status: "OK"}, nil
}
