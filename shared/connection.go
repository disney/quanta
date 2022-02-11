package shared

//
// The Conn struct and related functions represents an abstraction layer for connections
// between a client and server nodes in a cluster.  This includes managing group membership
// in the cluster and other administrative functions.  It does not implement a specific
// business API but provides a "base class" of functionality for doing so.  It supports the
// concept of a "masterless" architecture where each node is an active peer.  Cluster
// coordination is provided by a separate 3 node Consul cluster.  Is is important to note
// that a Conn instance represents a group of connections to all of the data nodes in the
// corresponding cluster.  A Conn can be used not only for communication from external 
// sources to the cluster, but for inter-node communication as well.
//

import (
	"context"
	"fmt"
	u "github.com/araddon/gou"
	pb "github.com/disney/quanta/grpc"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/hashicorp/consul/api"
	"github.com/stvp/rendezvous"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
	"log"
	"strings"
	"sync"
	"time"
)

var (
	// Deadline - gRPC calls should complete within 500 seconds.
	Deadline time.Duration = time.Duration(500) * time.Second
)

//
// Conn - Client side cluster state and connection abstraction.
//
// ServiceName - Name of service i.e. "quanta" for registration with Consul.
// Replicas - Number of data replicas to support HA in case of node failure.
// ConsulAgentAddr - Network endpoint for Consul agent.  Defaults to "127.0.0.1:8500".
// ServicePort - Port number for the service API endpoint.
// ServerHostOverride - Hack for resolving DNS hosts for TLS implementation.
// tls - Use TLS if "true".
// certFile - Certificate file location for TLS.
// admin - Cluster adminitration API and health checks.
// ClientConn - GRPC client connection wrapper.
// err - Error channel for administration API failures.
// stop - Channel for signaling cluster stop event.
// consul - Handle to Consul cluster.
// HashTable - Rendezvous hash table containing cluster members for sharding.
// pollWait - Wait interval for node membership polling events.
// nodes - List of nodes as currently registered with Consul.
// NodeMap - Map cluster node keys to connection/client arrays.
//
type Conn struct {
	ServiceName        string
	Quorum             int
	Replicas           int
	ConsulAgentAddr    string
	ServicePort        int
	ServerHostOverride string
	tls                bool
	certFile           string
	Admin              []pb.ClusterAdminClient
	clientConn         []*grpc.ClientConn
	err                chan error
	stop               chan struct{}
	Consul             *api.Client
	HashTable          *rendezvous.Table
	waitIndex          uint64
	pollWait           time.Duration
	nodes              []*api.ServiceEntry
	nodeMap            map[string]int
	nodeMapLock        sync.RWMutex
}

// NewDefaultConnection - Configure a connection with default values.
func NewDefaultConnection() *Conn {
	m := &Conn{}
	m.ServiceName = "quanta"
	m.ServicePort = 4000
	m.ServerHostOverride = "x.test.youtube.com"
	m.pollWait = time.Second * 5
	m.Quorum = 3
	m.Replicas = 2
	return m
}

// Connect with configured values.
func (m *Conn) Connect(consul *api.Client) (err error) {

	if m.ServicePort > 0 {
		if consul != nil {
			m.Consul = consul
		} else {
			m.Consul, err = api.NewClient(api.DefaultConfig())
			if err != nil {
				return fmt.Errorf("client: can't create Consul API client: %s", err)
			}
		}
		err = m.Update()
		if err != nil {
			return fmt.Errorf("node: can't fetch %s services list: %s", m.ServiceName, err)
		}

		if m.HashTable == nil {
			return fmt.Errorf("node: uninitialized %s services list: %s", m.ServiceName, err)
		}

		if len(m.nodes) < m.Quorum {
			return fmt.Errorf("node: quorum size %d currently,  must be at least %d to handle requests for service %s",
				len(m.nodes), m.Quorum, m.ServiceName)
		}
		m.clientConn, err = m.CreateNodeConnections(true)
		m.Admin = make([]pb.ClusterAdminClient, len(m.clientConn))
    	for i := 0; i < len(m.clientConn); i++ {
        	m.Admin[i] = pb.NewClusterAdminClient(m.clientConn[i])
    	}
		go m.Poll()
	} else {
		m.HashTable = rendezvous.New([]string{"test"})
		m.nodeMap = make(map[string]int, 1)
		m.nodeMap["test"] = 0
		ctx := context.Background()
		m.clientConn = make([]*grpc.ClientConn, 1)
		m.Admin = make([]pb.ClusterAdminClient, 1)
        m.Admin[0] = pb.NewClusterAdminClient(m.clientConn[0])
		m.clientConn[0], err = grpc.DialContext(ctx, "bufnet",
			grpc.WithDialer(TestDialer), grpc.WithInsecure())
		if err != nil {
			err = fmt.Errorf("Failed to dial bufnet: %v", err)
		}
	}

	if err == nil {
		m.stop = make(chan struct{})
	}
	return
}

// CreateNodeConnections - Open a set of GRPC connections to all data nodes
func (m *Conn) CreateNodeConnections(largeBuffer bool) (nodeConns []*grpc.ClientConn, err error) {

	nodeCount := len(m.nodes)
	if nodeCount == 0 {
		nodeCount = 1
	}
	nodeConns = make([]*grpc.ClientConn, nodeCount)
	var opts []grpc.DialOption

	if m.ServicePort == 0 { // Test harness
		ctx := context.Background()
		nodeConns[0], err = grpc.DialContext(ctx, "bufnet",
			grpc.WithDialer(TestDialer), grpc.WithInsecure())
		return
	}

	for i := 0; i < len(m.nodes); i++ {
		if largeBuffer {
			opts = append(opts,
				grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(GRPCRecvBufsize),
					grpc.MaxCallSendMsgSize(GRPCSendBufsize)))
		}
		if m.tls {
			if m.certFile == "" {
				m.certFile = testdata.Path("ca.pem")
			}
			creds, err2 := credentials.NewClientTLSFromFile(m.certFile, m.ServerHostOverride)
			if err2 != nil {
				err = fmt.Errorf("Failed to create TLS credentials %v", err2)
				return
			}
			opts = append(opts, grpc.WithTransportCredentials(creds))
		} else {
			opts = append(opts, grpc.WithInsecure())
		}

		nodeConns[i], err = grpc.Dial(fmt.Sprintf("%s:%d", m.nodes[i].Node.Address,
			m.ServicePort), opts...)
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
	}
	return
}

// SelectNodes - Return a list of nodes for a given shard key.  The returned node list is sorted in descending order
// highest random weight first.
// Resolve the node location(s) of a single key. If 'all' == true than all nodes are selected.
func (m *Conn) SelectNodes(key interface{}, onlyPrimary, all bool) []int {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()

	replicas := m.Replicas
	if all && len(m.nodes) > 0 {
		replicas = len(m.nodes)
	}
	if onlyPrimary {
		replicas = 1
	}

	nodeKeys := m.HashTable.GetN(replicas, ToString(key))
	indices := make([]int, replicas)

	for i, v := range nodeKeys {
		if j, ok := m.nodeMap[v]; ok {
			indices[i] = j
		}
	}
	return indices
}

// Disconnect - Terminate admin connections to all cluster nodes.
func (m *Conn) Disconnect() error {

	for i := 0; i < len(m.clientConn); i++ {
		if m.clientConn[i] == nil {
			return fmt.Errorf("client: error - attempt to close nil clientConn")
		}
		m.clientConn[i].Close()
	}
	select {
	case _, open := <-m.stop:
		if open {
			close(m.stop)
			m.stop = nil
		}
	default:
		if m.stop != nil {
			close(m.stop)
		}
	}
	return nil
}

// Status admin API.  TODO: Make this useful
func printStatus(client pb.ClusterAdminClient) {
	u.Infof("Checking status ... ")
	ctx, cancel := context.WithTimeout(context.Background(), Deadline*time.Second)
	defer cancel()
	status, err := client.Status(ctx, &empty.Empty{})
	if err != nil {
		log.Fatalf("%v.Status(_) = _, %v: ", client, err)
	}
	u.Info(status)
}

// Poll Consul for node membership list.
func (m *Conn) Poll() {
	var err error

	for {
		select {
		case <-m.stop:
			return
		case err = <-m.err:
			if err != nil {
				u.Errorf("[client %s] error: %s", m.ServiceName, err)
			}
			return
		case <-time.After(m.pollWait):
			err = m.Update()
			if err != nil {
				u.Errorf("[client %s] error: %s", m.ServiceName, err)
			}
		}
	}
}

// Update blocks until the service list changes or until the Consul agent's
// timeout is reached (10 minutes by default).
func (m *Conn) Update() (err error) {

	opts := &api.QueryOptions{WaitIndex: m.waitIndex}
	serviceEntries, meta, err := m.Consul.Health().Service(m.ServiceName, "", true, opts)
	if err != nil {
		return err
	}
	if serviceEntries == nil {
		return nil
	}

	m.nodeMapLock.Lock()
	defer m.nodeMapLock.Unlock()

	ids := make([]string, 0)
	idMap := make(map[string]struct{})
	for _, entry := range serviceEntries {
		node := strings.Split(entry.Node.Node, ".")[0]
		idMap[node] = struct{}{}
	}
	for k := range idMap {
		ids = append(ids, k)
	}

	m.nodes = make([]*api.ServiceEntry, 0)
	m.nodeMap = make(map[string]int)
	i := 0
	for _, entry := range serviceEntries {
		if _, found := idMap[entry.Service.ID]; found {
			if _, found := m.nodeMap[entry.Service.ID]; !found {
				m.nodes = append(m.nodes, entry)
				m.nodeMap[entry.Service.ID] = i
				i++
			}
		}
	}
	m.HashTable = rendezvous.New(ids)
	m.waitIndex = meta.LastIndex

	return nil
}

// Nodes - Return list of active nodes.
func (m *Conn) Nodes() []*api.ServiceEntry {
	return m.nodes
}

// ClientConnections - Return client connections.
func (m *Conn) ClientConnections() []*grpc.ClientConn {
	return m.clientConn
}
