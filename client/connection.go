package quanta

//
// The Conn struct and related functions represents an abstraction layer for connections
// between a client and server nodes in a cluster.  This includes managing group membership
// in the cluster and other administrative functions.  It does not implement a specific
// business API but provides a "base class" of functionality for doing so.  It supports the
// concept of a "masterless" architecture where each node is an active peer.  Cluster
// coordination is provided by a separate 3-5 node Consul cluster.
//

import (
	"context"
	"fmt"
	pb "github.com/disney/quanta/grpc"
	"github.com/disney/quanta/shared"
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
// clientConn - GRPC client connection wrapper.
// err - Error channel for administration API failures.
// stop - Channel for signaling cluster stop event.
// consul - Handle to Consul cluster.
// hashTable - Rendezvous hash table containing cluster members for sharding.
// pollWait - Wait interval for node membership polling events.
// nodes - List of nodes as currently registered with Consul.
// nodeMap - Map cluster node keys to connection/client arrays.
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
	admin              []pb.ClusterAdminClient
	clientConn         []*grpc.ClientConn
	err                chan error
	stop               chan struct{}
	Consul             *api.Client
	hashTable          *rendezvous.Table
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
	m.ServicePort = 5000
	m.ServerHostOverride = "x.test.youtube.com"
	m.pollWait = time.Second * 5
	m.Quorum = 1
	m.Replicas = 1
	return m
}

// Connect with configured values.
func (m *Conn) Connect(consul *api.Client) (err error) {

	if m.ServicePort > 0 {
		if consul != nil  {
			m.Consul = consul
		} else {
			m.Consul, err = api.NewClient(api.DefaultConfig())
			if err != nil {
				return fmt.Errorf("client: can't create Consul API client: %s", err)
			}
		}
		err = m.update()
		if err != nil {
			return fmt.Errorf("node: can't fetch %s services list: %s", m.ServiceName, err)
		}

		if m.hashTable == nil {
			return fmt.Errorf("node: uninitialized %s services list: %s", m.ServiceName, err)
		}

		if len(m.nodes) < m.Quorum {
			return fmt.Errorf("node: quorum size %d currently,  must be at least %d to handle requests for service %s",
				len(m.nodes), m.Quorum, m.ServiceName)
		}
		m.clientConn, err = m.CreateNodeConnections(true)
		m.admin = make([]pb.ClusterAdminClient, len(m.nodes))
		go m.poll()
	} else {
		m.hashTable = rendezvous.New([]string{"test"})
		m.nodeMap = make(map[string]int, 1)
		m.nodeMap["test"] = 0
		ctx := context.Background()
		m.clientConn = make([]*grpc.ClientConn, 1)
		m.admin = make([]pb.ClusterAdminClient, 1)
		m.clientConn[0], err = grpc.DialContext(ctx, "bufnet",
			grpc.WithDialer(shared.TestDialer), grpc.WithInsecure())
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

	if m.ServicePort == 0 {  // Test harness
		ctx := context.Background()
		nodeConns[0], err = grpc.DialContext(ctx, "bufnet",
            grpc.WithDialer(shared.TestDialer), grpc.WithInsecure())
		return 
	}

	for i := 0; i < len(m.nodes); i++ {
		if largeBuffer {
			opts = append(opts,
				grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(shared.GRPCRecvBufsize),
					grpc.MaxCallSendMsgSize(shared.GRPCSendBufsize)))
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

/*
 * Return a list of nodes for a given shard key.  The returned node list is sorted in descending order
 * highest random weight first.
 */
func (m *Conn) selectNodes(key interface{}) []pb.ClusterAdminClient {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()

	nodeKeys := m.hashTable.GetN(m.Replicas, shared.ToString(key))
	selected := make([]pb.ClusterAdminClient, len(nodeKeys))

	for i, v := range nodeKeys {
		if j, ok := m.nodeMap[v]; ok {
			selected[i] = m.admin[j]
		}
	}
	return selected
}

// Disconnect - Terminate admin connections to all cluster nodes.
func (m *Conn) Disconnect() error {

	for i := 0; i < len(m.clientConn); i++ {
		if m.clientConn[i] == nil {
			return fmt.Errorf("client: error - attempt to close nil clientConn")
		}
		m.clientConn[i].Close()
		m.admin[i] = nil
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
	fmt.Print("Checking status ... ")
	ctx, cancel := context.WithTimeout(context.Background(), Deadline*time.Second)
	defer cancel()
	status, err := client.Status(ctx, &empty.Empty{})
	if err != nil {
		log.Fatalf("%v.Status(_) = _, %v: ", client, err)
	}
	fmt.Println(status)
}

// Poll Consul for node membership list.
func (m *Conn) poll() {
	var err error

	for {
		select {
		case <-m.stop:
			return
		case err = <-m.err:
			if err != nil {
				log.Printf("[client %s] error: %s", m.ServiceName, err)
			}
			return
		case <-time.After(m.pollWait):
			err = m.update()
			if err != nil {
				log.Printf("[client %s] error: %s", m.ServiceName, err)
			}
		}
	}
}

// update blocks until the service list changes or until the Consul agent's
// timeout is reached (10 minutes by default).
func (m *Conn) update() (err error) {

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
	for k, _ := range idMap {
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
	m.hashTable = rendezvous.New(ids)
	m.waitIndex = meta.LastIndex

	return nil
}

// Nodes - Return list of active nodes.
func (m *Conn) Nodes() []*api.ServiceEntry {
    return m.nodes
}

