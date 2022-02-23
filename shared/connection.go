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
	_ "github.com/golang/protobuf/ptypes/empty"
	"github.com/hashicorp/consul/api"
	"github.com/stvp/rendezvous"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"
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
// Err - Error channel for administration API failures.
// Stop - Channel for signaling cluster Stop event.
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
	grpcOpts           []grpc.DialOption
	Admin              []pb.ClusterAdminClient
	clientConn         []*grpc.ClientConn
	Err                chan error
	Stop               chan struct{}
	Consul             *api.Client
	HashTable          *rendezvous.Table
	waitIndex          uint64
	pollWait           time.Duration
	nodes              []*api.ServiceEntry
	nodeMap            map[string]int
	nodeMapLock        sync.RWMutex
	registeredServices map[string]Service
	idMap              map[string]*api.ServiceEntry
	ids                []string
}

// NewDefaultConnection - Configure a connection with default values.
func NewDefaultConnection() *Conn {
	m := &Conn{}
	m.ServiceName = "quanta"
	m.ServicePort = 4000
	m.ServerHostOverride = "x.test.youtube.com"
	m.pollWait = DefaultPollInterval
	m.Quorum = 3
	m.Replicas = 2
	m.registeredServices = make(map[string]Service)
	return m
}

// RegisterService - Add a new service registration for member events.
func (m *Conn) RegisterService(svc Service) {

	n := reflect.TypeOf(svc).String()
	name := strings.Split(n, ".")[1]
	m.registeredServices[name] = svc
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
		err = m.update()
		if err != nil {
			return fmt.Errorf("node: can't fetch %s services list: %s", m.ServiceName, err)
		}

		if m.HashTable == nil {
			return fmt.Errorf("node: uninitialized %s services list: %s", m.ServiceName, err)
		}

		if len(m.nodeMap) < m.Quorum {
			return fmt.Errorf("node: quorum size %d currently,  must be at least %d to handle requests for service %s",
				len(m.idMap), m.Quorum, m.ServiceName)
		}
		m.clientConn, err = m.CreateNodeConnections(true)
		m.Admin = make([]pb.ClusterAdminClient, len(m.clientConn))
		for i := 0; i < len(m.clientConn); i++ {
			m.Admin[i] = pb.NewClusterAdminClient(m.clientConn[i])
			id := m.ids[i]
			entry := m.idMap[id]
			m.SendMemberJoined(id, entry.Node.Address, i)
		}
		go m.poll()
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
		m.Stop = make(chan struct{})
	}
	return
}

// CreateNodeConnections - Open a set of GRPC connections to all data nodes
func (m *Conn) CreateNodeConnections(largeBuffer bool) (nodeConns []*grpc.ClientConn, err error) {

	nodeCount := len(m.nodeMap)
	if m.ServicePort == 0 { // Test harness
		nodeConns = make([]*grpc.ClientConn, 1)
		ctx := context.Background()
		nodeConns[0], err = grpc.DialContext(ctx, "bufnet",
			grpc.WithDialer(TestDialer), grpc.WithInsecure())
		return
	}

	nodeConns = make([]*grpc.ClientConn, nodeCount)

	if largeBuffer {
		m.grpcOpts = append(m.grpcOpts,
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
		m.grpcOpts = append(m.grpcOpts, grpc.WithTransportCredentials(creds))
	} else {
		m.grpcOpts = append(m.grpcOpts, grpc.WithInsecure())
	}

	for i, id := range m.ids {
		entry := m.idMap[id]
		nodeConns[i], err = grpc.Dial(fmt.Sprintf("%s:%d", entry.Node.Address,
			m.ServicePort), m.grpcOpts...)
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
	if all && len(m.nodeMap) > 0 {
		replicas = len(m.nodeMap)
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

func (m *Conn) GetClientIndexForNodeID(nodeID string) int {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	if j, ok := m.nodeMap[nodeID]; ok {
		return j
	}
	return -1
}

func (m *Conn) GetNodeForID(nodeID string) (*api.ServiceEntry, bool) {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	if entry, ok := m.idMap[nodeID]; ok {
		return entry, true
	}
	return nil, false
}

// Disconnect - Terminate admin connections to all cluster nodes.
func (m *Conn) Disconnect() error {

	for i := 0; i < len(m.clientConn); i++ {
		if m.clientConn[i] != nil {
			m.clientConn[i].Close()
		}
	}
	select {
	case _, open := <-m.Stop:
		if open {
			close(m.Stop)
			m.Stop = nil
		}
	default:
		if m.Stop != nil {
			close(m.Stop)
		}
	}
	return nil
}

// Poll Consul for node membership list.
func (m *Conn) poll() {
	var err error

	for {
		select {
		case <-m.Stop:
			return
		case err = <-m.Err:
			if err != nil {
				u.Errorf("[client %s] error: %s", m.ServiceName, err)
			}
			return
		case <-time.After(m.pollWait):
			err = m.update()
			if err != nil {
				u.Errorf("[client %s] error: %s", m.ServiceName, err)
			}
		}
	}
}

// Update blocks until the service list changes or until the Consul agent's
// timeout is reached (10 minutes by default).
func (m *Conn) update() (err error) {

	opts := &api.QueryOptions{WaitIndex: m.waitIndex}
	serviceEntries, meta, err := m.Consul.Health().Service(m.ServiceName, "", false, opts)
	if err != nil {
		return err
	}
	if serviceEntries == nil {
		return nil
	}

	m.nodeMapLock.Lock()
	defer m.nodeMapLock.Unlock()

	ids := make([]string, 0)
	idMap := make(map[string]*api.ServiceEntry)
	for _, entry := range serviceEntries {
		if entry.Service.ID == "shutdown" {
			continue
		}
		if entry.Checks[0].Status == "passing" && entry.Checks[1].Status == "passing" {
			node := strings.Split(entry.Node.Node, ".")[0]
			idMap[node] = entry
			ids = append(ids, node)
		}
	}

	// Identify new member joins
	for index, id := range ids {
		if m.waitIndex > 0 { // not initial update
			if _, ok := m.nodeMap[id]; !ok {
				entry := idMap[id]
				// insert new connection and admin stub
				m.clientConn = append(m.clientConn, nil)
				copy(m.clientConn[index+1:], m.clientConn[index:])
				m.clientConn[index], err = grpc.Dial(fmt.Sprintf("%s:%d", entry.Node.Address,
					m.ServicePort), m.grpcOpts...)
				if err != nil {
					return err
				}
				m.Admin = append(m.Admin, nil)
				copy(m.Admin[index+1:], m.Admin[index:])
				m.Admin[index] = pb.NewClusterAdminClient(m.clientConn[index])
				u.Infof("NODE %s joined at index %d\n", id, index)
				m.SendMemberJoined(id, entry.Node.Address, index)
			}
		}
	}
	// Identify member leaves
	for id, index := range m.nodeMap {
		if _, ok := idMap[id]; !ok {
			// delete connection and admin stub
			m.clientConn[index].Close()
			if len(m.clientConn) > 1 {
				m.clientConn = append(m.clientConn[:index], m.clientConn[index+1:]...)
			} else {
				m.clientConn = make([]*grpc.ClientConn, 0)
			}
			if len(m.Admin) > 1 {
				m.Admin = append(m.Admin[:index], m.Admin[index+1:]...)
			} else {
				m.Admin = make([]pb.ClusterAdminClient, 0)
			}
			u.Infof("NODE %s left at index %d\n", id, index)
			m.SendMemberLeft(id, index)
		}
	}

	// Construct new state replacements
	m.nodes = make([]*api.ServiceEntry, 0)
	m.nodeMap = make(map[string]int)
	for _, entry := range serviceEntries {
		if entry.Service.ID == "shutdown" {
			continue
		}
		m.nodes = append(m.nodes, entry)
	}
	for i, id := range ids {
		m.nodeMap[id] = i
	}
	m.idMap = idMap
	m.ids = ids

	m.HashTable = rendezvous.New(ids)
	m.waitIndex = meta.LastIndex
	return nil
}

// Nodes - Return list of active nodes.
func (m *Conn) Nodes() []*api.ServiceEntry {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	return m.nodes
}

// ClientConnections - Return client connections.
func (m *Conn) ClientConnections() []*grpc.ClientConn {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	return m.clientConn
}

// GetNodeMap - Return list of members
func (m *Conn) GetNodeMap() map[string]int {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	return m.nodeMap
}

// GetHashTableWithNewNodes - Construct a new consistent hashtable with joining nodes
func (m *Conn) GetHashTableWithNewNodes(newIds []string) *rendezvous.Table {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	return rendezvous.New(append(m.ids, newIds...))
}

// SendMemberLeft - Notify listening service of MemberLeft event.
func (m *Conn) SendMemberLeft(nodeId string, index int) {

	for _, svc := range m.registeredServices {
		svc.MemberLeft(nodeId, index)
	}
}

// SendMemberJoined - Notify listening service of MemberLeft event.
func (m *Conn) SendMemberJoined(nodeId, ipAddress string, index int) {

	for _, svc := range m.registeredServices {
		svc.MemberJoined(nodeId, ipAddress, index)
	}
}

// GetService - Get a service by its name.
func (m *Conn) GetService(name string) Service {

	return m.registeredServices[name]
}
