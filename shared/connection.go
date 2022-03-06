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
	clusterSizeTarget  int
	nodeStatusMap      map[string]*pb.StatusMessage
	activeCount        int
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
	m.nodeStatusMap = make(map[string]*pb.StatusMessage, 0)
	return m
}

// OpType - Operation type
type OpType int

const (
	// NoQuorum - A quorum is not required for this operation.
	NoQuorum = OpType(iota)
	// Admin - Administrative operation to be applied to all nodes regardless of node status
	Admin
	// AllActive - All nodes must be in Active state.
	AllActive
	// ReadIntent - Read intent - A quorum is required, Syncing nodes are excluded.
	ReadIntent
	// ReadIntentAll - Write intent - A quorum is required, Syncing nodes excluded.
	ReadIntentAll
	// WriteIntent - Write intent - A quorum is required, Syncing nodes are included.
	WriteIntent
	// WriteIntentAll - Write intent - A quorum is required, Syncing nodes included.
	WriteIntentAll
)

// String - Returns a string representation of OpType
func (ot OpType) String() string {

	switch ot {
	case NoQuorum:
		return "NoQuorum"
	case Admin:
		return "Admin"
	case AllActive:
		return "AllActive"
	case ReadIntent:
		return "ReadIntent"
	case ReadIntentAll:
		return "ReadIntentAll"
	case WriteIntent:
		return "WriteIntent"
	case WriteIntentAll:
		return "WriteIntentAll"
	}
	return ""
}

// ClusterState - Overall cluster state indicators.
type ClusterState int

const (
	// Red - A quorum is not reached.  Cluster is down or in inactive state.
	Red = ClusterState(iota)
	// Orange - One or more nodes are down but cluster is serving requests.
	Orange
	// Green - All nodes are actively serving data.
	Green
)

// String - Returns a string representation of ClusterState
func (st ClusterState) String() string {

	switch st {
	case Red:
		return "RED"
	case Orange:
		return "ORANGE"
	case Green:
		return "GREEN"
	}
	return ""
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
			status, err := m.getNodeStatusForIndex(i)
			if err == nil {
				m.nodeStatusMap[id] = status
				if status.NodeState == "Active" {
					m.activeCount++
				}
			} else {
				u.Errorf("getNodeStatusForIndex: %v", err)
			}
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
// Resolve the node location(s) of a single key. If 'Admin' or 'AllActive' then many nodes selected.
/*
   // NoQuorum - A quorum is not required for this operation.
   NoQuorum = OpType(iota)
   // Admin - Administrative operation to be applied to all nodes regardless of node status
   Admin
   // AllActive - All nodes must be in Active state.
   AllActive
   // ReadIntent - Read intent - A quorum is required, Syncing nodes excluded.
   ReadIntent
   // WriteIntent - Write intent - A quorum is required, Syncing nodes included.
   WriteIntent
   // WriteIntentAll - Write intent - A quorum is required, Syncing nodes included.
   WriteIntentAll
*/
func (m *Conn) SelectNodes(key interface{}, op OpType) ([]int, error) {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()

	// test harness mode
	if m.ServicePort == 0 {
		return []int{0}, nil
	}

	clusterState, _, _ := m.GetClusterState()
	if clusterState != Green && op == AllActive {
		return nil, fmt.Errorf("This operation requires [%v], but the cluster state is [%v]", op, clusterState)
	}

	all := (op == AllActive || op == Admin || op == WriteIntentAll || op == ReadIntentAll)
	replicas := m.Replicas
	if all && len(m.nodeMap) > 0 {
		replicas = len(m.nodeMap)
	}

	nodeKeys := m.HashTable.GetN(replicas, ToString(key))
	indices := make([]int, 0)

	for _, v := range nodeKeys {
		status, found := m.nodeStatusMap[v]
		if !found {
			return nil, fmt.Errorf("SelectNodes: assert fail for key [%v], node %v not found in nodeStatusMap",
				ToString(key), v)
		}
		if status.NodeState != "Active" && status.NodeState != "Syncing" && !all {
			continue
		}
		if status.NodeState != "Active" && (op == ReadIntent || op == ReadIntentAll) {
			continue // Don't read from a node in Syncing state.
		}
		if j, ok := m.nodeMap[v]; ok {
			indices = append(indices, j)
			if op == ReadIntent { // Only need one
				break
			}
		}
	}
	if len(indices) == 0 { // Shouldn't happen
		return nil, fmt.Errorf("SelectNodes assert fail: none selected: nodeKeys [%v], nodeStatusMap [%#v], op [%s]",
			nodeKeys, m.nodeStatusMap, op)
	}
	return indices, nil
}

// GetClientIndexForNodeID - The the index into m.clientConn for a node ID.
func (m *Conn) GetClientIndexForNodeID(nodeID string) int {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	if j, ok := m.nodeMap[nodeID]; ok {
		return j
	}
	return -1
}

// GetNodeForID - Return the Consul ServiceEntry for a given node ID.
func (m *Conn) GetNodeForID(nodeID string) (*api.ServiceEntry, bool) {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	if entry, ok := m.idMap[nodeID]; ok {
		return entry, true
	}
	return nil, false
}

// Disconnect - Terminate connections to all cluster nodes.
func (m *Conn) Disconnect() error {

	for i := 0; i < len(m.clientConn); i++ {
		if m.clientConn[i] != nil {
			m.clientConn[i].Close()
		}
	}
	m.clientConn = make([]*grpc.ClientConn, 0)
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
	m.clusterSizeTarget, err = GetClusterSizeTarget(m.Consul)
	if err != nil {
		return err
	}
	if m.clusterSizeTarget == 0 {
		m.clusterSizeTarget = m.Replicas + 1
		u.Warnf("ClusterSizeTarget not set, defaulting target to replicas + 1, currently %d", m.clusterSizeTarget)
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
			delete(m.nodeStatusMap, id)
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

	// Refresh statuses
	m.activeCount = 0

	// if Admin connections are not initialized then skip this until next update, we are being called from Connect
	if len(m.Admin) == 0 {
		return nil
	}

	for k, v := range m.nodeMap {
		status, err := m.getNodeStatusForIndex(v)
		if err == nil {
			m.nodeStatusMap[k] = status
			if status.NodeState == "Active" {
				m.activeCount++
			}
		} else {
			u.Errorf("getNodeStatusForIndex: k = %s, i = %d - %v", k, v, err)
		}
	}
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

// CheckNodeForKey
func (m *Conn) CheckNodeForKey(key, nodeID string) (bool, int) {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()

	nodeKeys := m.HashTable.GetN(m.Replicas, key)

	for i, v := range nodeKeys {
		if v != nodeID {
			continue
		}
		status, found := m.nodeStatusMap[v]
		if !found {
			u.Errorf("CheckNodeForKey: assertion fail - for key %s, node %s status unknown", key, nodeID)
			return false, 0
		}
		if status.NodeState != "Active" && status.NodeState != "Syncing" {
			u.Errorf("CheckNodeForKey: assertion fail - for key %s, node %s not Active/Syncing", key, nodeID)
			return false, 0
		}
		return true, i + 1
	}
	return false, 0
}

// SendMemberLeft - Notify listening service of MemberLeft event.
func (m *Conn) SendMemberLeft(nodeID string, index int) {

	for _, svc := range m.registeredServices {
		svc.MemberLeft(nodeID, index)
	}
}

// SendMemberJoined - Notify listening service of MemberLeft event.
func (m *Conn) SendMemberJoined(nodeID, ipAddress string, index int) {

	for _, svc := range m.registeredServices {
		svc.MemberJoined(nodeID, ipAddress, index)
	}
}

// GetService - Get a service by its name.
func (m *Conn) GetService(name string) Service {

	return m.registeredServices[name]
}

// GetNodeStatusForID - Returns the node status for a given node ID.
func (m *Conn) GetNodeStatusForID(nodeID string) (*pb.StatusMessage, error) {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	s, ok := m.nodeStatusMap[nodeID]
	if !ok {
		return nil, fmt.Errorf("no node status for %s", nodeID)
	}
	return s, nil
}

func (m *Conn) getNodeStatusForID(nodeID string) (*pb.StatusMessage, error) {

	clientIndex, ok := m.nodeMap[nodeID]
	if !ok {
		return nil, fmt.Errorf("clientIndex == -1 node id %s missing: [%#v]", nodeID, m.nodeMap)
	}
	return m.getNodeStatusForIndex(clientIndex)
}

func (m *Conn) getNodeStatusForIndex(clientIndex int) (*pb.StatusMessage, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	result, err := m.Admin[clientIndex].Status(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("%v.Status(_) = _, %v, node = %s\n", m.Admin[clientIndex], err,
			m.clientConn[clientIndex].Target()))
	}
	return result, nil
}

// GetClusterState - Returns the overall cluster state health, active nodes, cluster size.
func (m *Conn) GetClusterState() (status ClusterState, activeCount, clusterSizeTarget int) {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()

	activeCount = m.activeCount
	clusterSizeTarget = m.clusterSizeTarget
	status = Red
	if m.activeCount == m.clusterSizeTarget {
		status = Green
		return
	}
	if m.activeCount >= m.clusterSizeTarget-(m.Replicas-1) {
		status = Orange
		return
	}
	return
}
