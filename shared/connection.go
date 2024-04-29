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

	"math"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	u "github.com/araddon/gou"
	pb "github.com/disney/quanta/grpc"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/hashicorp/consul/api"
	"github.com/stvp/rendezvous"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
)

// Conn - Client side cluster state and connection abstraction.
type Conn struct {
	ServiceName        string                       // Name of service i.e. "quanta" for registration with Consul.
	Quorum             int                          //
	Replicas           int                          // Number of data replicas to support HA in case of node failure.
	ConsulAgentAddr    string                       // Network endpoint for Consul agent.  Defaults to "127.0.0.1:8500".
	ServicePort        int                          // Port number for the service API endpoint.
	ServerHostOverride string                       // Hack for resolving DNS hosts for TLS implementation.
	tls                bool                         // Use TLS if "true".
	certFile           string                       // Certificate file location for TLS.
	grpcOpts           []grpc.DialOption            // GRPC options.
	Admin              []pb.ClusterAdminClient      // Cluster adminitration API and health checks.
	clientConn         []*grpc.ClientConn           // GRPC client connection wrapper. One per node.
	Err                chan error                   // Error channel for administration API failures.
	Stop               chan struct{}                // Channel for signaling cluster Stop event.
	Consul             *api.Client                  // Handle to Consul cluster.
	HashTable          *rendezvous.Table            // Rendezvous hash table containing cluster members for sharding.
	waitIndex          uint64                       // Consul wait index. For long polling consul.
	pollWait           time.Duration                // Wait interval for node membership polling events.
	nodes              []*api.ServiceEntry          // List of nodes as currently registered with Consul.
	nodeMap            map[string]int               // Map cluster node keys to connection/client arrays.
	nodeMapLock        sync.RWMutex                 // Lock for nodeMap.
	registeredServices map[string]Service           // service name to Service. Aka "BitmapIndex" to *server.BitmapIndex
	registerLock       sync.RWMutex                 // Lock for registeredServices.
	idMap              map[string]*api.ServiceEntry // Map of node ID to service entry.
	ids                []string                     // List of node IDs.
	clusterSizeTarget  int                          // Target cluster size.
	nodeStatusMap      map[string]*pb.StatusMessage // Map of node ID to status message.
	activeCount        int                          // Number of active nodes.
	IsLocalCluster     bool                         // Is this a local cluster? For debugging.
	owner              string                       // for debugging
}

// NewDefaultConnection - Configure a connection with default values.
func NewDefaultConnection(owner string) *Conn {

	u.Info("NewDefaultConnection ", owner)
	m := &Conn{}
	m.ServiceName = "quanta"
	m.ServicePort = 4000
	m.ServerHostOverride = "x.test.youtube.com"
	m.pollWait = DefaultPollInterval
	m.Quorum = 3
	m.Replicas = 2
	m.registeredServices = make(map[string]Service)
	m.nodeStatusMap = make(map[string]*pb.StatusMessage, 0)
	m.owner = owner // for debugging
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

/** We will need to upgrate the state to a more complex state machine.
Notes:

states:
	Inital state: nodes are starting up and there are not enough nodes to for clusterSizeTarget.
				requests are rejected until the cluster is ready.

	Running state: all nodes are active and the cluster is ready to accept requests.

	Node loss: A node is lost. Short of clusterSizeTarget. Requests are still accepted.
				Transition to loss rebalancing when possible.

		Loss rebalancing: same as node loss 1. Waiting for rebalance to complete. Requests are still accepted.

		Node loss Running: We are short a node but otherwise running. Requests are still accepted.

	Node add: A new node is added. Not at clusterSizeTarget. Requests are still accepted.

		Node add rebalancing: same as node add 1. Waiting for rebalance to complete. Requests are still accepted.

		Node add Running: We are up a node but otherwise running. Requests are still accepted.

Attributes:

	Every state has these attributes.

	Rejecting requests: Requests throw errors.

	Short: We are short a node but still balanced and accepts requests.

	Long: We have an extra node above clusterSizeTarget but still balanced and accepts requests.
		  We should consider changing the clusterSizeTarget to the new value.

	Orange: Some nodes are rebalancing. Requests are still accepted.

*/

// ClusterState - Overall cluster state indicators.
type ClusterState int

// TODO: update this to the more complex state machine.
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

	m.registerLock.Lock()
	defer m.registerLock.Unlock()
	n := reflect.TypeOf(svc).String()
	name := strings.Split(n, ".")[1]
	m.registeredServices[name] = svc
}

// Connect with configured values.
func (m *Conn) Connect(consul *api.Client) (err error) {

	u.Info("Conn Connect", m.owner)

	if m.ServicePort > 0 {
		if consul != nil {
			m.Consul = consul
		} else {
			m.Consul, err = api.NewClient(api.DefaultConfig())
			if err != nil {
				return fmt.Errorf("client: can't create Consul API client: %s", err)
			}
		}
		err = m.updateHealth(true) // initial scan
		if err != nil {
			return fmt.Errorf("node: can't fetch %s services list: %s", m.ServiceName, err)
		}

		if m.HashTable == nil {
			return fmt.Errorf("node: uninitialized %s services list: %s", m.ServiceName, err)
		}

		// Who uses this? do we need it?
		// if len(m.nodeMap) < m.Quorum {
		// 	return fmt.Errorf("node: quorum size %d currently,  must be at least %d to handle requests for service %s",
		// 		len(m.idMap), m.Quorum, m.ServiceName)
		// }

		m.clientConn, err = m.CreateNodeConnections(true)
		m.Admin = make([]pb.ClusterAdminClient, len(m.clientConn))
		activeCount := 0
		for i := 0; i < len(m.clientConn); i++ {
			id := m.ids[i]

			adminClient := pb.NewClusterAdminClient(m.clientConn[i])
			m.Admin[i] = adminClient

			status, err := m.getNodeStatusForIndex(i)
			// u.Info("Conn Connect getNodeStatusForIndex", m.owner, i, status.NodeState, err)
			if err == nil {
				m.nodeStatusMap[id] = status
				if status.NodeState == "Active" {
					activeCount++
				}
			} else {
				u.Errorf("getNodeStatusForIndex: %v", err)
			}
		}

		for i := 0; i < len(m.clientConn); i++ {
			id := m.ids[i]
			entry := m.idMap[id]
			m.SendMemberJoined(id, entry.Service.Address, i) // why?
		}

		m.GetAllPeerStatus()

		go m.poll()
	} else {
		// ServicePort is 0
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
// requires m.ids is be non empty
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
		nodeConnPort := entry.Service.Port //  m.ServicePort
		target := fmt.Sprintf("%s:%d", entry.Service.Address, nodeConnPort)
		nodeConns[i], err = grpc.Dial(target, m.grpcOpts...)
		if err != nil {
			u.Logf(u.FATAL, "fail to dial: %v", err)
		}
	}
	return nodeConns, nil
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
		return nil, fmt.Errorf("selectNodes requires [%v], but the cluster state is [%v] node=%v", op, clusterState, m.owner)
	}

	all := (op == AllActive || op == Admin || op == WriteIntentAll || op == ReadIntentAll)
	replicas := m.Replicas
	if all && len(m.nodeMap) > 0 {
		replicas = len(m.nodeMap)
	}

	nodeKeys := m.HashTable.GetN(replicas, ToString(key))
	indices := make([]int, 0)

	// u.Info("SelectNodes GetN", ToString(key), nodeKeys)

	// u.Info("SelectNodes nodeStatusMap", m.owner, m.nodeStatusMap)

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
	// what happens if we don't find any? Happens in test situations and startup
	if len(indices) == 0 {
		return nil, fmt.Errorf("SelectNodes assert fail: none selected: nodeKeys [%v], nodeStatusMap [%#v], op [%s]",
			nodeKeys, m.nodeStatusMap, op)
	}
	return indices, nil
}

// GetClientIndexForNodeID - The the index into m.clientConn for a node ID.
func (m *Conn) GetClientIndexForNodeID(nodeID string) (int, error) {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	if j, ok := m.nodeMap[nodeID]; ok {
		return j, nil
	}
	return -1, fmt.Errorf("GetClientIndexForNodeID: nodeID %s not found. owner %s", nodeID, m.owner)
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

func (m *Conn) Quit() {
	m.registeredServices = make(map[string]Service)
	m.nodeStatusMap = make(map[string]*pb.StatusMessage, 0)
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

	// defer u.Info("Conn poll QUIT", m.owner)

	consul := m.Consul

	// poll for AnyNodeStatusChangeTime
	// which is a node changes it's Status. This is a long poll, it will block until "AnyNodeStatusChangeTime" changes
	go func() {
		lastIndex := uint64(0)
		// defer u.Info("Conn poll AnyNodeStatusChangeTime QUIT", m.owner)
		for {
			select {
			case <-m.Stop:
				// u.Info("Conn poll AnyNodeStatusChangeTime STOP", m.owner)
				return
			case err = <-m.Err:
				if err != nil {
					u.Errorf("[client %s] error: %s", m.ServiceName, err)
				}
				return
			case <-time.After(m.pollWait):
				// u.Info("Conn poll AnyNodeStatusChangeTime start", m.owner)
				options := &api.QueryOptions{WaitIndex: lastIndex}
				pair, meta, err := consul.KV().Get("AnyNodeStatusChangeTime", options)
				if err != nil {
					u.Error("Conn poll AnyNodeStatusChangeTime error", m.owner, err)
					continue
				} else {
					// u.Info("Conn poll AnyNodeStatusChangeTime changed", m.owner, string(pair.Value))
					lastIndex = meta.LastIndex
					m.GetAllPeerStatus()
				}
				_ = pair
			}
		}
	}()

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
			err = m.updateHealth(false) // long poll
			if err != nil {
				u.Errorf("[client %s] error: %s", m.ServiceName, err)
			}
		}
	}
}

// TenthSecs returns the current time in 10ths of a second mod one minute
func TenthSecs() float64 {

	now := float64(time.Now().UnixMilli()) / 100.0 // 10ths of a second
	now += 0.00000001                              // avoid rounding errors
	now = float64(int(now))                        // truncate to 10ths of a second
	now = math.Mod(now, 600) / 10                  // 0-59.9
	return now
}

// updateHealth blocks until the service list changes or until the Consul agent's
// timeout is reached (10 minutes by default).
// if initial then the conn was just created.
func (m *Conn) updateHealth(initial bool) (err error) {

	now := TenthSecs()
	u.Debugf("Conn update %v %v", m.owner, now)
	defer func() {
		passed := TenthSecs() - now
		u.Debugf("Done Conn update  %v %v %v %v active %v %v ", m.owner, now, passed, m.clusterSizeTarget, m.activeCount, m.nodeMap)
	}()

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
			u.Info("have shutdown entry.Service.ID ", entry)
			continue
		}
		// what are these checks?  why are there two?
		if entry.Checks[0].Status == "passing" && entry.Checks[1].Status == "passing" {
			// node := strings.Split(entry.Node.Node, ".")[0]
			// let's not use the Node.Node, which is always like 'mbp-atw-2.lan' locally
			// node = entry.Node.ID and the ID's all the same too
			node := entry.Service.ID // this is like "quanta-node-1" which is better
			idMap[node] = entry
			ids = append(ids, node)
		}
	}

	oldNodeMap := m.nodeMap

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

	u.Info("Conn update nodeMap", m.owner, "old", oldNodeMap, "new", m.nodeMap, uintptr(unsafe.Pointer(m)))

	// Identify new member joins, if is an update and not first scan.
	if !initial {
		for index, id := range ids {
			_, ok := oldNodeMap[id]
			if !ok {
				entry := idMap[id]
				// insert new connection and admin stub
				m.clientConn = append(m.clientConn, nil)
				copy(m.clientConn[index+1:], m.clientConn[index:])
				servicePort := entry.Service.Port // m.ServicePort
				target := fmt.Sprintf("%s:%d", entry.Service.Address, servicePort)
				m.clientConn[index], err = grpc.Dial(target, m.grpcOpts...)
				if err != nil {
					u.Info("Conn new member dial fail", err)
					return err
				}
				m.Admin = append(m.Admin, nil)
				copy(m.Admin[index+1:], m.Admin[index:])
				m.Admin[index] = pb.NewClusterAdminClient(m.clientConn[index])
				u.Infof("NODE %s joined at index %d in %s\n", id, index, m.owner)
				m.SendMemberJoined(id, entry.Service.Address, index)
			}
		}
	}
	// Identify member leaves
	if !initial {
		for id, index := range oldNodeMap {
			_, ok := idMap[id]
			if !ok {
				// delete connection and admin stub
				if index >= len(m.clientConn) {
					continue
				}
				u.Info("Conn member left", m.owner, id)

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
	}

	m.idMap = idMap
	m.ids = ids

	u.Debug("Conn update new rendezvous ids", m.owner, ids, m.nodeMap, "old", oldNodeMap, "clusterSizeTarget", m.clusterSizeTarget)
	// if len(ids) > m.clusterSizeTarget { // atw fixme
	// 	ids = ids[:m.clusterSizeTarget] // ignore any new nodes until clusterSizeTarget changes
	// }
	m.HashTable = rendezvous.New(ids)
	m.waitIndex = meta.LastIndex

	// Refresh statuses

	// don't scan for status if the admin is not set up. This happens when we are called from Connect, initial = true
	if initial {
		return nil
	}

	m.GetAllPeerStatus()

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

// CheckNodeForKey - Verify the existence of a given shard on a specific node.
func (m *Conn) CheckNodeForKey(key, nodeID string) (bool, int) {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()

	// u.Debug("CheckNodeForKey GetN", m.Replicas, key, nodeID)

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

	m.registerLock.RLock()
	defer m.registerLock.RUnlock()

	for _, svc := range m.registeredServices {
		svc.MemberLeft(nodeID, index)
	}
}

// SendMemberJoined - Notify listening service of MemberJoined event.
func (m *Conn) SendMemberJoined(nodeID, ipAddress string, index int) {

	m.registerLock.RLock()
	defer m.registerLock.RUnlock()
	for _, svc := range m.registeredServices {
		svc.MemberJoined(nodeID, ipAddress, index)
	}
}

// GetService - Get a service by its name.
func (m *Conn) GetService(name string) Service {

	m.registerLock.RLock()
	defer m.registerLock.RUnlock()
	return m.registeredServices[name]
}

// GetNodeStatusForID - Returns the node status for a given node ID.
func (m *Conn) GetNodeStatusForID(nodeID string) (*pb.StatusMessage, error) {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	s, ok := m.nodeStatusMap[nodeID]
	if !ok {
		return nil, fmt.Errorf("no node status for %s owner %s", nodeID, m.owner)
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

// getNodeStatusForIndex - calls 'status' grpc on the admin client for the given index.
// admin must be filled out.
func (m *Conn) getNodeStatusForIndex(clientIndex int) (*pb.StatusMessage, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	if clientIndex >= len(m.Admin) {
		return nil, fmt.Errorf("clientIndex >= len(m.Admin) %d >= %d", clientIndex, len(m.Admin))
	}

	admin := m.Admin[clientIndex]
	result, err := admin.Status(ctx, &empty.Empty{})
	if err != nil {
		target := "unknown"
		if clientIndex < len(m.clientConn) {
			target = m.clientConn[clientIndex].Target()
		}
		e := fmt.Sprintf("getNodeStatusForIndex Status, err = %v, target = %s\n", err, target)
		return nil, fmt.Errorf(e)
	}
	return result, nil
}

// GetNodeStatusForIndex - Returns the node status for a given client index.
func (m *Conn) GetNodeStatusForIndex(clientIndex int) (*pb.StatusMessage, error) {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	return m.getNodeStatusForIndex(clientIndex)
}

// GetCachedNodeStatusForIndex - Returns the node status for a given client index.
func (m *Conn) GetCachedNodeStatusForIndex(clientIndex int) (*pb.StatusMessage, error) {

	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	if m.ServicePort == 0 { // test harness
		return &pb.StatusMessage{NodeState: "Active"}, nil
	}
	if clientIndex < 0 || clientIndex >= len(m.ids) {
		return nil, fmt.Errorf("clientIndex %d is invalid", clientIndex)
	}
	id := m.ids[clientIndex]
	status, found := m.nodeStatusMap[id]
	if !found {
		return nil, fmt.Errorf("node status not found for id %v at clientIndex %d", id, clientIndex)
	}
	return status, nil
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
	// there may be more nodes than there should be, but we don't care about that
	if m.activeCount > m.clusterSizeTarget {
		status = Green
		return
	}
	if m.activeCount >= m.clusterSizeTarget-(m.Replicas-1) {
		status = Orange
		return
	}
	return
}

// GetAllPeerStatus - grpc node status from all nodes in the cluster
// count them as we go.
func (m *Conn) GetAllPeerStatus() {

	// u.Debug("GetAllPeerStatus top", m.owner, m.nodeMap)

	// m.nodeMapLock.Lock() // we can't lock here, it will deadlock? FIXME:
	// defer m.nodeMapLock.Unlock()

	activeCount := 0
	nodeStatusMap := make(map[string]*pb.StatusMessage, 0)

	// Get the node status for all nodes in the cluster
	for k, v := range m.nodeMap {
		status, err := m.getNodeStatusForIndex(v)

		// u.Debug("GetAllPeerStatus getNodeStatusForIndex", m.owner, k, v, status.NodeState)
		if err == nil {
			nodeStatusMap[k] = status
			if status.NodeState == "Active" {
				activeCount++
			}
		} else {
			u.Errorf("GetAllPeerStatus getNodeStatusForIndex in update: k = %s, i = %d - %v", k, v, err)
			if !m.IsLocalCluster {
				os.Exit(1) // This is fatal
			}
		}
	}

	// u.Debug("GetAllPeerStatus done", m.owner, m.nodeMap, activeCount, nodeStatusMap)

	// got a deadlock from this. FIXME: there's probably a race condition here.
	// m.nodeMapLock.Lock()
	// defer m.nodeMapLock.Unlock()

	m.activeCount = activeCount
	for k, v := range nodeStatusMap {
		m.nodeStatusMap[k] = v
	}
	// u.Debug("GetAllPeerStatus bottom", m.owner, m.nodeMap, m.activeCount)
}
