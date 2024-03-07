package server

//
// This code manages the abstractions necessary for a server node.  This includes code
// to register node membership with Consul.  It is the "base class" containg common node level
// functions to support business APIs.
// A node normally listens for grpc messages on port 4000.  This is configurable.
// See quanta-node.go for the main entry point.
//
//

import (
	"context"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/disney/quanta/core"
	pb "github.com/disney/quanta/grpc"
	"github.com/disney/quanta/shared"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/testdata"
)

const (
	checkInterval = 5 * time.Second
	pollWait      = time.Second
	sep           = string(os.PathSeparator)
)

// StateType - LifeCycle States of a Node.
type StateType int

const (
	// Starting - Node is initializing
	Starting = StateType(iota)
	// Joining - Node is synchronizing its state with peers.
	Joining
	// Syncing - Node is actively taking write traffic only.
	Syncing
	// Active - Node is actively taking all (R/W) traffic.
	Active
	// Stopped - Node was stopped gracefully.
	Stopped
)

// String - Returns a string representation of StateType
func (st StateType) String() string {

	switch st {
	case Starting:
		return "Starting"
	case Joining:
		return "Joining"
	case Syncing:
		return "Syncing"
	case Active:
		return "Active"
	case Stopped:
		return "Stopped"
	}
	return ""
}

// NodeService - server side service lifecycle management interface.
type NodeService interface {
	Init() error
	JoinCluster()
	Shutdown()
}

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

	BindAddr    string
	serviceName string
	dataDir     string
	server      *grpc.Server
	consul      *api.Client
	hashKey     string
	version     string
	shardCount  int
	memoryUsed  int

	// TLS options
	tls      bool
	certFile string
	keyFile  string

	// Health check endpoint
	checkURL string

	// Shutdown channels
	Stop chan bool
	Err  chan error

	State         StateType
	localServices map[string]NodeService

	TableCache *core.TableCacheStruct

	listener net.Listener
}

// NewNode - Construct a new node instance.
func NewNode(version string, port int, bindAddr, dataDir, hashKey string, consul *api.Client) (*Node, error) {

	conn := shared.NewDefaultConnection("nodeKey-" + hashKey)
	m := &Node{Conn: conn, version: version}
	m.localServices = make(map[string]NodeService, 0)
	m.ServicePort = port
	m.Quorum = 0
	m.TableCache = core.NewTableCacheStruct()
	if hashKey == "" {
		return nil, fmt.Errorf("hash key is empty")
	}
	m.hashKey = hashKey
	if m.dataDir == "/" {
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

	// Register peer services with connection
	_ = shared.NewBitmapIndex(conn)
	_ = shared.NewKVStore(conn)
	_ = shared.NewStringSearch(conn, 20000)

	return m, nil
}

// GetNodeID - returns node identifier
func (n *Node) GetNodeID() string {
	return n.hashKey
}

// Join creates a new Node and adds it to the distributed hash table specified
// by the given name. This name should be unique among all Nodes in the hash
// table.
func (n *Node) Join(name string) error {

	n.serviceName = name
	n.checkURL = "Status"
	n.Stop = make(chan bool)
	n.Err = make(chan error)

	err := n.register()
	if err != nil {
		return fmt.Errorf("node: can't register %s service: %s", n.serviceName, err)
	}

	time.Sleep(1 * time.Second) // wait for everyone to register (atw)

	err = n.Connect(n.consul)
	if err != nil {
		return fmt.Errorf("node: Connect failed: %v", err)
	}

	n.State = Joining
	n.JoinServices()

	return nil
}

func (n *Node) register() (err error) {

	fmt.Printf("register node serviceName=%v hashKey=%v bindAddr=%v port=%v\n", n.serviceName, n.hashKey, n.BindAddr, n.ServicePort)

	err = n.consul.Agent().ServiceRegister(&api.AgentServiceRegistration{
		Name: n.serviceName,
		ID:   n.hashKey,
		Check: &api.AgentServiceCheck{
			GRPC:     fmt.Sprintf("%v:%v/%v", n.BindAddr, n.ServicePort, n.checkURL),
			Interval: checkInterval.String(),
		},
		Tags:    []string{"hashkey: " + n.hashKey, "address: " + n.BindAddr, "port: " + strconv.Itoa(n.ServicePort)},
		Port:    n.ServicePort,
		Address: n.BindAddr, // comes out as Service.Address and not Node.Address
	})
	return err
}

// What is this for?
func (n *Node) XxxxQuit() {
	n.Conn.Quit()
	n.Conn.Disconnect()
	n.server.Stop()
}

// Start the node endpoint.  Does not block.
func (n *Node) Start() {

	go func() {
		if n.ServicePort > 0 {
			var err error
			n.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", n.BindAddr, n.ServicePort))
			if err != nil {
				u.Errorf("error starting node listening endpoint: %v", err)
				n.Err <- err
			}
			go func() {
				for <-n.Stop {
					n.server.Stop()
					u.Info(n.hashKey, "node got n.Stop. Exiting.")
					os.Exit(0)
				}
				// 	select {
				// 	case <-n.Stop:
				// 		u.Info(n.hashKey, "node got n.Stop Stopping GRPC server.")
				// 		n.server.Stop()
				// 		u.Info("Exiting.")
				// 		os.Exit(0)
				// 	}
				// }
			}()
			n.server.Serve(n.listener)
		} else {
			n.server.Serve(shared.TestListener)
		}
	}()
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

	u.Debug(n.hashKey, "Node Leave")

	err = n.Disconnect()
	n.ShutdownServices()
	n.State = Stopped
	if err == nil {
		err = n.consul.Agent().ServiceDeregister(n.hashKey)
	}
	// close(n.Stop)
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

	ip, err := shared.GetLocalHostIP()
	if err != nil {
		return nil, err
	}
	status := &pb.StatusMessage{
		NodeState:  n.State.String(),
		LocalIP:    ip.String(),
		LocalPort:  uint32(n.ServicePort),
		Version:    n.version,
		Replicas:   uint32(n.Replicas),
		ShardCount: uint32(n.shardCount),
		MemoryUsed: uint32(n.memoryUsed),
	}

	u.Debugf("Node Status: %v %v %v %v ", n.hashKey, status.NodeState, " shards", status.ShardCount)

	return status, nil
}

// Shutdown - Shut the node down.
func (n *Node) Shutdown(ctx context.Context, e *empty.Empty) (*empty.Empty, error) {

	u.Warn("Received Shutdown call via API.")
	err := n.Leave() // closes Stop
	n.listener.Close()
	// fmt.Println(n.hashKey, "Node Shutdown sending n.Stop")
	// n.Stop <- true
	return e, err
}

// AddNodeService - Add a new node level service.
func (n *Node) AddNodeService(api NodeService) {

	s := reflect.TypeOf(api).String()
	name := strings.Split(s, ".")[1]
	n.localServices[name] = api
}

// ShutdownServices - Invoke service interface for Shutdown event
func (n *Node) ShutdownServices() {

	u.Warn("Shutting down services.")
	for _, v := range n.localServices {
		v.Shutdown()
	}
}

// JoinServices - Invoke service interface for Join event
func (n *Node) JoinServices() {

	u.Info("Services are joining.")
	for _, v := range n.localServices {
		v.JoinCluster()
	}
}

// InitServices - Initialize server side services.
func (n *Node) InitServices() error {

	u.Info("Services are initializing.")
	for _, v := range n.localServices {
		if err := v.Init(); err != nil {
			return err
		}
	}
	return nil
}

// GetNodeService - Get a service by its name.
func (n *Node) GetNodeService(name string) NodeService {

	return n.localServices[name]
}

// Global storage for Prometheus metrics
var (
	pUptimeHours = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "uptime_hours",
		Help: "Hours of up time",
	})

	pState = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "node_state",
		Help: "The State of the node [Starting = 0, Joining = 1, Syncing = 2, Active = 3]",
	})

	pKVStoreCloserLatency = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "index_closer_latency",
		Help: "Index closer thread latency",
	})

	pBitmapCacheUpdLatency = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bitmap_cache_update_latency",
		Help: "Bitmap cache update latency",
	})

	pBSICacheUpdLatency = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bsi_cache_update_latency",
		Help: "BSI cache update latency",
	})

	pBitmapPersistLatency = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bitmap_persistence_latency",
		Help: "Bitmap persistence latency",
	})

	pBSIPersistLatency = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bsi_persistence_latency",
		Help: "BSI persistence latency",
	})

	pBitmapEdgeTCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bitmap_edge_t_count",
		Help: "Count of edge triggered bitmap writes",
	})

	pBitmapTimeTCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bitmap_time_t_count",
		Help: "Count of time triggered bitmap writes",
	})

	pBSIEdgeTCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bsi_edge_t_count",
		Help: "Count of edge triggered bsi writes",
	})

	pBSITimeTCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bsi_time_t_count",
		Help: "Count of time triggered bsi writes",
	})
)

// PublishMetrics - Update Prometheus metrics
func (n *Node) PublishMetrics(upTime time.Duration, lastPublishedAt time.Time) time.Time {

	// Update Prometheus metrics
	pUptimeHours.Set(float64(upTime) / float64(1000000000*3600))
	pState.Set(float64(n.State))

	kvService := n.GetNodeService("KVStore").(*KVStore)
	if kvService != nil {
		pKVStoreCloserLatency.Set(float64(kvService.cleanupLatency))
	} else {
		u.Debug("Cannot publish metrics for KVStore.")
	}

	bitService := n.GetNodeService("BitmapIndex").(*BitmapIndex)
	if bitService != nil {
		pBitmapCacheUpdLatency.Set(float64(bitService.updBitmapTime.Load()))
		pBSICacheUpdLatency.Set(float64(bitService.updBSITime.Load()))
		pBitmapPersistLatency.Set(float64(bitService.saveBitmapTime.Load()))
		pBSIPersistLatency.Set(float64(bitService.saveBSITime.Load()))
		pBitmapEdgeTCount.Set(float64(bitService.saveBitmapECnt.Load()))
		pBSIEdgeTCount.Set(float64(bitService.saveBSIECnt.Load()))
		pBitmapTimeTCount.Set(float64(bitService.saveBitmapTCnt.Load()))
		pBSITimeTCount.Set(float64(bitService.saveBSITCnt.Load()))
	} else {
		u.Debug("Cannot publish metrics for Bitmap Index.")
	}

	return time.Now()
}

// InvokeUpdates - server side force re-fetch of all status unused deprecated
// func (n *Node) InvokeUpdates(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
// 	fmt.Println("server InvokeUpdates", n.hashKey)
// 	n.Conn.GetAllPeerStatus()
// 	return &emptypb.Empty{}, nil
// }
