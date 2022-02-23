package shared

//
// This code executes on the node server side "peer client"
// API wrappers for node startup synchronization resulting in successful "node join".
//

import (
	"context"
	"fmt"
	u "github.com/araddon/gou"
	pb "github.com/disney/quanta/grpc"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	"golang.org/x/sync/errgroup"
	"os"
	"time"
)

// Synchronize flow.
func (c *BitmapIndex) Synchronize(nodeKey string) error {

	var memberCount int
	clusterSizeTarget, err := GetClusterSizeTarget(c.Conn.Consul)
	if err != nil {
		return err
	}
	quorumSize := c.Conn.Replicas + 1
	if clusterSizeTarget > quorumSize {
		quorumSize = clusterSizeTarget
	}

	// Are we even ready to launch this process yet?
	// Do we have a quorum?
	// Are all participants (other nodes except me) in a state of readiness (Active or Joining)?
	for {
		var startingCount, readyCount, unknownCount int
		nodeMap := c.Conn.GetNodeMap()
		select {
		case _, open := <-c.Conn.Stop:
			if !open {
				err := fmt.Errorf("Shutdown initiated while waiting to start synchronization.")
				return err
			}
		case <-time.After(DefaultPollInterval):
			for id := range nodeMap {
				status, err := GetNodeStatusForID(c.Conn, id)
				if err != nil {
					u.Warnf("error %v\n", err)
					unknownCount++
					continue
				}
				switch status.NodeState {
				case "Joining":
					readyCount++
				case "Starting":
					startingCount++
				case "Active":
					readyCount++
				default:
					u.Infof("nodes %s in unknown state %v.", id, status.NodeState)
					unknownCount++
				}
			}
		}
		// No nodes in starting state and quorum reached
		if startingCount == 0 && unknownCount == 0 && readyCount >= quorumSize {
			memberCount = readyCount
			break
		}
		if unknownCount > 0 {
			u.Infof("%d nodes in unknown state.", unknownCount)
		}
		u.Infof("Join status nodemap len = %d, starting = %d, ready = %d, unknown = %d\n",
			len(nodeMap), startingCount, readyCount, unknownCount)
	}

	time.Sleep(DefaultPollInterval)
	u.Warnf("All %d cluster members are ready to push data to me (sync).", memberCount)

	var eg errgroup.Group
	// Connect to all peers (except myself) and kick off a syncronization push process.
	for i, n := range c.client {
		myIndex := c.Conn.GetClientIndexForNodeID(nodeKey)
		if myIndex == -1 {
			u.Errorf("Client index not valid exiting.")
			os.Exit(1)
		}
		if myIndex == i {
			continue
		}
		client := n
		clientIndex := i
		eg.Go(func() error {
			if err := c.syncClient(client, nodeKey, clientIndex); err != nil {
				return err
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	u.Warnf("Synchronization complete.")

	return nil
}

// Send a sync request.
func (c *BitmapIndex) syncClient(client pb.BitmapIndexClient, nodeKey string, clientIndex int) error {

	ctx, cancel := context.WithTimeout(context.Background(), SyncDeadline)
	defer cancel()

	nKey := &wrappers.StringValue{Value: nodeKey}
	if client == nil {
		u.Errorf("client is nil for nodekey = %s, index = %d", nodeKey, clientIndex)
		return nil
	}

	if _, err := client.Synchronize(ctx, nKey); err != nil {
		return fmt.Errorf("%v.Synchronize(_) = _, %v, node = %s", client, err,
			c.Conn.ClientConnections()[clientIndex].Target())
	}
	return nil
}

// Send a sync status  request.
func (c *BitmapIndex) syncStatusClient(client pb.BitmapIndexClient, req *pb.SyncStatusRequest,
	clientIndex int) (*pb.SyncStatusResponse, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	response, err := client.SyncStatus(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("%v.SyncStatus(_) = _, %v, node = %s", client, err,
			c.Conn.ClientConnections()[clientIndex].Target())
	}
	return response, nil
}

func GetNodeStatusForID(conn *Conn, nodeID string) (*pb.StatusMessage, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	// Invoke Status API
	clientIndex := conn.GetClientIndexForNodeID(nodeID)
	if clientIndex == -1 {
		return nil, fmt.Errorf("node id %s has left.", nodeID)
	}
	result, err := conn.Admin[clientIndex].Status(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("%v.Status(_) = _, %v, node = %s\n", conn.Admin[clientIndex], err,
			conn.ClientConnections()[clientIndex].Target()))
	}
	return result, nil
}
