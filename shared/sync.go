package shared

//
// This code executes on the node server side "peer client"
// API wrappers for node startup synchronization resulting in successful "node join".
//

import (
	"context"
	"fmt"
	"os"
	"time"

	u "github.com/araddon/gou"
	pb "github.com/disney/quanta/grpc"
	"github.com/golang/protobuf/ptypes/wrappers"
	"golang.org/x/sync/errgroup"
)

// Synchronize flow.  Remote nodes call this when starting.  Peer nodes respond my pushing data.
func (c *BitmapIndex) Synchronize(nodeKey string) (int, error) {

	var memberCount int
	clusterSizeTarget, err := GetClusterSizeTarget(c.Conn.Consul)
	if err != nil {
		return -1, err
	}
	quorumSize := c.Conn.Replicas + 1
	if clusterSizeTarget > quorumSize {
		quorumSize = clusterSizeTarget
	}
	fmt.Println("BitmapIndex Synchronize quorumSize", quorumSize, "node", nodeKey)

	ourPollInterval := DefaultPollInterval
	if c.IsLocalCluster {
		ourPollInterval /= 5 // faster please. I can see my life passing before my eyes.
	}

	// Are we even ready to launch this process yet?
	// Do we have a quorum?
	// Are all participants (other nodes except me) in a state of readiness (Active, Syncing  or Joining)?
	for {
		var startingCount, readyCount, unknownCount int
		nodeMap := c.Conn.GetNodeMap()
		select {
		case _, open := <-c.Conn.Stop:
			if !open {
				err := fmt.Errorf("Shutdown initiated while waiting to start synchronization")
				return -1, err
			}
		case <-time.After(ourPollInterval):
			for id := range nodeMap {
				// fmt.Println("Synchronize 1", id, c.Conn.ServicePort, c.Conn.ServiceName)
				status, err := c.Conn.getNodeStatusForID(id)
				// fmt.Println("Synchronize getNodeStatusForID", id, status, err)
				if err != nil {
					u.Warnf("error %v\n", err)
					unknownCount++
					continue
				}
				switch status.NodeState {
				case "Joining":
					//readyCount++
					startingCount++
				case "Starting":
					startingCount++
				case "Syncing":
					readyCount++
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
		u.Infof("%s Join status nodemap len = %d, starting = %d, ready = %d, unknown = %d\n", nodeKey,
			len(nodeMap), startingCount, readyCount, unknownCount)
	}

	time.Sleep(ourPollInterval)
	u.Warnf("All %d cluster members are ready to push data to me, I am now in Syncing State.", memberCount)

	resultChan := make(chan int64, memberCount-1)
	var diffCount int
	var eg errgroup.Group
	// Connect to all peers (except myself) and kick off a syncronization push process.
	for i, n := range c.client {
		myIndex := c.Conn.GetClientIndexForNodeID(nodeKey)
		if myIndex == -1 {
			u.Errorf("shared/sync:GetClientIndexForNodeID(%s) failed", nodeKey)
			os.Exit(1)
		}
		if myIndex == i {
			continue
		}
		client := n
		clientIndex := i
		eg.Go(func() error {
			diffc, err := c.syncClient(client, nodeKey, clientIndex)
			if err != nil {
				return err
			}
			resultChan <- diffc
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return -1, err
	}
	close(resultChan)

	for dr := range resultChan {
		diffCount += int(dr)
	}

	if diffCount == 0 {
		u.Warnf("Synchronization complete for node %s, no differences detected", nodeKey)
	} else {
		u.Warnf("Synchronization complete for node %s, %d differences detected", nodeKey, diffCount)
	}

	return diffCount, nil
}

// Send a sync request.
func (c *BitmapIndex) syncClient(client pb.BitmapIndexClient, nodeKey string, clientIndex int) (int64, error) {

	ctx, cancel := context.WithTimeout(context.Background(), SyncDeadline)
	defer cancel()

	nKey := &wrappers.StringValue{Value: nodeKey}
	if client == nil {
		err := fmt.Errorf("client is nil for nodekey = %s, index = %d", nodeKey, clientIndex)
		return int64(-1), err
	}

	diffCount, err := client.Synchronize(ctx, nKey)
	if err != nil {
		return int64(-1), fmt.Errorf("%v.Synchronize(_) = _, %v, node = %s", client, err,
			c.Conn.ClientConnections()[clientIndex].Target())
	}
	return diffCount.Value, nil
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
