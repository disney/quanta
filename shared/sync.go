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
	"unsafe"

	u "github.com/araddon/gou"
	pb "github.com/disney/quanta/grpc"
	"github.com/golang/protobuf/ptypes/wrappers"
	"golang.org/x/sync/errgroup"
)

// Synchronize flow.  Remote nodes call this when starting.  Peer nodes respond by pushing data.
// called by BitmapIndex in bitmap.go in verifyNode
// nodeKey is the nodeID of the node that is starting up.
func (c *BitmapIndex) Synchronize(nodeKey string) (int, error) {

	var memberCount int
	clusterSizeTarget, err := GetClusterSizeTarget(c.Conn.Consul)
	if err != nil {
		return -1, err
	}
	quorumSize := c.Conn.Replicas + 1 // why?
	if clusterSizeTarget > quorumSize {
		quorumSize = clusterSizeTarget
	}
	u.Debug("BitmapIndex Synchronize quorumSize", quorumSize, "node", nodeKey, " owner", c.Conn.owner)

	ourPollInterval := DefaultPollInterval
	if c.IsLocalCluster {
		ourPollInterval /= 5 // faster please. I can see my life passing before my eyes.
	}
	ourPollInterval = 1 * time.Second
	useThisInterval := 1 * time.Millisecond // start immediately

	for len(c.Conn.GetNodeMap()) < quorumSize {
		time.Sleep(100 * time.Millisecond)
	}
	u.Debug("BitmapIndex Synchronize nodeMap len", len(c.Conn.GetNodeMap()), "quorumSize", quorumSize, "node", nodeKey, " owner", c.Conn.owner)

	// Are we even ready to launch this process yet?
	// Do we have a quorum?
	// Are all participants (other nodes except me) in a state of readiness (Active, Syncing  or Joining)?
	pass := 0
	for {
		var startingCount, readyCount, unknownCount int
		u.Debug("BitmapIndex Synchronize nodeMap", c.Conn.owner, c.Conn.GetNodeMap(), "pass", pass, uintptr(unsafe.Pointer(c.Conn)))
		pass += 1
		select {
		case _, open := <-c.Conn.Stop:
			if !open {
				err := fmt.Errorf("shutdown initiated while waiting to start synchronization")
				return -1, err
			}
		case <-time.After(useThisInterval):
			useThisInterval = ourPollInterval

			u.Debug("BitmapIndex Synchronize node loop starting", c.Conn.owner, pass, c.Conn.GetNodeMap(), uintptr(unsafe.Pointer(c.Conn)))

			for id := range c.Conn.GetNodeMap() {
				u.Debug("Synchronize getNodeStatusForID starts", id, c.Conn.owner, pass)
				status, err := c.Conn.getNodeStatusForID(id)
				u.Debug("Synchronize getNodeStatusForID after", id, c.Conn.owner, status.NodeState, pass)
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
			u.Debug("BitmapIndex Synchronize node loop done", c.Conn.owner, pass, c.Conn.GetNodeMap(), uintptr(unsafe.Pointer(c.Conn)))
		} // end select
		// No nodes in starting state and quorum reached
		if startingCount == 0 && unknownCount == 0 && readyCount >= quorumSize {
			memberCount = readyCount
			break
		}
		if unknownCount > 0 {
			u.Infof("%d nodes in unknown state.", unknownCount)
		}
		u.Infof("%s BitmapIndex Synchronize Join status %v, starting = %d, ready = %d, unknown = %d quorum = %d\n", nodeKey,
			c.Conn.GetNodeMap(), startingCount, readyCount, unknownCount, quorumSize)
	} // end for

	time.Sleep(ourPollInterval) // why? atw delete me
	u.Warnf("All %d cluster members are ready to push data to me (%s), I am now in Syncing State.", memberCount, nodeKey)

	resultChan := make(chan int64, memberCount-1)
	var diffCount int
	var eg errgroup.Group
	// Connect to all peers (except myself) and kick off a syncronization push process.
	for i, n := range c.client {
		myIndex, err := c.Conn.GetClientIndexForNodeID(nodeKey)
		if err != nil {
			u.Errorf("shared/sync:GetClientIndexForNodeID(%s) failed", nodeKey)
			os.Exit(1)
		}
		if myIndex == i {
			continue // don't sync with myself
		}
		client := n
		clientIndex := i
		eg.Go(func() error {
			u.Debug("Synchronize syncClient", nodeKey, c.owner, clientIndex)
			diffc, err := c.syncClient(client, nodeKey, clientIndex) // send sync grpc request to peer
			u.Debug("done Synchronize syncClient", nodeKey, c.owner, clientIndex)

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

// syncClient Sends a sync request grpg.
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
