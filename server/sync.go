package server

import (
	"context"
	"fmt"
    u "github.com/araddon/gou"
    pb "github.com/disney/quanta/grpc"
    "github.com/disney/quanta/shared"
    "github.com/golang/protobuf/ptypes/empty"
    "github.com/golang/protobuf/ptypes/wrappers"
    "github.com/RoaringBitmap/roaring/roaring64"
	"os"
	"strings"
	"time"
)

// SyncStatus - Sync handshake.
/*  SyncStatusRequest
    Index       string `protobuf:"bytes,1,opt,name=index,proto3" json:"index,omitempty"`
    Field       string `protobuf:"bytes,2,opt,name=field,proto3" json:"field,omitempty"`
    RowId       int64  `protobuf:"varint,3,opt,name=rowId,proto3" json:"rowId,omitempty"`
    Time        int64  `protobuf:"varint,4,opt,name=time,proto3" json:"time,omitempty"`
    Cardinality uint64 `protobuf:"varint,5,opt,name=cardinality,proto3" json:"cardinality,omitempty"`
    BsiChecksum int64  `protobuf:"varint,6,opt,name=bsiChecksum,proto3" json:"bsiChecksum,omitempty"`
    ModTime     int64  `protobuf:"varint,7,opt,name=modTime,proto3" json:"modTime,omitempty"`
    SendData    bool   `protobuf:"varint,8,opt,name=sendData,proto3" json:"sendData,omitempty"`
*/
func (m *BitmapIndex) SyncStatus(ctx context.Context, req *pb.SyncStatusRequest) (*pb.SyncStatusResponse, error) {

	if req.Index == "" {
        return nil, fmt.Errorf("index not specified for sync status.")
    }
    if req.Field == "" {
        return nil, fmt.Errorf("field not specified for sync status.")
    }

    // Silently ignore non-existing fields for now
    attr, err := m.getFieldConfig(req.Index, req.Field)
    if err != nil {
        return nil, err
    }

    tq := attr.Parent.TimeQuantumType
	if tq != "" && req.Time == 0 {
        return nil, fmt.Errorf("time not specified for sync status and time quantum is enabled for %s.", req.Index)
	}

    isBSI := m.isBSI(req.Index, req.Field)
	response := &pb.SyncStatusResponse{}
	var skew time.Duration
	reqTime := time.Unix(0, req.ModTime)
    if isBSI {
    	v := m.bsiCache[req.Index][req.Field][req.Time]
		if v == nil {
		    return response, nil
		}
		v.Lock.RLock()
		defer v.Lock.RUnlock()
		sum, card := v.Sum(v.GetExistenceBitmap())

		response.Cardinality = uint64(card)
		response.ModTime = v.ModTime.UnixNano()
		response.BSIChecksum = int64(sum)
		if v.ModTime.After(reqTime) {
			skew = v.ModTime.Sub(reqTime)
		} else {
			skew = reqTime.Sub(v.ModTime)
		}
		cardDiff := response.Cardinality - req.Cardinality
		sumDiff := response.BSIChecksum - req.BSIChecksum
		response.Ok = cardDiff == 0 && sumDiff == 0 && skew.Milliseconds() < 100
		if !response.Ok && req.SendData {
	        ba, err := v.MarshalBinary()
	        if err != nil {
	            return response, err
	        }
		    response.Data = ba
		}
	} else {
    	v := m.bitmapCache[req.Index][req.Field][req.RowId][req.Time]
		if v == nil {
		    return response, nil
		}
		v.Lock.RLock()
		defer v.Lock.RUnlock()
		response.Cardinality = v.Bits.GetCardinality()
		response.ModTime = v.ModTime.UnixNano()
		if v.ModTime.After(reqTime) {
			skew = v.ModTime.Sub(reqTime)
		} else {
			skew = reqTime.Sub(v.ModTime)
		}
		cardDiff := response.Cardinality - req.Cardinality
		response.Ok = cardDiff == 0 && skew.Milliseconds() < 100
		if !response.Ok && req.SendData {
	        buf, err := v.Bits.ToBytes()
	        if err != nil {
	            return response, err
	        }
	        ba := make([][]byte, 1)
			ba[0] = buf
	        response.Data = ba
		}
	}

/*  SyncStatusResponse
    Ok          bool     `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
    Cardinality uint64   `protobuf:"varint,2,opt,name=cardinality,proto3" json:"cardinality,omitempty"`
    BsiChecksum int64    `protobuf:"varint,3,opt,name=bsiChecksum,proto3" json:"bsiChecksum,omitempty"`
    ModTime     int64    `protobuf:"varint,4,opt,name=modTime,proto3" json:"modTime,omitempty"`
    Data        [][]byte `protobuf:"bytes,5,rep,name=data,proto3" json:"data,omitempty"`
*/
	return response, nil
}

// Synchronize - Connect to peer to pushback deltas
func (m *BitmapIndex) Synchronize(ctx context.Context, req *wrappers.StringValue) (*empty.Empty, error) {

    // This is the entire synchronization flow.  Connect to new node and push data.
	newNodeID := req.Value

	// FIXME - this will break
	ci := m.GetClientIndexForNodeID(newNodeID)
	if ci == -1 {
        u.Errorf("GetClientForNodeID = %s failed.", newNodeID)
		os.Exit(1)
	}
	targetIP := m.ClientConnections()[ci].Target()

    cx, cancel := context.WithTimeout(context.Background(), shared.Deadline)
    defer cancel()

	// Invoke status IP on new node.
	status, err := m.Admin[ci].Status(cx, &empty.Empty{})
	if err != nil {
        return &empty.Empty{}, fmt.Errorf(fmt.Sprintf("%v.Status(_) = _, %v, node = %s\n", m.Admin[ci], err, targetIP))
	}

    // Verify that client stub IP (targetIP) is the same as the new nodes IP returned by status.
	if ! strings.HasPrefix(targetIP, status.LocalIP) {
        return &empty.Empty{}, fmt.Errorf("Stub IP %v does not match new node (remote) = %v", targetIP, status.LocalIP)
	}

	u.Infof("New joining node %s is requesting a sync push.", newNodeID)

	peerClient := m.Conn.GetService("BitmapIndex").(*shared.BitmapIndex)
	newNode := peerClient.Client(ci) // <- bitmap peer client for new node.

	// Iterate over bitmap cache.  
    for indexName, index := range m.bitmapCache {
        for fieldName, field := range index {
			// TODO: If field is StringEnum, push metadata
            for rowID, ts := range field {
                for t, bitmap := range ts {
					// Should this item be pushed to new node?
					key := fmt.Sprintf("%s/%s/%d/%s", indexName, fieldName, rowID, time.Unix(0, t).Format(timeFmt))
					nMap := m.getNodeMapForKey(newNodeID, key)
					replica, found := nMap[newNodeID]
					if !found {
						continue   // nope soldier on
					}
					u.Infof("Key %s should be replica %d on node %s.", key, replica, newNodeID)
					// invoke status check
					bitmap.Lock.RLock()
					reqs := &pb.SyncStatusRequest{Index: indexName, Field: fieldName, RowId: rowID, Time: t, SendData: true,
						Cardinality: bitmap.Bits.GetCardinality(),
						ModTime: bitmap.ModTime.UnixNano(),
					}
					res, err := newNode.SyncStatus(cx, reqs)
					bitmap.Lock.RUnlock()
					if err != nil {
						return &empty.Empty{}, 
								fmt.Errorf(fmt.Sprintf("%v.SyncStatus(_) = _, %v, node = %s\n", newNode, err, targetIP))
					}
					if res.Ok {
					    u.Infof("No differences for key %s.", key)
						continue   // data matches
					}
					// TODO: Unmarshal data from response
					resBm := roaring64.NewBitmap()
					if len(res.Data) != 1 && res.Cardinality > 0 {
						return &empty.Empty{}, 
							fmt.Errorf("deserialize sync response - Index out of range %d, Index = %s, Field = %s, rowId = %d",
								len(res.Data), indexName, fieldName, rowID)
					}
					if len(res.Data) == 1 && res.Cardinality > 0 {
						if err := resBm.UnmarshalBinary(res.Data[0]); err != nil {
							return &empty.Empty{}, 
									fmt.Errorf("deserialize sync reponse - UnmarshalBinary error - %v", err)
						}
					}
					// TODO: Calculate the diff
					pushDiff := roaring64.AndNot(bitmap.Bits, resBm)
					//pullDiff := roaring64.AndNot(resBm, bitmap.Bits)

					// TODO: Push the pushDiff, OR in the localDiff
					u.Infof("Pushing server diff for key %s, local = %d, remote (new) = %d, delta = %d.\n", key, 
						bitmap.Bits.GetCardinality(), resBm.GetCardinality(), pushDiff.GetCardinality())
					if resBm.GetCardinality() > 0 {
						// There was something on remote
					}
				}
			}
		}
	}

	return &empty.Empty{}, nil
}

// Return a map of weighted nodes for a given lookup key
func (m *BitmapIndex) getNodeMapForKey(newNodeID, key string) map[string]int {

	newHashTable := m.Conn.GetHashTableWithNewNodes([]string{newNodeID})
	nodeKeys := newHashTable.GetN(m.Conn.Replicas, newNodeID)
	nodeMap := make(map[string]int, m.Conn.Replicas)
	for i, v := range nodeKeys {
		nodeMap[v] = i
	} 
	return nodeMap
}

