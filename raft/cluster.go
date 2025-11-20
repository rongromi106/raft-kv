package raft

import (
	"context"
	"fmt"
	"log"
	"time"
)

type MemoryCluster struct {
	nodes         map[NodeID]*RaftNode
	logger        *log.Logger
	currentLeader NodeID
	tracker       *ElectionTracker
}

type Cluster interface {
	SendAppendEntries(from, to NodeID, req *AppendEntriesRequest)
	SendRequestVote(from, to NodeID, req *RequestVoteRequest)
	Init(ctx context.Context) error
	Stop()
	SendRequestVoteResponse(from, to NodeID, resp *RequestVoteResponse)
	SendAppendEntriesResponse(from, to NodeID, resp *AppendEntriesResponse)
	SetCurrentLeader(leader NodeID)
	KillCurrentLeader() NodeID
	SendRoleChange(node NodeID, term Term, oldRole, newRole Role)
	ElectionSamples() []time.Duration
}

func NewMemoryCluster(clusterSize int) Cluster {
	cluster := &MemoryCluster{
		nodes:  make(map[NodeID]*RaftNode),
		logger: log.Default(),
	}
	for i := 0; i < clusterSize; i++ {
		node := NewRaftNode(Config{
			ID:                  NodeID(fmt.Sprintf("node-%d", i)),
			HeartbeatInterval:   100 * time.Millisecond,
			MinElectionTimeout:  300 * time.Millisecond,
			MaxElectionTimeout:  600 * time.Millisecond,
			Logger:              log.Default(),
			ElectionTimeoutMode: ElectionTimeoutFixed,
		}, cluster)
		cluster.nodes[node.id] = node
	}
	for i := 0; i < clusterSize; i++ {
		selfID := NodeID(fmt.Sprintf("node-%d", i))
		selfNode := cluster.nodes[selfID]
		for j := 0; j < clusterSize; j++ {
			if j == i {
				continue
			}
			selfNode.peerIds = append(selfNode.peerIds, NodeID(fmt.Sprintf("node-%d", j)))
		}
	}
	cluster.tracker = NewElectionTracker(cluster.nodes)
	return cluster
}

func (c *MemoryCluster) SendAppendEntries(from, to NodeID, req *AppendEntriesRequest) {
	msg := RPCMessage{
		From:             from,
		To:               to,
		Type:             RPCAppendEntries,
		AppendEntriesReq: req,
	}
	go func() {
		c.nodes[to].recvRPCCh <- msg
	}()
}

func (c *MemoryCluster) SendRequestVote(from, to NodeID, req *RequestVoteRequest) {
	msg := RPCMessage{
		From:           from,
		To:             to,
		Type:           RPCRequestVote,
		RequestVoteReq: req,
	}
	go func() {
		c.nodes[to].recvRPCCh <- msg
	}()
}

func (c *MemoryCluster) SendRequestVoteResponse(from, to NodeID, resp *RequestVoteResponse) {
	msg := RPCMessage{
		From:            from,
		To:              to,
		Type:            RPCRequestVoteResponse,
		RequestVoteResp: resp,
	}
	go func() {
		c.nodes[to].recvRPCCh <- msg
	}()
}

func (c *MemoryCluster) SendAppendEntriesResponse(from, to NodeID, resp *AppendEntriesResponse) {
	msg := RPCMessage{
		From:              from,
		To:                to,
		Type:              RPCAppendEntriesResponse,
		AppendEntriesResp: resp,
	}
	go func() {
		c.nodes[to].recvRPCCh <- msg
	}()
}

func (c *MemoryCluster) Init(ctx context.Context) error {
	for _, node := range c.nodes {
		err := node.Start(context.Background())
		if err != nil {
			c.logger.Fatalf("start node: %v", err)
			return err
		}
	}
	return nil
}

func (c *MemoryCluster) Stop() {
	for _, node := range c.nodes {
		node.Stop()
	}
}

func (c *MemoryCluster) SetCurrentLeader(leader NodeID) {
	c.currentLeader = leader
}

// this is for testing only, by not sending heartbeat to followers, we can test the re-election process
func (c *MemoryCluster) KillCurrentLeader() NodeID {
	leader := c.currentLeader
	c.nodes[leader].stopHeartbeatTimer()
	c.currentLeader = ""
	return leader
}

func (c *MemoryCluster) SendRoleChange(node NodeID, term Term, oldRole, newRole Role) {
	c.tracker.OnRoleChange(node, term, oldRole, newRole)
}

func (c *MemoryCluster) ElectionSamples() []time.Duration {
	return c.tracker.Samples()
}
