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
	network       *NetworkSimulator
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
			ElectionTimeoutMode: ElectionTimeoutRandom,
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
	// Initialize network simulator with no delay/drop by default.
	cluster.network = NewNetworkSimulator(0, 0, 0, func(to NodeID, msg RPCMessage) {
		cluster.nodes[to].recvRPCCh <- msg
	})
	return cluster
}

func (c *MemoryCluster) SendAppendEntries(from, to NodeID, req *AppendEntriesRequest) {
	c.network.SendAppendEntries(from, to, req)
}

func (c *MemoryCluster) SendRequestVote(from, to NodeID, req *RequestVoteRequest) {
	c.network.SendRequestVote(from, to, req)
}

func (c *MemoryCluster) SendRequestVoteResponse(from, to NodeID, resp *RequestVoteResponse) {
	c.network.SendRequestVoteResponse(from, to, resp)
}

func (c *MemoryCluster) SendAppendEntriesResponse(from, to NodeID, resp *AppendEntriesResponse) {
	c.network.SendAppendEntriesResponse(from, to, resp)
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
