package raft

import (
	"context"
	"fmt"
	"log"
	"time"
)

type MemoryCluster struct {
	nodes              map[NodeID]*RaftNode
	logger             *log.Logger
	currentLeader      NodeID
	tracker            *ElectionTracker
	network            *NetworkSimulator
	numberOfPartitions int
	partitions         map[NodeID]int
	queue              chan ClientPutRequest
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
	CreatePartitions(numberOfPartitions int)
	// Client put request ingress and subscription
	SendClientPut(req ClientPutRequest)
	ClientPutRequests() <-chan ClientPutRequest
	Get(key string) string
}

func NewMemoryCluster(cfg *ClusterConfig) Cluster {
	if cfg == nil {
		def := (&ClusterConfig{}).withThreeNodesPerfectNetwork()
		def.NumberOfPartitions = 1
		cfg = &def
	}
	cluster := &MemoryCluster{
		nodes:              make(map[NodeID]*RaftNode),
		logger:             log.Default(),
		numberOfPartitions: cfg.NumberOfPartitions,
		queue:              make(chan ClientPutRequest, 100),
	}
	for i := 0; i < cfg.ClusterSize; i++ {
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
	for i := 0; i < cfg.ClusterSize; i++ {
		selfID := NodeID(fmt.Sprintf("node-%d", i))
		selfNode := cluster.nodes[selfID]
		for j := 0; j < cfg.ClusterSize; j++ {
			if j == i {
				continue
			}
			selfNode.peerIds = append(selfNode.peerIds, NodeID(fmt.Sprintf("node-%d", j)))
		}
		selfNode.state.nextIndex = make(map[NodeID]LogIndex)
	}
	cluster.tracker = NewElectionTracker(cluster.nodes)
	// Initialize network simulator from config (defaults ensured by withThreeNodesPerfectNetwork).
	cluster.network = NewNetworkSimulator(cfg.NetworkConfig.MinDelayMs, cfg.NetworkConfig.MaxDelayMs, cfg.NetworkConfig.DropRate, func(to NodeID, msg RPCMessage) {
		cluster.nodes[to].recvRPCCh <- msg
	})
	return cluster
}

func (c *MemoryCluster) SendAppendEntries(from, to NodeID, req *AppendEntriesRequest) {
	// stop cross partition RPC
	if c.numberOfPartitions > 1 {
		if c.partitions[from] != c.partitions[to] {
			return
		}
	}
	c.network.SendAppendEntries(from, to, req)
}

func (c *MemoryCluster) SendRequestVote(from, to NodeID, req *RequestVoteRequest) {
	// stop cross partition RPC
	if c.numberOfPartitions > 1 {
		if c.partitions[from] != c.partitions[to] {
			return
		}
	}
	c.network.SendRequestVote(from, to, req)
}

func (c *MemoryCluster) SendRequestVoteResponse(from, to NodeID, resp *RequestVoteResponse) {
	// stop cross partition RPC
	if c.numberOfPartitions > 1 {
		if c.partitions[from] != c.partitions[to] {
			return
		}
	}
	c.network.SendRequestVoteResponse(from, to, resp)
}

func (c *MemoryCluster) SendAppendEntriesResponse(from, to NodeID, resp *AppendEntriesResponse) {
	// stop cross partition RPC
	if c.numberOfPartitions > 1 {
		if c.partitions[from] != c.partitions[to] {
			return
		}
	}
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

func (c *MemoryCluster) CreatePartitions(numberOfPartitions int) {
	// set and validate partition count
	c.numberOfPartitions = numberOfPartitions
	if c.numberOfPartitions <= 1 {
		return
	}
	c.partitions = make(map[NodeID]int)
	count := 0
	for nodeId := range c.nodes {
		partitionId := count % c.numberOfPartitions
		c.partitions[nodeId] = partitionId
		c.logger.Printf("[cluster] assigned node %s to partition %d", nodeId, partitionId)
		count++
	}
}

func (c *MemoryCluster) SendClientPut(req ClientPutRequest) {
	go func() {
		c.queue <- req
	}()
}

func (c *MemoryCluster) ClientPutRequests() <-chan ClientPutRequest {
	return c.queue
}

func (c *MemoryCluster) Get(key string) string {
	if len(c.nodes) == 0 {
		return ""
	}
	// pick a pseudo-random node based on current time
	nodes := make([]*RaftNode, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	idx := int(time.Now().UnixNano() % int64(len(nodes)))
	node := nodes[idx]
	val := node.kvstore.Get(key)
	_, role := node.state.getTermAndRole()
	return fmt.Sprintf("node=%s role=%s value=%s", node.id, role, val)
}
