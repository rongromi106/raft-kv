package raft

import (
	"context"
	"fmt"
	"log"
)

type MemoryCluster struct {
	nodes  map[NodeID]*RaftNode
	logger *log.Logger
}

type Cluster interface {
	SendAppendEntries(from, to NodeID, req *AppendEntriesRequest)
	SendRequestVote(from, to NodeID, req *RequestVoteRequest)
	Init(ctx context.Context) error
}

func NewMemoryCluster(clusterSize int) Cluster {
	cluster := &MemoryCluster{
		nodes:  make(map[NodeID]*RaftNode),
		logger: log.Default(),
	}
	for i := 0; i < clusterSize; i++ {
		node := NewRaftNode(Config{
			ID: NodeID(fmt.Sprintf("node-%d", i)),
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
	return cluster
}

func (c *MemoryCluster) SendAppendEntries(from, to NodeID, req *AppendEntriesRequest) {
	return
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
