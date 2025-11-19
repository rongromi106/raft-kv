package raft

import (
	"context"
	"log"
	"sync"
	"time"
)

type RaftNode struct {
	id      NodeID
	cfg     Config
	logger  *log.Logger
	cluster Cluster

	state *RaftState

	recvRPCCh chan RPCMessage
	stopCh    chan struct{}
	stopped   chan struct{}

	electionTimer *time.Timer

	mu sync.Mutex

	peerIds []NodeID
}

func NewRaftNode(cfg Config, cluster Cluster) *RaftNode {
	c := cfg.withDefaults()

	n := &RaftNode{
		id:      c.ID,
		cfg:     c,
		logger:  c.Logger,
		cluster: cluster,

		state: newRaftState(),

		recvRPCCh: make(chan RPCMessage, 16),
		stopCh:    make(chan struct{}),
		stopped:   make(chan struct{}),
	}

	return n
}

func (n *RaftNode) Start(ctx context.Context) error {
	n.logger.Printf("[node %s] starting", n.id)

	timeout := newElectionTimeout(n.cfg.MinElectionTimeout, n.cfg.MaxElectionTimeout)
	n.resetElectionTimer(timeout)

	go n.run(ctx)
	return nil
}

func (n *RaftNode) Stop() {
	select {
	case <-n.stopped:
		return
	default:
		close(n.stopCh)
		<-n.stopped
	}
}

func (n *RaftNode) run(ctx context.Context) {
	defer close(n.stopped)
	n.logger.Printf("[node %s] main loop started", n.id)

	for {
		select {
		case <-ctx.Done():
			n.logger.Printf("[node %s] context cancelled, exit", n.id)
			return

		case <-n.stopCh:
			n.logger.Printf("[node %s] stop signal received", n.id)
			return

		case <-n.electionTimer.C:
			term, role := n.state.getTermAndRole()
			n.startElection()
			n.logger.Printf("[node %s] election timeout fired (term=%d, role=%s) -> starting election",
				n.id, term, role)
			n.resetElectionTimer(newElectionTimeout(n.cfg.MinElectionTimeout, n.cfg.MaxElectionTimeout))

		case msg := <-n.recvRPCCh:
			n.handleRPC(msg)
		}
	}
}

func (n *RaftNode) resetElectionTimer(d time.Duration) {
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(d)
	} else {
		if !n.electionTimer.Stop() {
			select {
			case <-n.electionTimer.C:
			default:
			}
		}
		n.electionTimer.Reset(d)
	}
	n.logger.Printf("[node %s] reset election timer to %v", n.id, d)
}

func (n *RaftNode) handleRPC(msg RPCMessage) {
	switch msg.Type {
	case RPCRequestVote:
		n.logger.Printf("[node %s] received RequestVote request from=%s (term=%d)",
			n.id, msg.From, msg.RequestVoteReq.Term)

	case RPCAppendEntries:
		n.logger.Printf("[node %s] received AppendEntries request from=%s (term=%d)",
			n.id, msg.From, msg.AppendEntriesReq.Term)

	default:
		term, role := n.state.getTermAndRole()
		n.logger.Printf("[node %s] received RPC type=%v from=%s (term=%d, role=%s) [no-op in Milestone1]",
			n.id, msg.Type, msg.From, term, role)
	}

}

func (n *RaftNode) startElection() {
	/*
		currentTerm++
		role = Candidate
		votedFor = self
		voteCount = 1
		broadcast RequestVote
		reset election timer
	*/
	n.state.mu.Lock()
	n.state.currentTerm++
	n.state.role = RoleCandidate
	n.state.votedFor = &n.id

	n.state.voteCount = 1
	n.state.mu.Unlock()
	currentTerm, _ := n.state.getTermAndRole()
	for _, peerId := range n.peerIds {
		// TODO: fix lastlog index and last log term in milestone 3
		n.cluster.SendRequestVote(n.id, peerId, &RequestVoteRequest{
			Term:         currentTerm,
			CandidateID:  n.id,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})
	}
	n.resetElectionTimer(newElectionTimeout(n.cfg.MinElectionTimeout, n.cfg.MaxElectionTimeout))
}
