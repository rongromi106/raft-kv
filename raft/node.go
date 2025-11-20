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

	heartbeatTimer *time.Timer

	// Used only when ElectionTimeoutMode == ElectionTimeoutFixed
	fixedElectionTimeout time.Duration

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

	// Initialize per-node fixed election timeout if configured
	if c.ElectionTimeoutMode == ElectionTimeoutFixed {
		// node always use the same election timeout
		n.fixedElectionTimeout = newElectionTimeout(c.MinElectionTimeout, c.MaxElectionTimeout)
	}

	return n
}

func (n *RaftNode) Start(ctx context.Context) error {
	n.logger.Printf("[node %s] starting", n.id)

	n.resetElectionTimerByMode()

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

	// Enable heartbeat tick only for leaders; nil channel disables the select case.
	for {
		var hbCh <-chan time.Time
		_, role := n.state.getTermAndRole()
		if role == RoleLeader {
			if n.heartbeatTimer == nil {
				n.resetHeartbeatTimer(n.cfg.HeartbeatInterval)
			}
			hbCh = n.heartbeatTimer.C
		}

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
			n.resetElectionTimerByMode()

		case msg := <-n.recvRPCCh:
			n.handleRPC(msg)

		case <-hbCh:
			term, role := n.state.getTermAndRole()
			if role == RoleLeader {
				n.startHeartbeat()
				n.logger.Printf("[node %s] heartbeat timeout fired (term=%d, role=%s) -> starting heartbeat",
					n.id, term, role)
				n.resetHeartbeatTimer(n.cfg.HeartbeatInterval)
			}
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

// resetElectionTimerFixed resets the election timer using a fixed duration,
// typically the configured MinElectionTimeout. This provides an option to
// run elections on a fixed schedule instead of randomized intervals.
func (n *RaftNode) resetElectionTimerFixed() {
	d := n.fixedElectionTimeout
	if d == 0 {
		// Fallback in case not pre-initialized; keep behavior deterministic per call site
		d = n.cfg.MinElectionTimeout
	}
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
	n.logger.Printf("[node %s] reset election timer (fixed) to %v", n.id, d)
}

// resetElectionTimerByMode picks fixed or randomized timeout based on config.
func (n *RaftNode) resetElectionTimerByMode() {
	if n.cfg.ElectionTimeoutMode == ElectionTimeoutFixed {
		n.resetElectionTimerFixed()
		return
	}
	n.resetElectionTimer(newElectionTimeout(n.cfg.MinElectionTimeout, n.cfg.MaxElectionTimeout))
}

func (n *RaftNode) resetHeartbeatTimer(d time.Duration) {
	if n.heartbeatTimer == nil {
		n.heartbeatTimer = time.NewTimer(d)
	} else {
		if !n.heartbeatTimer.Stop() {
			select {
			case <-n.heartbeatTimer.C:
			default:
			}
		}
		n.heartbeatTimer.Reset(d)
	}
	n.logger.Printf("[node %s] reset heartbeat timer to %v", n.id, d)
}

func (n *RaftNode) stopHeartbeatTimer() {
	if n.heartbeatTimer == nil {
		return
	}
	if !n.heartbeatTimer.Stop() {
		select {
		case <-n.heartbeatTimer.C:
		default:
		}
	}
}

func (n *RaftNode) handleRPC(msg RPCMessage) {
	switch msg.Type {
	case RPCRequestVote:
		n.logger.Printf("[node %s] received RequestVote request from=%s (term=%d)",
			n.id, msg.From, msg.RequestVoteReq.Term)
		n.handleRequestVote(msg.RequestVoteReq)
	case RPCRequestVoteResponse:
		n.logger.Printf("[node %s] received RequestVoteResponse from=%s (term=%d)",
			n.id, msg.From, msg.RequestVoteResp.Term)
		n.handleRequestVoteResponse(msg.RequestVoteResp)
	case RPCAppendEntries:
		n.logger.Printf("[node %s] received AppendEntries request from=%s (term=%d)",
			n.id, msg.From, msg.AppendEntriesReq.Term)
		n.handleAppendEntries(msg.AppendEntriesReq)
	case RPCAppendEntriesResponse:
		n.logger.Printf("[node %s] received AppendEntriesResponse from=%s (term=%d)",
			n.id, msg.From, msg.AppendEntriesResp.Term)
		n.handleAppendEntriesResponse(msg.AppendEntriesResp)
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
	oldRole := n.state.role
	n.state.currentTerm++
	n.state.role = RoleCandidate
	n.state.votedFor = &n.id

	n.state.voteCount = 1
	term := n.state.currentTerm
	n.state.mu.Unlock()
	if oldRole != RoleCandidate {
		n.onRoleChange(term, oldRole, RoleCandidate)
	}
	currentTerm := term
	for _, peerId := range n.peerIds {
		// TODO: fix lastlog index and last log term in milestone 3
		n.cluster.SendRequestVote(n.id, peerId, &RequestVoteRequest{
			Term:         currentTerm,
			CandidateID:  n.id,
			LastLogIndex: 0,
			LastLogTerm:  0,
		})
	}
	n.resetElectionTimerByMode()
}

func (n *RaftNode) handleRequestVote(req *RequestVoteRequest) {
	n.state.mu.Lock()

	// vote granted for leader and reset election timeout to prevent unnecessary elections
	if req.Term > n.state.currentTerm && (n.state.votedFor == nil || *n.state.votedFor == req.CandidateID) {
		// step down and record vote
		oldRole := n.state.role
		n.state.currentTerm = req.Term
		n.state.votedFor = &req.CandidateID
		n.state.role = RoleFollower
		n.state.voteCount = 0
		term := n.state.currentTerm
		candidateID := req.CandidateID
		n.state.mu.Unlock()
		// actions after releasing lock
		n.cluster.SendRequestVoteResponse(n.id, candidateID, &RequestVoteResponse{
			Term:        term,
			VoteGranted: true,
		})
		n.stopHeartbeatTimer()
		n.resetElectionTimerByMode()
		if oldRole != RoleFollower {
			n.onRoleChange(term, oldRole, RoleFollower)
		}
		return
	}

	// reject vote request
	n.state.mu.Unlock()
}

func (n *RaftNode) handleRequestVoteResponse(resp *RequestVoteResponse) {
	n.state.mu.Lock()

	if resp.VoteGranted {
		n.state.voteCount++
		if n.state.voteCount > len(n.peerIds)/2 {
			oldRole := n.state.role
			n.state.role = RoleLeader
			term := n.state.currentTerm
			n.state.mu.Unlock()
			if oldRole != RoleLeader {
				n.onRoleChange(term, oldRole, RoleLeader)
			}
			n.cluster.SetCurrentLeader(n.id)
			n.startHeartbeat()
			return
		}
	}
	n.state.mu.Unlock()
}

func (n *RaftNode) startHeartbeat() {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()

	for _, peerId := range n.peerIds {
		if peerId == n.id {
			continue
		}
		n.cluster.SendAppendEntries(n.id, peerId, &AppendEntriesRequest{
			Term:         n.state.currentTerm,
			LeaderID:     n.id,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      make([]LogEntry, 0),
			LeaderCommit: 0,
			IsHeartbeat:  true,
		})

	}
	n.resetHeartbeatTimer(n.cfg.HeartbeatInterval)

}

func (n *RaftNode) handleAppendEntries(req *AppendEntriesRequest) {
	n.state.mu.Lock()

	if req.Term > n.state.currentTerm {
		oldRole := n.state.role
		n.state.currentTerm = req.Term
		n.state.votedFor = nil
		n.state.role = RoleFollower
		n.state.voteCount = 0
		term := n.state.currentTerm
		// step down: ensure we stop heartbeats
		n.state.mu.Unlock()
		n.stopHeartbeatTimer()
		if oldRole != RoleFollower {
			n.onRoleChange(term, oldRole, RoleFollower)
		}
		return
	} else if req.Term < n.state.currentTerm {
		n.cluster.SendAppendEntriesResponse(n.id, req.LeaderID, &AppendEntriesResponse{
			Term:    n.state.currentTerm,
			Success: false,
		})
		n.state.mu.Unlock()
		return
	}
	n.state.mu.Unlock()
}

func (n *RaftNode) handleAppendEntriesResponse(resp *AppendEntriesResponse) {
	n.state.mu.Lock()

	if !resp.Success {
		oldRole := n.state.role
		n.state.currentTerm = resp.Term
		n.state.votedFor = nil
		n.state.role = RoleFollower
		n.state.voteCount = 0
		term := n.state.currentTerm
		n.state.mu.Unlock()
		n.stopHeartbeatTimer()
		n.resetElectionTimerByMode()
		if oldRole != RoleFollower {
			n.onRoleChange(term, oldRole, RoleFollower)
		}
		return
	}

	// TODO: handle success
	n.state.mu.Unlock()
}

func (n *RaftNode) onRoleChange(term Term, oldRole, newRole Role) {
	n.cluster.SendRoleChange(n.id, term, oldRole, newRole)
}
