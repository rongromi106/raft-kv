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

	kvstore *KVStore
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
		kvstore:   NewKVStore(),
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
	myLastLogIndex := LogIndex(0)
	myLastLogTerm := Term(0)
	if len(n.state.log) > 0 {
		myLastLogIndex = n.state.log[len(n.state.log)-1].Index
		myLastLogTerm = n.state.log[len(n.state.log)-1].Term
	}
	for _, peerId := range n.peerIds {
		n.cluster.SendRequestVote(n.id, peerId, &RequestVoteRequest{
			Term:         currentTerm,
			CandidateID:  n.id,
			LastLogIndex: myLastLogIndex,
			LastLogTerm:  myLastLogTerm,
		})
	}
	n.resetElectionTimerByMode()
}

func (n *RaftNode) handleRequestVote(req *RequestVoteRequest) {
	n.state.mu.Lock()
	// Decide vote under lock; perform side effects after unlocking
	currentTerm := n.state.currentTerm
	oldRole := n.state.role
	stepDown := false
	grant := false

	if req.Term < currentTerm {
		// lower term -> reject
		term := n.state.currentTerm
		n.state.mu.Unlock()
		n.cluster.SendRequestVoteResponse(n.id, req.CandidateID, &RequestVoteResponse{
			Term:        term,
			VoteGranted: false,
		})
		return
	}

	if req.Term > currentTerm {
		// observe higher term -> step down to follower
		n.state.currentTerm = req.Term
		n.state.role = RoleFollower
		n.state.votedFor = nil
		n.state.voteCount = 0
		stepDown = true
	}

	// Term is now equal to req.Term
	if n.state.votedFor == nil || (n.state.votedFor != nil && *n.state.votedFor == req.CandidateID) {
		// log freshness check: only grant a vote to a candidate whose log is at least as up-to-date as their own.
		myLastLogIndex := LogIndex(0)
		myLastLogTerm := Term(0)
		if len(n.state.log) > 0 {
			myLastLogIndex = n.state.log[len(n.state.log)-1].Index
			myLastLogTerm = n.state.log[len(n.state.log)-1].Term
		}
		// candidate is up-to-date if higher term, or same term with >= index
		upToDate := (req.LastLogTerm > myLastLogTerm) ||
			(req.LastLogTerm == myLastLogTerm && req.LastLogIndex >= myLastLogIndex)
		if n.state.votedFor == nil || (n.state.votedFor != nil && *n.state.votedFor == req.CandidateID) {
			if !upToDate {
				term := n.state.currentTerm
				n.state.mu.Unlock()
				n.cluster.SendRequestVoteResponse(n.id, req.CandidateID, &RequestVoteResponse{
					Term:        term,
					VoteGranted: false,
				})
				return
			}
			n.state.votedFor = &req.CandidateID
			grant = true
		}
	}
	term := n.state.currentTerm
	candidateID := req.CandidateID
	n.state.mu.Unlock()

	// Perform side effects
	if stepDown && oldRole != RoleFollower {
		n.stopHeartbeatTimer()
		n.onRoleChange(term, oldRole, RoleFollower)
	}

	n.cluster.SendRequestVoteResponse(n.id, candidateID, &RequestVoteResponse{
		Term:        term,
		VoteGranted: grant,
	})
	if grant {
		n.resetElectionTimerByMode()
	}
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
			n.OnBecomeLeader()
			n.startHeartbeat()
			return
		}
	}
	n.state.mu.Unlock()
}

// Confirmed leader role calling this function
func (n *RaftNode) startHeartbeat() {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	for _, peerId := range n.peerIds {
		if peerId == n.id {
			continue
		}
		var prevLogTerm Term
		// the index that the follower was supposed to sync -ed to
		// last log item for leader, this is different than the last applied or commit index for
		// both leader and follower.
		prevLogIndex := n.state.nextIndex[peerId] - 1
		if prevLogIndex == 0 {
			prevLogTerm = 0
		} else {
			// translate logical index to slice offset; example if dense 1-based:
			offset := int(prevLogIndex) - 1
			if offset >= 0 && offset < len(n.state.log) {
				prevLogTerm = n.state.log[offset].Term
			} else {
				// handle inconsistency: skip or fallback; better add a termAt helper
				prevLogTerm = 0
			}
		}

		n.cluster.SendAppendEntries(n.id, peerId, &AppendEntriesRequest{
			Term:         n.state.currentTerm,
			LeaderID:     n.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      nil, // empty entries for heartbeat
			LeaderCommit: n.state.commitIndex,
			IsHeartbeat:  true,
		})

	}
	n.resetHeartbeatTimer(n.cfg.HeartbeatInterval)

}

func (n *RaftNode) handleAppendEntries(req *AppendEntriesRequest) {
	n.state.mu.Lock()
	// step 1: term check with deferred side effects
	oldRole := n.state.role
	stepDown := false
	ackIndex := LogIndex(0)
	var toApplyFollower []LogEntry
	if req.Term > n.state.currentTerm {
		n.state.currentTerm = req.Term
		n.state.votedFor = nil
		n.state.role = RoleFollower
		n.state.voteCount = 0
		stepDown = (oldRole != RoleFollower)
	} else if req.Term < n.state.currentTerm {
		term := n.state.currentTerm
		n.state.mu.Unlock()
		n.cluster.SendAppendEntriesResponse(n.id, req.LeaderID, &AppendEntriesResponse{
			From:    n.id,
			Term:    term,
			Success: false,
		})
		return
	}
	// step 2: log check and state update under lock, then send response after unlock
	success := false
	if n.state.role == RoleFollower {
		// Calculate follower's last log index (assuming log indices are 1-based)
		lastLogIdx := LogIndex(len(n.state.log))

		// Reject if we don't have the entry at PrevLogIndex (except when PrevLogIndex == 0)
		if req.PrevLogIndex > 0 && req.PrevLogIndex > lastLogIdx {
			success = false
			ackIndex = lastLogIdx
		} else {
			// If PrevLogIndex > 0, verify term matches at that index
			if req.PrevLogIndex > 0 {
				pos := int(req.PrevLogIndex) - 1
				if pos < 0 || pos >= len(n.state.log) || n.state.log[pos].Term != req.PrevLogTerm {
					success = false
					ackIndex = req.PrevLogIndex - 1
				} else {
					// Truncate and append
					truncateLen := int(req.PrevLogIndex)
					if truncateLen < 0 {
						truncateLen = 0
					}
					if truncateLen > len(n.state.log) {
						truncateLen = len(n.state.log)
					}
					n.state.log = n.state.log[:truncateLen]
					if len(req.Entries) > 0 {
						n.state.log = append(n.state.log, req.Entries...)
					}
					if len(n.state.log) > 0 {
						n.state.commitIndex = min(req.LeaderCommit, n.state.log[len(n.state.log)-1].Index)
					} else {
						n.state.commitIndex = 0
					}
					// ack up to prev + appended
					ackIndex = req.PrevLogIndex + LogIndex(len(req.Entries))
					success = true
				}
			} else {
				// PrevLogIndex == 0, append at beginning
				n.state.log = n.state.log[:0]
				if len(req.Entries) > 0 {
					n.state.log = append(n.state.log, req.Entries...)
				}
				if len(n.state.log) > 0 {
					n.state.commitIndex = min(req.LeaderCommit, n.state.log[len(n.state.log)-1].Index)
				} else {
					n.state.commitIndex = 0
				}
				// ack up to appended length
				ackIndex = LogIndex(len(req.Entries))
				success = true
			}
		}

		// follower prepares entries to apply: [lastApplied+1, commitIndex]
		start := n.state.lastApplied + 1
		end := n.state.commitIndex
		if end >= start && end > 0 && len(n.state.log) > 0 {
			lo := int(start) - 1
			if lo < 0 {
				lo = 0
			}
			hi := int(end)
			if hi > len(n.state.log) {
				hi = len(n.state.log)
			}
			if lo < hi {
				toApplyFollower = append([]LogEntry(nil), n.state.log[lo:hi]...)
				n.state.lastApplied = end
			}
		}
	}
	term := n.state.currentTerm
	n.state.mu.Unlock()
	// follower applies outside lock
	if len(toApplyFollower) > 0 {
		go func(entries []LogEntry) {
			for _, entry := range entries {
				if err := n.kvstore.Apply(entry.Command); err != nil {
					n.logger.Printf("[node %s] failed to apply command: %v", n.id, err)
				}
			}
		}(toApplyFollower)
	}
	// perform side effects after unlock
	if stepDown && oldRole != RoleFollower {
		n.stopHeartbeatTimer()
		n.onRoleChange(term, oldRole, RoleFollower)
	}
	// send response
	n.cluster.SendAppendEntriesResponse(n.id, req.LeaderID, &AppendEntriesResponse{
		From:     n.id,
		Term:     term,
		Success:  success,
		AckIndex: ackIndex,
	})
}

func (n *RaftNode) handleAppendEntriesResponse(resp *AppendEntriesResponse) {
	var toApplyLeader []LogEntry
	n.state.mu.Lock()

	// Ignore stale term responses
	if resp.Term < n.state.currentTerm {
		n.state.mu.Unlock()
		return
	}

	if !resp.Success {
		// step down to follower
		if resp.Term > n.state.currentTerm {
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
		} else if resp.Term == n.state.currentTerm {
			/*
				If resp.Term == currentTerm and !resp.Success: treat as log inconsistency;
				do NOT step down. Decrement or jump nextIndex[peer] and retry AppendEntries.
			*/
			if n.state.nextIndex[resp.From] > 1 {
				n.state.nextIndex[resp.From] = n.state.nextIndex[resp.From] - 1
			} else {
				n.state.nextIndex[resp.From] = 1
			}

		}

	} else {
		n.state.matchIndex[resp.From] = resp.AckIndex
		n.state.nextIndex[resp.From] = resp.AckIndex + 1

		// update commit index based on majority match in current term
		if len(n.state.log) > 0 {
			currentTerm := n.state.currentTerm
			lastIndex := n.state.log[len(n.state.log)-1].Index
			newCommit := n.state.commitIndex
			totalNodes := len(n.peerIds) + 1
			required := totalNodes/2 + 1
			for i := n.state.commitIndex + 1; i <= lastIndex; i++ {
				// only commit entries from current term
				pos := int(i) - 1
				if pos < 0 || pos >= len(n.state.log) || n.state.log[pos].Term != currentTerm {
					continue
				}
				peerMatches := 0
				for _, peerId := range n.peerIds {
					if n.state.matchIndex[peerId] >= i {
						peerMatches++
					}
				}
				matched := 1 + peerMatches // include leader
				if matched >= required {
					newCommit = i
				}
			}
			// prepare leader apply slice from lastApplied+1 to newCommit
			if newCommit > n.state.lastApplied {
				start := n.state.lastApplied + 1
				end := newCommit
				lo := int(start) - 1
				if lo < 0 {
					lo = 0
				}
				hi := int(end)
				if hi > len(n.state.log) {
					hi = len(n.state.log)
				}
				if lo < hi {
					toApplyLeader = append([]LogEntry(nil), n.state.log[lo:hi]...)
					n.state.lastApplied = end
				}
			}
			n.state.commitIndex = newCommit
		}
	}

	n.state.mu.Unlock()
	// leader applies outside lock to prevent go routine to access shared state (racy)
	if len(toApplyLeader) > 0 {
		go func(entries []LogEntry) {
			for _, entry := range entries {
				if err := n.kvstore.Apply(entry.Command); err != nil {
					n.logger.Printf("[node %s] failed to apply command: %v", n.id, err)
				}
			}
		}(toApplyLeader)
	}
}

func (n *RaftNode) onRoleChange(term Term, oldRole, newRole Role) {
	n.cluster.SendRoleChange(n.id, term, oldRole, newRole)
}

// update nextIndex and matchIndex for all followers
func (n *RaftNode) OnBecomeLeader() {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	log.Printf("[node %s] became leader on term %d", n.id, n.state.currentTerm)
	// Ensure follower tracking maps are initialized
	if n.state.nextIndex == nil {
		n.state.nextIndex = make(map[NodeID]LogIndex)
	}
	if n.state.matchIndex == nil {
		n.state.matchIndex = make(map[NodeID]LogIndex)
	}
	// Handle empty log safely
	var lastLogIndex LogIndex
	if len(n.state.log) == 0 {
		lastLogIndex = 0
	} else {
		lastLogIndex = n.state.log[len(n.state.log)-1].Index
	}
	for _, peerId := range n.peerIds {
		if peerId == n.id {
			continue
		}
		n.state.nextIndex[peerId] = lastLogIndex + 1
		n.state.matchIndex[peerId] = 0
	}
}
