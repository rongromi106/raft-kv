package raft

import (
	"sync"
	"time"
)

// ElectionTracker 负责基于 RoleChange 事件，统计选主耗时
type ElectionTracker struct {
	mu sync.Mutex

	nodesRoles      map[NodeID]Role
	inElection      bool
	electionTerm    Term
	electionStart   time.Time
	electionSamples []time.Duration
}

func NewElectionTracker(nodes map[NodeID]*RaftNode) *ElectionTracker {
	tracker := &ElectionTracker{
		nodesRoles: make(map[NodeID]Role),
	}
	for _, node := range nodes {
		tracker.nodesRoles[node.id] = node.state.role
	}
	return tracker
}

func (t *ElectionTracker) OnRoleChange(nodeID NodeID, term Term, oldRole, newRole Role) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 记录每个节点当前 role
	t.nodesRoles[nodeID] = newRole

	hasLeaderBefore := t.hasLeaderExcept(nodeID) || oldRole == RoleLeader
	hasLeaderAfter := t.hasLeader()

	// election starts
	switch {
	case hasLeaderBefore && !hasLeaderAfter:
		t.inElection = true
		t.electionTerm = term + 1
		t.electionStart = time.Now()
	// election ends
	case !hasLeaderBefore && hasLeaderAfter:
		if t.inElection {
			dur := time.Since(t.electionStart)
			t.electionSamples = append(t.electionSamples, dur)
			t.inElection = false
		}
	default:
		return
	}
}

func (t *ElectionTracker) hasLeaderExcept(nodeID NodeID) bool {
	for id, r := range t.nodesRoles {
		if id == nodeID {
			continue
		}
		if r == RoleLeader {
			return true
		}
	}
	return false
}

func (t *ElectionTracker) hasLeader() bool {
	for _, r := range t.nodesRoles {
		if r == RoleLeader {
			return true
		}
	}
	return false
}

// get samples for analysis of election duration
func (t *ElectionTracker) Samples() []time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]time.Duration, len(t.electionSamples))
	copy(out, t.electionSamples)
	return out
}
