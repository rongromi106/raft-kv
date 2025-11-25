package raft

import "sync"

type RaftState struct {
	mu sync.Mutex

	currentTerm Term
	votedFor    *NodeID
	log         []LogEntry

	commitIndex LogIndex
	lastApplied LogIndex

	role      Role
	voteCount int

	nextIndex map[NodeID]LogIndex

	matchIndex map[NodeID]LogIndex
}

func newRaftState() *RaftState {
	return &RaftState{
		log:        make([]LogEntry, 0),
		role:       RoleFollower,
		nextIndex:  make(map[NodeID]LogIndex),
		matchIndex: make(map[NodeID]LogIndex),
	}
}

func (s *RaftState) getTermAndRole() (Term, Role) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentTerm, s.role
}

func (s *RaftState) setRole(role Role) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.role = role
}
