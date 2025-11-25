package raft

import (
	"context"
	"testing"
	"time"
)

// stubCluster implements Cluster for tests, capturing responses without network.
type stubCluster struct {
	lastAppendResp     *AppendEntriesResponse
	lastAppendRespTo   NodeID
	lastAppendRespFrom NodeID
}

func (s *stubCluster) SendAppendEntries(from, to NodeID, req *AppendEntriesRequest) {}
func (s *stubCluster) SendRequestVote(from, to NodeID, req *RequestVoteRequest)     {}
func (s *stubCluster) Init(ctx context.Context) error                               { return nil }
func (s *stubCluster) Stop()                                                        {}
func (s *stubCluster) SendRequestVoteResponse(from, to NodeID, resp *RequestVoteResponse) {
}
func (s *stubCluster) SendAppendEntriesResponse(from, to NodeID, resp *AppendEntriesResponse) {
	s.lastAppendRespFrom = from
	s.lastAppendRespTo = to
	s.lastAppendResp = resp
}
func (s *stubCluster) SetCurrentLeader(leader NodeID)                               {}
func (s *stubCluster) KillCurrentLeader() NodeID                                    { return "" }
func (s *stubCluster) SendRoleChange(node NodeID, term Term, oldRole, newRole Role) {}
func (s *stubCluster) ElectionSamples() []time.Duration                             { return nil }
func (s *stubCluster) CreatePartitions(numberOfPartitions int)                      {}

func newTestNode(t *testing.T, id NodeID) (*RaftNode, *stubCluster) {
	t.Helper()
	sc := &stubCluster{}
	n := NewRaftNode(Config{
		ID:                  id,
		HeartbeatInterval:   50 * time.Millisecond,
		MinElectionTimeout:  150 * time.Millisecond,
		MaxElectionTimeout:  300 * time.Millisecond,
		ElectionTimeoutMode: ElectionTimeoutFixed,
	}, sc)
	return n, sc
}

func TestAppendEntries_EmptyFollowerAppend(t *testing.T) {
	follower, sc := newTestNode(t, NodeID("f1"))
	if follower.state.role != RoleFollower {
		t.Fatalf("expected follower role, got %v", follower.state.role)
	}
	if len(follower.state.log) != 0 {
		t.Fatalf("expected empty log, got %d", len(follower.state.log))
	}
	leaderID := NodeID("leader-1")
	leaderTerm := Term(1)
	entry := LogEntry{Term: leaderTerm, Index: 1, Command: []byte("k=v")}
	req := &AppendEntriesRequest{
		Term:         leaderTerm,
		LeaderID:     leaderID,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{entry},
		LeaderCommit: 1,
		IsHeartbeat:  false,
	}
	follower.handleAppendEntries(req)
	if sc.lastAppendResp == nil || !sc.lastAppendResp.Success {
		t.Fatalf("expected success response, got %#v", sc.lastAppendResp)
	}
	if len(follower.state.log) != 1 {
		t.Fatalf("expected log length 1, got %d", len(follower.state.log))
	}
	if follower.state.log[0].Term != leaderTerm || follower.state.log[0].Index != 1 {
		t.Fatalf("unexpected log entry: %#v", follower.state.log[0])
	}
	if follower.state.commitIndex != 1 {
		t.Fatalf("expected commitIndex 1, got %d", follower.state.commitIndex)
	}
}

func TestAppendEntries_MatchingPrefixTruncateAndAppend(t *testing.T) {
	follower, sc := newTestNode(t, NodeID("f2"))
	// Seed follower with 3 entries (indices 1..3)
	follower.state.log = []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a=1")},
		{Term: 1, Index: 2, Command: []byte("b=1")},
		{Term: 2, Index: 3, Command: []byte("c=1")},
	}
	leaderID := NodeID("leader-2")
	leaderTerm := Term(3)
	// Matching prefix at index 2, term 1
	req := &AppendEntriesRequest{
		Term:         leaderTerm,
		LeaderID:     leaderID,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: leaderTerm, Index: 3, Command: []byte("c=2")},
			{Term: leaderTerm, Index: 4, Command: []byte("d=2")},
		},
		LeaderCommit: 4,
		IsHeartbeat:  false,
	}
	follower.handleAppendEntries(req)
	if sc.lastAppendResp == nil || !sc.lastAppendResp.Success {
		t.Fatalf("expected success response, got %#v", sc.lastAppendResp)
	}
	if len(follower.state.log) != 4 {
		t.Fatalf("expected log length 4, got %d", len(follower.state.log))
	}
	// Entry at index 3 should be overwritten
	if follower.state.log[2].Term != leaderTerm || string(follower.state.log[2].Command) != "c=2" {
		t.Fatalf("unexpected entry at idx 3: %#v", follower.state.log[2])
	}
	if follower.state.log[3].Index != 4 {
		t.Fatalf("unexpected index at last entry: %#v", follower.state.log[3])
	}
}

func TestAppendEntries_MismatchRejectNoMutation(t *testing.T) {
	follower, sc := newTestNode(t, NodeID("f3"))
	// Seed follower with 2 entries (indices 1..2)
	orig := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a=1")},
		{Term: 1, Index: 2, Command: []byte("b=1")},
	}
	follower.state.log = append(follower.state.log, orig...)
	leaderID := NodeID("leader-3")
	leaderTerm := Term(2)
	// Mismatch: PrevLogIndex=2 but PrevLogTerm=9 (wrong)
	req := &AppendEntriesRequest{
		Term:         leaderTerm,
		LeaderID:     leaderID,
		PrevLogIndex: 2,
		PrevLogTerm:  9,
		Entries: []LogEntry{
			{Term: leaderTerm, Index: 3, Command: []byte("c=1")},
		},
		LeaderCommit: 3,
		IsHeartbeat:  false,
	}
	follower.handleAppendEntries(req)
	if sc.lastAppendResp == nil || sc.lastAppendResp.Success {
		t.Fatalf("expected reject response, got %#v", sc.lastAppendResp)
	}
	if len(follower.state.log) != len(orig) {
		t.Fatalf("expected log unchanged length %d, got %d", len(orig), len(follower.state.log))
	}
	for i := range orig {
		if follower.state.log[i].Term != orig[i].Term ||
			follower.state.log[i].Index != orig[i].Index ||
			string(follower.state.log[i].Command) != string(orig[i].Command) {
			t.Fatalf("log mutated at %d: before=%#v after=%#v", i, orig[i], follower.state.log[i])
		}
	}
}

func TestAppendEntries_CommitIndexCappedByLastIndex(t *testing.T) {
	follower, _ := newTestNode(t, NodeID("f4"))
	leaderID := NodeID("leader-4")
	leaderTerm := Term(1)
	entries := []LogEntry{
		{Term: leaderTerm, Index: 1, Command: []byte("x=1")},
		{Term: leaderTerm, Index: 2, Command: []byte("y=1")},
	}
	req := &AppendEntriesRequest{
		Term:         leaderTerm,
		LeaderID:     leaderID,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: 10, // larger than last index
		IsHeartbeat:  false,
	}
	follower.handleAppendEntries(req)
	if len(follower.state.log) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(follower.state.log))
	}
	if follower.state.commitIndex != 2 {
		t.Fatalf("expected commitIndex capped at 2, got %d", follower.state.commitIndex)
	}
}
