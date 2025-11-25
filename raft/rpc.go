package raft

type LogEntry struct {
	Term    Term
	Index   LogIndex
	Command []byte
}

type AppendEntriesRequest struct {
	Term     Term
	LeaderID NodeID

	PrevLogIndex LogIndex
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit LogIndex
	IsHeartbeat  bool
}

type AppendEntriesResponse struct {
	Term    Term
	Success bool
}

type ClientPutRequest struct {
	Key   string
	Value []byte
}

type ClientPutResponse struct {
	Term    Term
	Success bool
	Leader  NodeID
}

type RequestVoteRequest struct {
	Term        Term
	CandidateID NodeID

	LastLogIndex LogIndex
	LastLogTerm  Term
}

type RequestVoteResponse struct {
	Term        Term
	VoteGranted bool
}

type RPCType int

const (
	RPCAppendEntries RPCType = iota
	RPCRequestVote
	RPCRequestVoteResponse
	RPCAppendEntriesResponse
)

type RPCMessage struct {
	From NodeID
	To   NodeID
	Type RPCType

	AppendEntriesReq  *AppendEntriesRequest
	RequestVoteReq    *RequestVoteRequest
	AppendEntriesResp *AppendEntriesResponse
	RequestVoteResp   *RequestVoteResponse
}
