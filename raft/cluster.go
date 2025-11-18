package raft

type Cluster interface {
	SendAppendEntries(from, to NodeID, req *AppendEntriesRequest)
	SendRequestVote(from, to NodeID, req *RequestVoteRequest)
}
