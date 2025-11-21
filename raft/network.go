package raft

import (
	"math/rand"
	"time"
)

type Network interface {
	SendAppendEntries(from, to NodeID, req *AppendEntriesRequest)
	SendRequestVote(from, to NodeID, req *RequestVoteRequest)
	SendRequestVoteResponse(from, to NodeID, resp *RequestVoteResponse)
	SendAppendEntriesResponse(from, to NodeID, resp *AppendEntriesResponse)
}

type NetworkSimulator struct {
	minDelayMs time.Duration
	maxDelayMs time.Duration
	dropRate   float64
	deliver    func(to NodeID, msg RPCMessage)
}

func NewNetworkSimulator(minDelayMs, maxDelayMs time.Duration, dropRate float64, deliver func(to NodeID, msg RPCMessage)) *NetworkSimulator {
	if maxDelayMs < minDelayMs {
		maxDelayMs = minDelayMs
	}
	// Seed the PRNG for randomized delay/drop simulation.
	rand.Seed(time.Now().UnixNano())
	return &NetworkSimulator{
		minDelayMs: minDelayMs,
		maxDelayMs: maxDelayMs,
		dropRate:   dropRate,
		deliver:    deliver,
	}
}

func (n *NetworkSimulator) SendAppendEntries(from, to NodeID, req *AppendEntriesRequest) {
	msg := RPCMessage{
		From:             from,
		To:               to,
		Type:             RPCAppendEntries,
		AppendEntriesReq: req,
	}
	go func() {
		// Simulate drop
		if n.dropRate > 0 && rand.Float64() < n.dropRate {
			return
		}
		// Simulate delay in [minDelayMs, maxDelayMs]
		delay := n.minDelayMs
		if n.maxDelayMs > n.minDelayMs {
			jitter := n.maxDelayMs - n.minDelayMs
			delay = n.minDelayMs + time.Duration(rand.Int63n(int64(jitter)+1))
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		if n.deliver != nil {
			n.deliver(to, msg)
		}
	}()
}

func (n *NetworkSimulator) SendRequestVote(from, to NodeID, req *RequestVoteRequest) {
	msg := RPCMessage{
		From:           from,
		To:             to,
		Type:           RPCRequestVote,
		RequestVoteReq: req,
	}
	go func() {
		if n.dropRate > 0 && rand.Float64() < n.dropRate {
			return
		}
		delay := n.minDelayMs
		if n.maxDelayMs > n.minDelayMs {
			jitter := n.maxDelayMs - n.minDelayMs
			delay = n.minDelayMs + time.Duration(rand.Int63n(int64(jitter)+1))
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		if n.deliver != nil {
			n.deliver(to, msg)
		}
	}()
}

func (n *NetworkSimulator) SendRequestVoteResponse(from, to NodeID, resp *RequestVoteResponse) {
	msg := RPCMessage{
		From:            from,
		To:              to,
		Type:            RPCRequestVoteResponse,
		RequestVoteResp: resp,
	}
	go func() {
		if n.dropRate > 0 && rand.Float64() < n.dropRate {
			return
		}
		delay := n.minDelayMs
		if n.maxDelayMs > n.minDelayMs {
			jitter := n.maxDelayMs - n.minDelayMs
			delay = n.minDelayMs + time.Duration(rand.Int63n(int64(jitter)+1))
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		if n.deliver != nil {
			n.deliver(to, msg)
		}
	}()
}

func (n *NetworkSimulator) SendAppendEntriesResponse(from, to NodeID, resp *AppendEntriesResponse) {
	msg := RPCMessage{
		From:              from,
		To:                to,
		Type:              RPCAppendEntriesResponse,
		AppendEntriesResp: resp,
	}
	go func() {
		if n.dropRate > 0 && rand.Float64() < n.dropRate {
			return
		}
		delay := n.minDelayMs
		if n.maxDelayMs > n.minDelayMs {
			jitter := n.maxDelayMs - n.minDelayMs
			delay = n.minDelayMs + time.Duration(rand.Int63n(int64(jitter)+1))
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		if n.deliver != nil {
			n.deliver(to, msg)
		}
	}()
}
