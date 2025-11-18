package raft

import (
	"math/rand"
	"time"
)

func newElectionTimeout(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	return min + time.Duration(rand.Int63n(int64(max-min)))
}
