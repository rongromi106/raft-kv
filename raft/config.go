package raft

import (
	"log"
	"time"
)

type ElectionTimeoutMode int

const (
	ElectionTimeoutRandom ElectionTimeoutMode = iota
	ElectionTimeoutFixed
)

type Config struct {
	ID NodeID

	HeartbeatInterval  time.Duration
	MinElectionTimeout time.Duration
	MaxElectionTimeout time.Duration

	// ElectionTimeoutMode selects whether to use randomized or fixed election timeouts.
	// Default is ElectionTimeoutRandom.
	ElectionTimeoutMode ElectionTimeoutMode

	Logger *log.Logger
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.HeartbeatInterval == 0 {
		out.HeartbeatInterval = 100 * time.Millisecond
	}
	if out.MinElectionTimeout == 0 {
		out.MinElectionTimeout = 300 * time.Millisecond
	}
	if out.MaxElectionTimeout == 0 {
		out.MaxElectionTimeout = 600 * time.Millisecond
	}
	if out.Logger == nil {
		out.Logger = log.Default()
	}
	return out
}
