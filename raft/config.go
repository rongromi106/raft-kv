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

// RaftNodeConfig
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

type NetworkConfig struct {
	MinDelayMs time.Duration
	MaxDelayMs time.Duration
	DropRate   float64
}

type ClusterConfig struct {
	ClusterSize        int
	NetworkConfig      NetworkConfig
	NumberOfPartitions int
}

func (c *ClusterConfig) withThreeNodesPerfectNetwork() ClusterConfig {
	out := *c
	out.ClusterSize = 3
	out.NetworkConfig.MinDelayMs = 0
	out.NetworkConfig.MaxDelayMs = 0
	out.NetworkConfig.DropRate = 0
	return out
}
