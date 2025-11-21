package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/rongrong/raft-kv/raft"
)

func RunNetworkDelayScenario(clusterSize int, minDelayMs, maxDelayMs time.Duration, dropRate float64) {
	logger := log.New(os.Stdout, "[simulation] ", log.LstdFlags|log.Lmicroseconds)
	cfg := &raft.ClusterConfig{
		ClusterSize: clusterSize,
		NetworkConfig: raft.NetworkConfig{
			MinDelayMs: minDelayMs,
			MaxDelayMs: maxDelayMs,
			DropRate:   dropRate,
		},
	}
	cluster := raft.NewMemoryCluster(cfg)
	cluster.Init(context.Background())
	time.Sleep(10 * time.Second)
	cluster.Stop()
	time.Sleep(200 * time.Millisecond)
	logger.Println("election tracker samples:", cluster.ElectionSamples())
}
