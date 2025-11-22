package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/rongrong/raft-kv/raft"
)

func RunPerfectScenario() {
	logger := log.New(os.Stdout, "[simulation] ", log.LstdFlags|log.Lmicroseconds)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Perfect network defaults: 3 nodes, no delay, no drops, single partition
	cluster := raft.NewMemoryCluster(nil)
	if err := cluster.Init(ctx); err != nil {
		logger.Fatalf("Error initializing cluster: %v", err)
	}

	logger.Println("raft cluster started, running for 20 seconds...")
	time.Sleep(20 * time.Second)

	logger.Println("shutting down raft cluster...")
	cluster.Stop()
	time.Sleep(200 * time.Millisecond)
	logger.Println("election tracker samples:", cluster.ElectionSamples())
}

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

func RunPartitionScenario(clusterSize int, numberOfPartitions int) {
	logger := log.New(os.Stdout, "[simulation] ", log.LstdFlags|log.Lmicroseconds)
	cfg := &raft.ClusterConfig{
		ClusterSize:        clusterSize,
		NumberOfPartitions: numberOfPartitions,
	}
	cluster := raft.NewMemoryCluster(cfg)
	cluster.Init(context.Background())
	time.Sleep(5 * time.Second)
	logger.Println("Partition starts after five seconds")
	cluster.CreatePartitions(numberOfPartitions)
	time.Sleep(10 * time.Second)
	cluster.Stop()
	time.Sleep(200 * time.Millisecond)
	logger.Println("election tracker samples:", cluster.ElectionSamples())
}
