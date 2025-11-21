package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/rongrong/raft-kv/raft"
)

func main() {
	logger := log.New(os.Stdout, "[demo] ", log.LstdFlags|log.Lmicroseconds)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// demo cluster with 3 nodes and perfect network
	memCluster := raft.NewMemoryCluster(nil)
	if err := memCluster.Init(ctx); err != nil {
		logger.Fatalf("Error initializing cluster: %v", err)
	}

	logger.Println("raft cluster started, running for 10 seconds...")
	time.Sleep(20 * time.Second)

	logger.Println("shutting down raft cluster...")
	memCluster.Stop()
	time.Sleep(200 * time.Millisecond)
	logger.Println("election tracker samples:", memCluster.ElectionSamples())

	// Run network delay scenario -- each scenario is run for 10 seconds
	RunNetworkDelayScenario(3, 100*time.Millisecond, 250*time.Millisecond, 0)
}
