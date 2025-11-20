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

	memCluster := raft.NewMemoryCluster(3)
	if err := memCluster.Init(ctx); err != nil {
		logger.Fatalf("Error initializing cluster: %v", err)
	}

	logger.Println("raft cluster started, running for 10 seconds...")
	time.Sleep(10 * time.Second)

	logger.Println("shutting down raft cluster...")
	memCluster.Stop()
	time.Sleep(200 * time.Millisecond)
	logger.Println("election tracker samples:", memCluster.ElectionSamples())
}
