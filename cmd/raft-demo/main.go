package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rongrong/raft-kv/raft"
)

func main() {
	logger := log.New(os.Stdout, "[demo] ", log.LstdFlags|log.Lmicroseconds)

	cfg := raft.Config{
		ID:                 "node-1",
		HeartbeatInterval:  100 * time.Millisecond,
		MinElectionTimeout: 500 * time.Millisecond,
		MaxElectionTimeout: 800 * time.Millisecond,
		Logger:             logger,
	}

	node := raft.NewRaftNode(cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := node.Start(ctx); err != nil {
		logger.Fatalf("start node: %v", err)
	}

	logger.Println("raft node started, press Ctrl+C to stop...")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Println("shutting down raft node...")
	node.Stop()
	time.Sleep(200 * time.Millisecond)
}
