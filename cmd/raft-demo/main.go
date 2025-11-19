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

	// cfg := raft.Config{
	// 	ID:                 "node-1",
	// 	HeartbeatInterval:  100 * time.Millisecond,
	// 	MinElectionTimeout: 500 * time.Millisecond,
	// 	MaxElectionTimeout: 800 * time.Millisecond,
	// 	Logger:             logger,
	// }

	// node := raft.NewRaftNode(cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	memCluster := raft.NewMemoryCluster(3)
	if err := memCluster.Init(ctx); err != nil {
		logger.Fatalf("Error initializing cluster: %v", err)
	}

	logger.Println("raft cluster started, press Ctrl+C to stop...")

	go func() {
		time.Sleep(3 * time.Second)
		logger.Println("cluster --> killing current leader")
		prevLeader := memCluster.KillCurrentLeader()
		logger.Printf("cluster --> previous leader %s killed", prevLeader)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Println("shutting down raft cluster...")
	time.Sleep(200 * time.Millisecond)

}
