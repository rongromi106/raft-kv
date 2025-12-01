package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/rongrong/raft-kv/raft"
)

type ClientPutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

var cluster = raft.NewMemoryCluster(nil)

func StartServer(ctx context.Context, addr string) error {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	addr = fmt.Sprintf(":%s", port)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Hello, World!"))
	})
	mux.HandleFunc("/put", putHandler)

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Channel to listen for interrupt / terminate signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	go cluster.Init(context.Background())

	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("server shutdown: %s\n", err)
	}
	cluster.Stop()
	log.Println("server exiting")
	return nil
}

func putHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("Method not allowed"))
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to read body"))
		return
	}

	var putRequest raft.ClientPutRequest
	if err := json.Unmarshal(body, &putRequest); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request body"))
		return
	}

	// send the key / value pairs to the cluster

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}
