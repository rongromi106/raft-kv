package main

import "context"

func main() {
	// Perfect scenario: 3 nodes, no network delay or partitions
	// RunPerfectScenario()
	// Run network delay scenario -- each scenario is run for 10 seconds
	// RunNetworkDelayScenario(3, 100*time.Millisecond, 250*time.Millisecond, 0)
	// RunPartitionScenario(3, 2)

	// HTTP Part, start a server on port 8080
	StartServer(context.Background(), "localhost:8080")
}
