package main

func main() {
	// Perfect scenario: 3 nodes, no network delay or partitions
	// RunPerfectScenario()
	// Run network delay scenario -- each scenario is run for 10 seconds
	// RunNetworkDelayScenario(3, 100*time.Millisecond, 250*time.Millisecond, 0)
	RunPartitionScenario(3, 2)
}
