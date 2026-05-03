package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// nodeConfig holds per-cluster-member info.
type nodeConfig struct {
	ServiceAddr string // HTTP service address
}

var defaultCluster = map[string]nodeConfig{
	"node-a": {ServiceAddr: "localhost:8001"},
	"node-b": {ServiceAddr: "localhost:8002"},
	"node-c": {ServiceAddr: "localhost:8003"},
	"node-d": {ServiceAddr: "localhost:8004"},
}

const lbAddr = "localhost:8080"

func main() {
	cmd := flag.String("cmd", "", "Command: kill|metrics|stress|failover|partition")
	nodeID := flag.String("node", "", "Target node ID (for kill/failover/partition)")
	count := flag.Int("count", 100, "Request count for stress test")
	duration := flag.Duration("duration", 15*time.Second, "Partition duration (for partition command)")
	flag.Parse()

	switch *cmd {
	case "kill":
		if *nodeID == "" {
			fmt.Fprintln(os.Stderr, "kill requires -node")
			os.Exit(1)
		}
		killNode(*nodeID)

	case "metrics":
		collectMetrics()

	case "stress":
		stressTest(*count)

	case "failover":
		if *nodeID == "" {
			fmt.Fprintln(os.Stderr, "failover requires -node")
			os.Exit(1)
		}
		measureFailover(*nodeID)

	case "partition":
		if *nodeID == "" {
			fmt.Fprintln(os.Stderr, "partition requires -node")
			os.Exit(1)
		}
		partitionNode(*nodeID, *duration)

	default:
		fmt.Fprintln(os.Stderr, "Usage:")
		fmt.Fprintln(os.Stderr, "  simulator -cmd=kill      -node=node-c")
		fmt.Fprintln(os.Stderr, "  simulator -cmd=partition -node=node-b [-duration=15s]")
		fmt.Fprintln(os.Stderr, "  simulator -cmd=metrics")
		fmt.Fprintln(os.Stderr, "  simulator -cmd=stress    [-count=1000]")
		fmt.Fprintln(os.Stderr, "  simulator -cmd=failover  -node=node-c")
		flag.Usage()
		os.Exit(1)
	}
}

// killNode kills a node via its /debug/kill endpoint.
func killNode(nodeID string) {
	node, ok := defaultCluster[nodeID]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown node: %s\n", nodeID)
		fmt.Fprintf(os.Stderr, "available nodes: %s\n", nodeList())
		os.Exit(1)
	}

	url := fmt.Sprintf("http://%s/debug/kill", node.ServiceAddr)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kill failed: %v\n", err)
		os.Exit(1)
	}
	resp.Body.Close()
	fmt.Printf("[%s] node killed: %s\n", time.Now().Format("15:04:05.000"), nodeID)
}

// partitionNode isolates a node from gossip (drops UDP packets) for a
// given duration. The node continues to serve HTTP requests but
// becomes invisible to gossip — used to test convergence (K2).
func partitionNode(nodeID string, duration time.Duration) {
	node, ok := defaultCluster[nodeID]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown node: %s\n", nodeID)
		os.Exit(1)
	}

	url := fmt.Sprintf("http://%s/debug/partition?duration=%s", node.ServiceAddr, duration)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "partition failed: %v\n", err)
		os.Exit(1)
	}
	resp.Body.Close()
	fmt.Printf("[%s] node isolated from gossip: %s (duration: %v)\n",
		time.Now().Format("15:04:05.000"), nodeID, duration)
	fmt.Printf("Run `simulator -cmd=metrics` to inspect convergence.\n")
}

// collectMetrics queries every node's membership view and prints them
// side by side. Used to verify K2: state convergence < 10s.
func collectMetrics() {
	type memberInfo struct {
		ID    string `json:"id"`
		State int32  `json:"state"`
		Addr  string `json:"addr"`
	}

	client := &http.Client{Timeout: 2 * time.Second}
	fmt.Println("=== Membership Views ===")

	stateNames := []string{"ALIVE", "SUSPECT", "DEAD"}

	for nodeID, node := range defaultCluster {
		url := fmt.Sprintf("http://%s/members", node.ServiceAddr)
		resp, err := client.Get(url)
		if err != nil {
			fmt.Printf("[%s] UNREACHABLE: %v\n", nodeID, err)
			continue
		}

		var members []memberInfo
		if err := json.NewDecoder(resp.Body).Decode(&members); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] JSON decode error: %v\n", nodeID, err)
		}
		resp.Body.Close()

		fmt.Printf("\n[%s] sees %d members:\n", nodeID, len(members))
		for _, m := range members {
			stateName := "UNKNOWN"
			if int(m.State) < len(stateNames) {
				stateName = stateNames[m.State]
			}
			fmt.Printf("  - %s @ %s => %s\n", m.ID, m.Addr, stateName)
		}
	}
}

// stressTest sends N HTTP requests through the LB and reports the
// distribution. Verifies K6: load distribution std deviation < 15%.
// Note: real standard deviation = sqrt(variance), normalized by the
// number of active backends.
func stressTest(count int) {
	fmt.Printf("=== Stress Test: %d requests ===\n", count)

	client := &http.Client{Timeout: 2 * time.Second}
	url := fmt.Sprintf("http://%s/", lbAddr)

	var (
		success int64
		failed  int64
	)

	// Per-node counter.
	nodeCounts := make(map[string]*int64)
	for id := range defaultCluster {
		var c int64
		nodeCounts[id] = &c
	}

	var mu sync.Mutex
	unknownCounts := make(map[string]int)

	var wg sync.WaitGroup
	sem := make(chan struct{}, 20)

	for i := 0; i < count; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			resp, err := client.Get(url)
			if err != nil {
				atomic.AddInt64(&failed, 1)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				atomic.AddInt64(&failed, 1)
				return
			}

			atomic.AddInt64(&success, 1)

			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return
			}
			if nodeVal, ok := result["node"].(string); ok {
				if counter, found := nodeCounts[nodeVal]; found {
					atomic.AddInt64(counter, 1)
				} else {
					mu.Lock()
					unknownCounts[nodeVal]++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	fmt.Printf("\nResults:\n")
	fmt.Printf("  Successful: %d\n", success)
	fmt.Printf("  Failed:     %d\n", failed)
	fmt.Printf("\nNode distribution:\n")

	// Identify active nodes (received at least one request).
	activeNodes := 0
	total := int64(0)
	for id, counter := range nodeCounts {
		c := atomic.LoadInt64(counter)
		total += c
		fmt.Printf("  %s: %d requests\n", id, c)
		if c > 0 {
			activeNodes++
		}
	}
	for id, c := range unknownCounts {
		fmt.Printf("  %s (unknown): %d requests\n", id, c)
	}

	if total > 0 && activeNodes > 0 {
		// Expected distribution normalized by active node count.
		expected := float64(total) / float64(activeNodes)
		fmt.Printf("\nActive nodes:   %d\n", activeNodes)
		fmt.Printf("Expected/node:  ~%.0f\n", expected)

		// Real std deviation = sqrt(variance).
		sumSq := 0.0
		for _, counter := range nodeCounts {
			c := float64(atomic.LoadInt64(counter))
			if c == 0 {
				continue // skip inactive nodes
			}
			diff := c - expected
			sumSq += diff * diff
		}
		variance := sumSq / float64(activeNodes)
		stdDev := math.Sqrt(variance)

		fmt.Printf("Std deviation:  %.2f\n", stdDev)
		cv := (stdDev / expected) * 100 // Coefficient of Variation (%)
		fmt.Printf("CV:             %.1f%% (K6 target: <15%%)\n", cv)
		if cv < 15 {
			fmt.Println("K6 PASS")
		} else {
			fmt.Printf("K6 FAIL (%.1f%% > 15%%)\n", cv)
		}
	}
}

// measureFailover times how long it takes for the LB to stop routing
// to a killed node. Verifies K1: failover < 5s.
//
// Notes:
//   - The timer starts AFTER killNode returns (the kill RTT is excluded).
//   - One 200 OK isn't enough: we wait for 5 consecutive successful
//     responses that did NOT come from the killed node.
func measureFailover(nodeID string) {
	fmt.Printf("=== Failover Test: %s ===\n", nodeID)

	client := &http.Client{Timeout: 1 * time.Second}
	lbURL := fmt.Sprintf("http://%s/", lbAddr)

	// Verify the LB is reachable.
	resp, err := client.Get(lbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "LB unreachable: %v\n", err)
		os.Exit(1)
	}
	resp.Body.Close()
	fmt.Println("LB reachable")

	// Kill the node — the timer starts AFTER the kill returns.
	fmt.Printf("[%s] killing %s...\n", time.Now().Format("15:04:05.000"), nodeID)
	start := time.Now()
	killNode(nodeID)
	fmt.Println("Failover measurement started (waiting for gossip convergence)...")

	const (
		maxAttempts    = 300 // max 15s (300 × 50ms)
		pollInterval   = 50 * time.Millisecond
		requiredConsec = 5 // consecutive successes required
	)

	consecutive := 0
	for i := 0; i < maxAttempts; i++ {
		time.Sleep(pollInterval)

		resp, err := client.Get(lbURL)
		if err != nil {
			consecutive = 0
			continue
		}

		// Which node served the response?
		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			fmt.Fprintf(os.Stderr, "failover JSON decode error: %v\n", err)
		}
		resp.Body.Close()

		respNode, _ := result["node"].(string)

		if resp.StatusCode == http.StatusOK && respNode != nodeID {
			// Successful response that did NOT come from the killed node.
			consecutive++
			if consecutive >= requiredConsec {
				elapsed := time.Since(start)
				fmt.Printf("\n[%s] Failover complete (%d consecutive successes)\n",
					time.Now().Format("15:04:05.000"), requiredConsec)
				fmt.Printf("Failover time: %v\n", elapsed.Round(time.Millisecond))

				if elapsed < 5*time.Second {
					fmt.Println("K1 PASS (< 5s)")
				} else {
					fmt.Printf("K1 FAIL (%.1fs > 5s)\n", elapsed.Seconds())
				}
				return
			}
		} else {
			// Either traffic still hits the killed node, or there was an error.
			if respNode == nodeID {
				fmt.Printf("  [!] traffic still hitting %s (gossip hasn't converged yet)\n", nodeID)
			}
			consecutive = 0
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("Failover did not complete within %v (FAIL)\n", elapsed.Round(time.Millisecond))
}

// nodeList returns a comma-separated list of cluster node names.
func nodeList() string {
	names := make([]string, 0, len(defaultCluster))
	for id := range defaultCluster {
		names = append(names, id)
	}
	return strings.Join(names, ", ")
}
