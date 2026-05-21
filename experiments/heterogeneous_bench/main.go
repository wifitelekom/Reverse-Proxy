// Experiment 4: Comparative selector benchmark on a heterogeneous backend cluster.
//
// Where the stress experiment (experiments/stress) used identical
// backends to measure fairness, this one uses *heterogeneous* backends
// to measure how the four shipped selectors react to a deliberately
// non-uniform cluster:
//
//	node-fast    — 1 ms artificial work
//	node-medium  — 5 ms artificial work
//	node-slow    — 20 ms artificial work
//
// 2 000 requests are driven at concurrency 32 against each selector
// in turn. For each run we record:
//   - per-backend hit count
//   - per-backend mean wall-clock response time (client-side)
//   - the overall p50 / p99 / mean
//
// The expected outcome:
//   - RoundRobin distributes evenly (≈ 33 % each), so the slow backend
//     drags the aggregate p99 up.
//   - LeastConnections also distributes ~ evenly under low contention
//     (with concurrency 32 and 3 backends, in-flight count is small).
//   - P2C distributes ~ evenly but with the smoothing effect of the
//     two-sample comparison; little advantage over LeastConn here.
//   - P2C-EWMA correctly biases toward the fast backend once the EWMA
//     converges; the aggregate p99 should be lower than the other three.
//
// Output: JSON on stdout; human-readable summary on stderr.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/loadbalancer"
	"p2p-loadbalancer/pkg/metrics"
	"p2p-loadbalancer/pkg/types"
)

const (
	requestsPerSelector = 2000
	concurrency         = 32
)

// heteroBackend wraps an httptest server with a fixed artificial delay
// and per-instance counters.
type heteroBackend struct {
	id      string
	delay   time.Duration
	srv     *httptest.Server
	hits    atomic.Int64
	totalNs atomic.Int64
}

func newHetero(id string, delay time.Duration) *heteroBackend {
	b := &heteroBackend{id: id, delay: delay}
	b.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.hits.Add(1)
		time.Sleep(delay)
		fmt.Fprintf(w, `{"node":"%s","delay_ms":%d}`, b.id, delay.Milliseconds())
	}))
	return b
}

type runResult struct {
	Selector   string            `json:"selector"`
	N          int               `json:"n"`
	WallMs     float64           `json:"wall_ms"`
	P50Ms      float64           `json:"p50_ms"`
	P90Ms      float64           `json:"p90_ms"`
	P99Ms      float64           `json:"p99_ms"`
	MeanMs     float64           `json:"mean_ms"`
	PerBackend map[string]bShare `json:"per_backend"`
}

type bShare struct {
	Hits           int64   `json:"hits"`
	SharePct       float64 `json:"share_pct"`
	BackendDelayMs int64   `json:"backend_delay_ms"`
}

func main() {
	results := []runResult{}
	for _, strat := range []string{"roundrobin", "leastconn", "p2c", "p2c-ewma"} {
		fmt.Fprintf(os.Stderr, "==> Selector: %s\n", strat)
		r := runOnce(strat)
		r.Selector = strat
		results = append(results, r)
	}

	// Identify the best (lowest p99) selector for the heterogeneous cluster.
	best := results[0]
	for _, r := range results {
		if r.P99Ms < best.P99Ms {
			best = r
		}
	}

	out := map[string]any{
		"experiment":       "heterogeneous_bench",
		"requests_per_run": requestsPerSelector,
		"concurrency":      concurrency,
		"runs":             results,
		"best_by_p99":      best.Selector,
		"best_p99_ms":      best.P99Ms,
	}
	json.NewEncoder(os.Stdout).Encode(out)

	// Human-readable summary
	fmt.Fprintln(os.Stderr, "---")
	fmt.Fprintf(os.Stderr, "%-12s  %-10s  %-10s  %-10s  %-10s   distribution\n",
		"selector", "p50 (ms)", "p90 (ms)", "p99 (ms)", "mean (ms)")
	for _, r := range results {
		dist := ""
		for _, id := range []string{"node-fast", "node-medium", "node-slow"} {
			b := r.PerBackend[id]
			dist += fmt.Sprintf("%s=%4.1f%% ", id, b.SharePct)
		}
		fmt.Fprintf(os.Stderr, "%-12s  %-10.2f  %-10.2f  %-10.2f  %-10.2f   %s\n",
			r.Selector, r.P50Ms, r.P90Ms, r.P99Ms, r.MeanMs, dist)
	}
	fmt.Fprintln(os.Stderr, "---")
	fmt.Fprintf(os.Stderr, "Best selector by p99: %s (%.2f ms)\n", best.Selector, best.P99Ms)
}

func runOnce(strategy string) runResult {
	backends := []*heteroBackend{
		newHetero("node-fast", 1*time.Millisecond),
		newHetero("node-medium", 5*time.Millisecond),
		newHetero("node-slow", 20*time.Millisecond),
	}
	defer func() {
		for _, b := range backends {
			b.srv.Close()
		}
	}()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ml := gossip.NewMembershipList("lb", logger)
	for _, b := range backends {
		addr := b.srv.URL[len("http://"):]
		ml.AddOrUpdate(gossip.NodeInfo{
			ID:          b.id,
			Addr:        addr,
			ServiceAddr: addr,
			State:       gossip.StateAlive,
			Incarnation: 1,
		})
	}

	var sel loadbalancer.Selector
	switch strategy {
	case "roundrobin":
		sel = loadbalancer.NewRoundRobin()
	case "leastconn":
		sel = loadbalancer.NewLeastConnections()
	case "p2c":
		sel = loadbalancer.NewP2C()
	case "p2c-ewma":
		sel = loadbalancer.NewP2CEWMA()
	}

	cfg := types.LBConfig{ListenAddr: ":0", Strategy: strategy}
	lb, err := loadbalancer.NewLoadBalancer(cfg, ml, sel, logger, nil, metrics.New())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	srv := httptest.NewServer(lb.Handler())
	defer srv.Close()

	client := &http.Client{Timeout: 10 * time.Second}
	durations := make([]time.Duration, 0, requestsPerSelector)
	var mu sync.Mutex
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	wg.Add(requestsPerSelector)
	t0 := time.Now()
	for i := 0; i < requestsPerSelector; i++ {
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			start := time.Now()
			resp, err := client.Get(srv.URL + "/")
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				d := time.Since(start)
				mu.Lock()
				durations = append(durations, d)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	wall := time.Since(t0)

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	mean := time.Duration(int64(sum) / int64(max(len(durations), 1)))

	r := runResult{
		N:          len(durations),
		WallMs:     ms(wall),
		P50Ms:      ms(durations[pctIdx(len(durations), 0.50)]),
		P90Ms:      ms(durations[pctIdx(len(durations), 0.90)]),
		P99Ms:      ms(durations[pctIdx(len(durations), 0.99)]),
		MeanMs:     ms(mean),
		PerBackend: map[string]bShare{},
	}
	totalHits := int64(0)
	for _, b := range backends {
		totalHits += b.hits.Load()
	}
	for _, b := range backends {
		hits := b.hits.Load()
		var share float64
		if totalHits > 0 {
			share = 100 * float64(hits) / float64(totalHits)
		}
		r.PerBackend[b.id] = bShare{
			Hits:           hits,
			SharePct:       share,
			BackendDelayMs: int64(b.delay / time.Millisecond),
		}
	}
	return r
}

func ms(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e6 }

func pctIdx(n int, p float64) int {
	idx := int(math.Round(float64(n-1) * p))
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return idx
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
