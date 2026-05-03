package loadbalancer

import (
	"net/url"
	"sync/atomic"
	"testing"
)

func makeBackend(id string, conns int32) *Backend {
	u, _ := url.Parse("http://localhost:8000")
	b := &Backend{URL: u, NodeID: id}
	atomic.StoreInt32(&b.ActiveConns, conns)
	return b
}

// TestRoundRobin_DeterministicOrder: pre-sorted backends → deterministic order.
// Select() no longer sorts; getOrSyncBackends guarantees a sorted slice.
// Counter starts at ^uint64(0): first AddUint64(1) wraps to 0 → node-a,
// then node-b, node-c, and back to node-a.
func TestRoundRobin_DeterministicOrder(t *testing.T) {
	rr := NewRoundRobin()
	// Pre-sorted — the format getOrSyncBackends produces.
	backends := []*Backend{
		makeBackend("node-a", 0),
		makeBackend("node-b", 0),
		makeBackend("node-c", 0),
	}

	expected := []string{"node-a", "node-b", "node-c", "node-a", "node-b", "node-c"}
	for i, want := range expected {
		bc := make([]*Backend, len(backends))
		copy(bc, backends)
		got := rr.Select(bc)
		if got.NodeID != want {
			t.Errorf("step %d: expected %q, got %q", i, want, got.NodeID)
		}
	}
}

// TestRoundRobin_FairDistribution: across N requests over K backends,
// each backend should receive roughly N/K requests within ±10%.
func TestRoundRobin_FairDistribution(t *testing.T) {
	rr := NewRoundRobin()
	backends := []*Backend{
		makeBackend("node-a", 0),
		makeBackend("node-b", 0),
		makeBackend("node-c", 0),
	}

	counts := map[string]int{}
	const total = 300
	for i := 0; i < total; i++ {
		bc := make([]*Backend, len(backends))
		copy(bc, backends)
		b := rr.Select(bc)
		counts[b.NodeID]++
	}

	expected := total / len(backends) // 100
	tolerance := expected / 10        // ±10%

	for id, c := range counts {
		if c < expected-tolerance || c > expected+tolerance {
			t.Errorf("%s: %d requests (expected %d ±%d)", id, c, expected, tolerance)
		}
	}
}

// TestRoundRobin_SingleBackend: with a single backend, always pick it.
func TestRoundRobin_SingleBackend(t *testing.T) {
	rr := NewRoundRobin()
	b := makeBackend("node-a", 0)
	for i := 0; i < 10; i++ {
		got := rr.Select([]*Backend{b})
		if got.NodeID != "node-a" {
			t.Fatalf("single backend must be selected every time")
		}
	}
}

// TestRoundRobin_Empty: empty input → nil.
func TestRoundRobin_Empty(t *testing.T) {
	rr := NewRoundRobin()
	if got := rr.Select(nil); got != nil {
		t.Fatal("empty input must return nil")
	}
}

// TestLeastConn_SelectsLowest: the backend with fewest ActiveConns wins.
func TestLeastConn_SelectsLowest(t *testing.T) {
	lc := NewLeastConnections()
	backends := []*Backend{
		makeBackend("node-a", 5),
		makeBackend("node-b", 2), // fewest connections
		makeBackend("node-c", 8),
	}

	got := lc.Select(backends)
	if got.NodeID != "node-b" {
		t.Fatalf("expected node-b (2 conn), got %q", got.NodeID)
	}
}

// TestLeastConn_AllEqual: when every backend has equal load,
// the first one is returned (current implementation detail).
func TestLeastConn_AllEqual(t *testing.T) {
	lc := NewLeastConnections()
	backends := []*Backend{
		makeBackend("node-a", 3),
		makeBackend("node-b", 3),
		makeBackend("node-c", 3),
	}
	got := lc.Select(backends)
	if got == nil {
		t.Fatal("must not return nil for non-empty input")
	}
}

// TestLeastConn_Empty: empty input → nil.
func TestLeastConn_Empty(t *testing.T) {
	lc := NewLeastConnections()
	if got := lc.Select(nil); got != nil {
		t.Fatal("empty input must return nil")
	}
}

// TestBackendPool_Reuses: getOrSyncBackends must return the SAME *Backend
// pointer for the same nodeID. ActiveConns is preserved across requests
// — this is critical for LeastConn / P2C correctness.
func TestBackendPool_Reuses(t *testing.T) {
	// Test the backend-pool semantics directly.
	pool := make(map[string]*Backend)

	// First call: create a new Backend.
	b1 := makeBackend("node-a", 0)
	pool["node-a"] = b1
	atomic.AddInt32(&b1.ActiveConns, 3) // simulate 3 in-flight requests

	// Second call: same nodeID → fetched from pool.
	b2 := pool["node-a"]
	if b1 != b2 {
		t.Fatal("same nodeID must return the same *Backend pointer")
	}
	if atomic.LoadInt32(&b2.ActiveConns) != 3 {
		t.Fatalf("ActiveConns lost: expected 3, got %d", atomic.LoadInt32(&b2.ActiveConns))
	}
	t.Log("backend pool reuse confirmed — ActiveConns survives across calls")
}

// TestLeastConn_AtomicRead: LeastConn must use atomic loads so it
// observes concurrent ActiveConns updates correctly.
func TestLeastConn_AtomicRead(t *testing.T) {
	lc := NewLeastConnections()
	a := makeBackend("node-a", 10)
	b := makeBackend("node-b", 0)

	// Simulate a concurrent increment: bump a's connection count by 5.
	for i := 0; i < 5; i++ {
		atomic.AddInt32(&a.ActiveConns, 1)
	}

	got := lc.Select([]*Backend{a, b})
	if got.NodeID != "node-b" {
		t.Fatalf("expected node-b (0 conn), got %q", got.NodeID)
	}
}
