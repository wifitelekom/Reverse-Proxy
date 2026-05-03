package loadbalancer

import (
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// makeBackendL builds a backend with both ActiveConns and a preset
// LatencyEWMA — used by the P2CEWMA tests to inject known latencies
// without going through RecordLatency.
func makeBackendL(id string, conns int32, ewmaNs int64) *Backend {
	u, _ := url.Parse("http://localhost:8000")
	b := &Backend{URL: u, NodeID: id}
	atomic.StoreInt32(&b.ActiveConns, conns)
	atomic.StoreInt64(&b.LatencyEWMA, ewmaNs)
	return b
}

// =====================================================================
// pickTwoDistinct invariants
// =====================================================================

func TestPickTwoDistinct_AlwaysDifferent(t *testing.T) {
	// The whole P2C correctness story rests on i != j. Verify across many
	// trials and across small n values where the chance of collision in a
	// naive implementation would be highest.
	for n := 2; n <= 8; n++ {
		for trial := 0; trial < 1000; trial++ {
			i, j := pickTwoDistinct(n)
			if i == j {
				t.Fatalf("n=%d: got i==j==%d", n, i)
			}
			if i < 0 || i >= n || j < 0 || j >= n {
				t.Fatalf("n=%d: out-of-range (%d,%d)", n, i, j)
			}
		}
	}
}

func TestPickTwoDistinct_UniformPairCoverage(t *testing.T) {
	// With n=4 there are 12 ordered pairs (4*3). Across many samples each
	// should appear roughly equally often. We accept ±50% slack — this is
	// a sanity check, not a tight statistical test.
	const n = 4
	const trials = 12000
	pairs := make(map[[2]int]int)
	for i := 0; i < trials; i++ {
		a, b := pickTwoDistinct(n)
		pairs[[2]int{a, b}]++
	}
	expected := trials / 12
	for pair, count := range pairs {
		if count < expected/2 || count > expected*3/2 {
			t.Errorf("pair %v: got %d, expected ~%d (±50%%)", pair, count, expected)
		}
	}
}

// =====================================================================
// P2C selector
// =====================================================================

func TestP2C_Empty(t *testing.T) {
	if got := NewP2C().Select(nil); got != nil {
		t.Fatal("nil expected for empty input")
	}
}

func TestP2C_SingleBackend(t *testing.T) {
	p := NewP2C()
	b := makeBackend("only", 99)
	for i := 0; i < 10; i++ {
		if got := p.Select([]*Backend{b}); got != b {
			t.Fatalf("single-backend case must return that backend")
		}
	}
}

func TestP2C_TwoBackendsAlwaysPicksLighter(t *testing.T) {
	// With exactly two backends both are always sampled, so the lighter
	// one wins deterministically — useful for spotting comparator bugs.
	p := NewP2C()
	heavy := makeBackend("heavy", 100)
	light := makeBackend("light", 1)

	for i := 0; i < 50; i++ {
		got := p.Select([]*Backend{heavy, light})
		if got.NodeID != "light" {
			t.Fatalf("trial %d: lighter backend not chosen, got %s", i, got.NodeID)
		}
	}
}

func TestP2C_SkewsTowardLighterUnderUneven(t *testing.T) {
	// Across many calls, the lightly-loaded backend should be chosen
	// substantially more often than the heavily-loaded one. We don't pin
	// to an exact ratio (P2C is stochastic) — just check the trend.
	p := NewP2C()
	backends := []*Backend{
		makeBackend("hot-1", 50),
		makeBackend("hot-2", 50),
		makeBackend("cool", 0),
	}

	const trials = 3000
	counts := map[string]int{}
	for i := 0; i < trials; i++ {
		counts[p.Select(backends).NodeID]++
	}

	// "cool" is sampled with prob 2/3 in any pair (probability of being one
	// of two distinct picks from 3). When sampled it always wins (load 0).
	// So expect cool ≈ 2*trials/3 ≈ 2000.
	if counts["cool"] < trials/2 {
		t.Errorf("cool backend underused: %d of %d (expected > %d)",
			counts["cool"], trials, trials/2)
	}
	t.Logf("distribution: %+v", counts)
}

func TestP2C_UniformUnderEqualLoad(t *testing.T) {
	// With equal loads each pair tie-breaks deterministically (returns the
	// first sampled), so the distribution should reflect the underlying
	// uniform pair sampling. Every backend must see substantial traffic —
	// a bug that always returned the same pointer would fail here.
	//
	// Note: if the comparator picks the FIRST sampled on ties (which it
	// does here), each backend's frequency equals its probability of being
	// the i-coordinate of pickTwoDistinct, which is uniform = 1/n.
	p := NewP2C()
	backends := []*Backend{
		makeBackend("a", 5),
		makeBackend("b", 5),
		makeBackend("c", 5),
	}

	const trials = 3000
	counts := map[string]int{}
	for i := 0; i < trials; i++ {
		counts[p.Select(backends).NodeID]++
	}

	expected := trials / 3
	tolerance := expected / 4 // ±25% — loose because of tie-breaking direction
	for _, id := range []string{"a", "b", "c"} {
		c := counts[id]
		if c < expected-tolerance || c > expected+tolerance {
			t.Errorf("%s: %d (expected %d ±%d)", id, c, expected, tolerance)
		}
	}
	t.Logf("equal-load distribution: %+v", counts)
}

// TestP2C_NoHerdBehavior — two independent P2C selectors hammering the
// same backend list shouldn't gang up on the global minimum the way
// LeastConn would. We verify by comparing the spread of selections:
// LeastConn would put 100% on the min; P2C should be much more spread.
func TestP2C_NoHerdBehavior(t *testing.T) {
	backends := []*Backend{
		makeBackend("a", 1),
		makeBackend("b", 5),
		makeBackend("c", 10),
		makeBackend("d", 20),
	}

	// Same backends, two selectors firing concurrently.
	const perSelector = 500
	var wg sync.WaitGroup
	counts := make(map[string]int)
	var mu sync.Mutex

	for s := 0; s < 2; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := NewP2C()
			local := map[string]int{}
			for i := 0; i < perSelector; i++ {
				local[p.Select(backends).NodeID]++
			}
			mu.Lock()
			for k, v := range local {
				counts[k] += v
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	// LeastConn would put 100% on "a" (load=1). P2C should spread enough
	// that at least 3 of 4 backends see traffic.
	visited := 0
	for _, c := range counts {
		if c > 0 {
			visited++
		}
	}
	if visited < 3 {
		t.Errorf("herd-like behavior: only %d/4 backends saw traffic; counts=%+v",
			visited, counts)
	}
}

// =====================================================================
// P2CEWMA selector
// =====================================================================

func TestP2CEWMA_Empty(t *testing.T) {
	if got := NewP2CEWMA().Select(nil); got != nil {
		t.Fatal("nil expected for empty input")
	}
}

func TestP2CEWMA_SingleBackend(t *testing.T) {
	p := NewP2CEWMA()
	b := makeBackendL("only", 0, int64(50*time.Millisecond))
	if got := p.Select([]*Backend{b}); got != b {
		t.Fatalf("single backend must be returned")
	}
}

func TestP2CEWMA_PrefersLowerLatency(t *testing.T) {
	// Equal connection count, very different latencies. The lower-latency
	// backend must be picked every time when both are sampled.
	p := NewP2CEWMA()
	fast := makeBackendL("fast", 5, int64(10*time.Millisecond))
	slow := makeBackendL("slow", 5, int64(200*time.Millisecond))

	for i := 0; i < 100; i++ {
		got := p.Select([]*Backend{fast, slow})
		if got.NodeID != "fast" {
			t.Fatalf("trial %d: P2CEWMA chose slow over fast", i)
		}
	}
}

func TestP2CEWMA_FallsBackToConnsWhenUnmeasured(t *testing.T) {
	// One backend has no measurements (ewma=0). Without the fallback,
	// either it would always win (treated as zero latency) or always
	// lose (treated as infinite). With the fallback, it competes on
	// connection count — and here it has fewer, so it should win.
	p := NewP2CEWMA()
	measured := makeBackendL("measured", 10, int64(50*time.Millisecond))
	fresh := makeBackendL("fresh", 1, 0) // no samples yet

	for i := 0; i < 100; i++ {
		got := p.Select([]*Backend{measured, fresh})
		if got.NodeID != "fresh" {
			t.Fatalf("trial %d: fallback to ActiveConns failed; got %s", i, got.NodeID)
		}
	}
}

// =====================================================================
// Backend.RecordLatency (EWMA arithmetic)
// =====================================================================

func TestRecordLatency_FirstSampleStoredDirectly(t *testing.T) {
	b := makeBackendL("b", 0, 0)
	b.RecordLatency(100 * time.Millisecond)
	got := atomic.LoadInt64(&b.LatencyEWMA)
	if got != int64(100*time.Millisecond) {
		t.Errorf("first sample: got %v, expected 100ms", time.Duration(got))
	}
}

func TestRecordLatency_BlendsWithEWMA(t *testing.T) {
	// With α=0.5, 100ms then 200ms → EWMA = 0.5*200 + 0.5*100 = 150ms.
	// With α=0.5, then 100ms again → 0.5*100 + 0.5*150 = 125ms.
	b := makeBackendL("b", 0, 0)
	b.RecordLatency(100 * time.Millisecond)
	b.RecordLatency(200 * time.Millisecond)

	got := time.Duration(atomic.LoadInt64(&b.LatencyEWMA))
	want := 150 * time.Millisecond
	if got != want {
		t.Errorf("after 100ms,200ms: got %v, want %v", got, want)
	}

	b.RecordLatency(100 * time.Millisecond)
	got = time.Duration(atomic.LoadInt64(&b.LatencyEWMA))
	want = 125 * time.Millisecond
	if got != want {
		t.Errorf("after 100ms,200ms,100ms: got %v, want %v", got, want)
	}
}

func TestRecordLatency_ConcurrentSafe(t *testing.T) {
	// Hammer RecordLatency from many goroutines and confirm:
	//   - no torn writes / negative values
	//   - the result is bounded by the input range
	b := makeBackendL("b", 0, 0)
	const goroutines = 50
	const iterations = 200

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				b.RecordLatency(50 * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	got := atomic.LoadInt64(&b.LatencyEWMA)
	if got <= 0 {
		t.Errorf("ewma went non-positive under concurrency: %d", got)
	}
	// All inputs were 50ms, so EWMA must converge to exactly 50ms.
	want := int64(50 * time.Millisecond)
	if got != want {
		t.Errorf("expected EWMA to converge to %v, got %v", time.Duration(want), time.Duration(got))
	}
}
