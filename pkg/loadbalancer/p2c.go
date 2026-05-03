package loadbalancer

import (
	"math/rand"
	"sync/atomic"
)

// P2C implements the "Power of Two Choices" load balancing algorithm
// (Mitzenmacher 1996). On each Select it samples two distinct backends
// uniformly at random and returns the one with fewer active connections.
//
// Why P2C beats LeastConnections in distributed setups:
//
// Multiple LB instances running in parallel each independently scan the
// full backend list and pick the global minimum. They tend to pile their
// next request onto the same "least loaded" backend — the herd / thundering-
// herd problem. With P2C the random sampling decorrelates choices across
// LBs, so the maximum load on any backend is bounded by O(log log N)
// instead of LeastConn's O(log N) per round.
//
// Cost: O(1) per call, two atomic reads, no allocation. Backends do not
// need to be sorted (unlike RoundRobin).
//
// Concurrency: math/rand's top-level Intn is safe for concurrent use as of
// Go 1.20+, so no per-selector mutex is required.
type P2C struct{}

// NewP2C creates a Power-of-Two-Choices selector.
func NewP2C() *P2C { return &P2C{} }

// Select samples two distinct backends and returns the one with fewer
// active connections. Edge cases:
//
//   - len(backends) == 0 → nil
//   - len(backends) == 1 → that backend (no choice to make)
//   - len(backends) == 2 → both are sampled, the lighter one wins
func (p *P2C) Select(backends []*Backend) *Backend {
	n := len(backends)
	if n == 0 {
		return nil
	}
	if n == 1 {
		return backends[0]
	}

	i, j := pickTwoDistinct(n)
	return lighterByConns(backends[i], backends[j])
}

// P2CEWMA is the latency-aware variant of P2C. Same two-random sampling,
// but the comparator picks the backend with the lower exponentially-
// weighted moving average response time.
//
// When either sampled backend has no measurements yet (LatencyEWMA == 0)
// the comparator falls back to ActiveConns. This avoids two failure modes:
//
//  1. A freshly-added backend would otherwise look "best" (0 latency) and
//     get hammered by every request until it accumulates a sample.
//  2. A freshly-added backend would otherwise be starved if 0 was treated
//     as infinity.
//
// Falling back to ActiveConns gives sane behaviour during the brief warmup
// window. Once both backends have any sample at all, latency wins.
type P2CEWMA struct{}

// NewP2CEWMA creates a latency-aware P2C selector.
func NewP2CEWMA() *P2CEWMA { return &P2CEWMA{} }

// Select samples two distinct backends and returns the lower-latency one
// (or, during warmup, the lower-load one).
func (p *P2CEWMA) Select(backends []*Backend) *Backend {
	n := len(backends)
	if n == 0 {
		return nil
	}
	if n == 1 {
		return backends[0]
	}

	i, j := pickTwoDistinct(n)
	a, b := backends[i], backends[j]

	la := atomic.LoadInt64(&a.LatencyEWMA)
	lb := atomic.LoadInt64(&b.LatencyEWMA)

	// Warmup fallback: if either side has no measurement yet, lean on
	// connection count, which is already a valid signal.
	if la == 0 || lb == 0 {
		return lighterByConns(a, b)
	}

	if la <= lb {
		return a
	}
	return b
}

// pickTwoDistinct returns two uniformly-distributed distinct indices in
// [0, n). Standard rejection-free trick: pick j from [0, n-1) and shift
// it past i if needed. This avoids retry loops.
//
// Precondition: n >= 2.
func pickTwoDistinct(n int) (int, int) {
	i := rand.Intn(n)
	j := rand.Intn(n - 1)
	if j >= i {
		j++
	}
	return i, j
}

// lighterByConns returns the backend with fewer active connections.
// Ties go to a (the first sampled), making the result fully deterministic
// once the random pair is fixed — useful for reproducibility in tests.
func lighterByConns(a, b *Backend) *Backend {
	if atomic.LoadInt32(&a.ActiveConns) <= atomic.LoadInt32(&b.ActiveConns) {
		return a
	}
	return b
}
