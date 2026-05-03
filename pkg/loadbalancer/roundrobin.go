package loadbalancer

import "sync/atomic"

// RoundRobin selects backends in order. Thread-safe via an atomic
// counter — no mutex required. The backend slice is delivered already
// sorted by NodeID by getOrSyncBackends, so Select only computes an
// atomic index — O(1) with no allocation.
type RoundRobin struct {
	counter uint64 // atomic
}

// NewRoundRobin creates a new RoundRobin selector.
// The counter starts at ^uint64(0) (maxUint64) so the first
// AddUint64(1) wraps to 0, selecting index 0 on the first call.
func NewRoundRobin() *RoundRobin {
	return &RoundRobin{counter: ^uint64(0)}
}

// Select returns the next backend in round-robin order — O(1).
// Precondition: backends must be sorted by NodeID (guaranteed by
// LoadBalancer.getOrSyncBackends). Sorting is the load balancer's
// responsibility, not this selector's.
func (rr *RoundRobin) Select(backends []*Backend) *Backend {
	if len(backends) == 0 {
		return nil
	}
	idx := atomic.AddUint64(&rr.counter, 1)
	return backends[idx%uint64(len(backends))]
}
