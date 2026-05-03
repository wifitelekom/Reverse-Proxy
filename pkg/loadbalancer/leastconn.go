package loadbalancer

import "sync/atomic"

// LeastConnections selects the backend with the fewest active connections.
// Stateless: it doesn't cache the backend list, it iterates on every Select.
type LeastConnections struct{}

// NewLeastConnections creates a new LeastConnections selector.
func NewLeastConnections() *LeastConnections {
	return &LeastConnections{}
}

// Select returns the backend with the lowest ActiveConns count.
// O(N) scan — acceptable for small N (4–10 nodes), which is the
// typical fleet size this load balancer targets.
// atomic.LoadInt32 keeps the per-backend read race-free.
func (lc *LeastConnections) Select(backends []*Backend) *Backend {
	if len(backends) == 0 {
		return nil
	}

	var best *Backend
	var bestConns int32 = -1

	for _, b := range backends {
		conns := atomic.LoadInt32(&b.ActiveConns)
		if bestConns == -1 || conns < bestConns {
			best = b
			bestConns = conns
		}
	}

	return best
}
