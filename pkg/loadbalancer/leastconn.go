package loadbalancer

import "sync/atomic"

// LeastConnections en az aktif bağlantıya sahip backend'i seçer.
// Stateless: backend listesini saklamaz, her Select çağrısında iterate eder.
type LeastConnections struct{}

// NewLeastConnections yeni bir LeastConnections selector oluşturur.
func NewLeastConnections() *LeastConnections {
	return &LeastConnections{}
}

// Select en az ActiveConns'lu backend'i döner.
// O(N) scan — N küçük (4-10 node) için kabul edilebilir.
// atomic.LoadInt32 ile thread-safe okuma.
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
