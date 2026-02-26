package loadbalancer

import "sync/atomic"

// RoundRobin sırayla backend seçer.
// Atomic counter ile thread-safe — mutex gerektirmez.
// Backend listesi dışarıdan gelir, RoundRobin listeyi saklamaz.
type RoundRobin struct {
	counter uint64 // atomic
}

// NewRoundRobin yeni bir RoundRobin selector oluşturur.
func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

// Select sıradaki backend'i döner.
// Modulo aritmetiği: uint64 overflow'da bile doğru çalışır.
func (rr *RoundRobin) Select(backends []*Backend) *Backend {
	if len(backends) == 0 {
		return nil
	}
	idx := atomic.AddUint64(&rr.counter, 1)
	return backends[idx%uint64(len(backends))]
}
