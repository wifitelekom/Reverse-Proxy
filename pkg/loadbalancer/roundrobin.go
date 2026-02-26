package loadbalancer

import (
	"sort"
	"sync/atomic"
)

// RoundRobin sırayla backend seçer.
// Atomic counter ile thread-safe — mutex gerektirmez.
// Backend listesi dışarıdan gelir; Select() içinde NodeID'ye göre sıralanır.
type RoundRobin struct {
	counter uint64 // atomic
}

// NewRoundRobin yeni bir RoundRobin selector oluşturur.
func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

// Select sıradaki backend'i döner.
// Backend listesi önce NodeID'ye göre lexicographic sıralanır —
// map iteration rastgeleliği etkisiz kılınır, adil dağılım garanti edilir.
func (rr *RoundRobin) Select(backends []*Backend) *Backend {
	if len(backends) == 0 {
		return nil
	}
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].NodeID < backends[j].NodeID
	})
	idx := atomic.AddUint64(&rr.counter, 1)
	return backends[idx%uint64(len(backends))]
}
