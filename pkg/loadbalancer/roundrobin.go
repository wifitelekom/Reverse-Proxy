package loadbalancer

import "sync/atomic"

// RoundRobin sırayla backend seçer.
// Atomic counter ile thread-safe — mutex gerektirmez.
// Backends getOrSyncBackends tarafından NodeID'ye göre pre-sorted gelir;
// Select() sadece atomic index hesaplar — O(1), allocation yok.
type RoundRobin struct {
	counter uint64 // atomic
}

// NewRoundRobin yeni bir RoundRobin selector oluşturur.
// counter ^uint64(0) (maxUint64) ile başlar: ilk AddUint64(1) → 0 → index 0 seçilir.
func NewRoundRobin() *RoundRobin {
	return &RoundRobin{counter: ^uint64(0)}
}

// Select sıradaki backend'i döner — O(1).
// Ön koşul: backends NodeID'ye göre sıralı olmalı (getOrSyncBackends garantisi).
// Sıralama sorumluluğu LoadBalancer.getOrSyncBackends'ta; burada yapılmaz.
func (rr *RoundRobin) Select(backends []*Backend) *Backend {
	if len(backends) == 0 {
		return nil
	}
	idx := atomic.AddUint64(&rr.counter, 1)
	return backends[idx%uint64(len(backends))]
}
