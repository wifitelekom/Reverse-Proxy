package gossip

import (
	"math"
	"sync"
	"time"
)

// SelfAwareness tracks the local node's recent probe success rate so the
// protocol can scale its outgoing ping/indirect timeouts under stress.
//
// The "noop" counter is the original term from the Lifeguard paper
// (HashiCorp / arXiv 1707.00788). It increments on a missed direct ACK and
// decrements on a successful one. The score, normalized to [0, max], drives
// a linear interpolation between min and max timeouts:
//
//	timeout = min + (max - min) * (noop / max)
//
// A healthy node keeps noop near 0 and operates at the snappy minimum.
// A node hammered by GC pauses, CPU starvation, or network jitter rises
// toward noop=max and self-extends its tolerances, avoiding the false-
// positive cascades that vanilla SWIM is prone to in those conditions.
//
// Concurrency: a small mutex is sufficient — Record* and ScaleTimeout fire
// at most a handful of times per protocol round (1Hz default), so lock
// contention is irrelevant.
type SelfAwareness struct {
	mu   sync.Mutex
	noop int
	max  int
}

// NewSelfAwareness creates a tracker capped at max. A max <= 0 disables
// scaling effectively — ScaleTimeout will always return min.
func NewSelfAwareness(max int) *SelfAwareness {
	return &SelfAwareness{max: max}
}

// RecordSuccess decrements noop, floored at 0.
// Call after a direct ACK is received.
func (s *SelfAwareness) RecordSuccess() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.noop > 0 {
		s.noop--
	}
}

// RecordFailure increments noop, capped at max.
// Call after a direct ping times out.
func (s *SelfAwareness) RecordFailure() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.noop < s.max {
		s.noop++
	}
}

// Score returns the current noop count. Mainly for tests and observability.
func (s *SelfAwareness) Score() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.noop
}

// ScaleTimeout linearly interpolates between min and max based on the
// current noop score. If max <= 0 (disabled) or min >= max the result is
// min — a safe degenerate case.
func (s *SelfAwareness) ScaleTimeout(min, max time.Duration) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.max <= 0 || min >= max {
		return min
	}
	factor := float64(s.noop) / float64(s.max)
	return min + time.Duration(float64(max-min)*factor)
}

// dogpileTimeout is the Lifeguard "Dogpile" formula:
//
//	timeout = max(min, max * log(K+1) / log(confirmations + K + 1))
//
// At zero confirmations the ratio is 1.0 → timeout = max (the most lenient
// state). Each independent confirmation grows the denominator, pulling the
// timeout down toward min. K (the protocol's fan-out factor) tunes how
// aggressively confirmations shorten the window — a wider K means each vote
// has proportionally less weight, which is what we want when the suspect
// signal could already be widespread by gossip alone.
//
// Returns max if confirmations <= 0; never below min.
func dogpileTimeout(min, max time.Duration, confirmations, k int) time.Duration {
	if confirmations <= 0 || max <= 0 {
		return max
	}
	if k < 1 {
		k = 1
	}
	ratio := math.Log(float64(k+1)) / math.Log(float64(confirmations+k+1))
	t := time.Duration(float64(max) * ratio)
	if t < min {
		return min
	}
	return t
}
