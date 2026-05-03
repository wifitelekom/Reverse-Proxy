package healthcheck

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCallbackFanout_BoundedUnderSaturation pins the H-1 fix.
//
// We install a deliberately slow callback (held by a channel) and
// trigger far more transitions than the slot capacity. Without the
// bound, every transition would launch its own goroutine — leaking
// hundreds. With the bound, in-flight goroutines are capped at
// callbackSlotCount; the rest are dropped with a warn log.
func TestCallbackFanout_BoundedUnderSaturation(t *testing.T) {
	c := NewChecker(fastConfig("/"), silentLogger())

	// Mint enough targets to drive many transitions. Each gets a
	// "node-N" id so applyResult can find it.
	const numTargets = 100
	targets := make(map[string]string, numTargets)
	for i := 0; i < numTargets; i++ {
		id := nodeID(i)
		targets[id] = "127.0.0.1:1"
	}
	c.UpdateTargets(targets)

	// Slow callback — blocks on `release` so we can saturate slots
	// deterministically. Counter tracks how many goroutines are
	// concurrently inside the callback.
	release := make(chan struct{})
	var inFlight atomic.Int32
	var peakInFlight atomic.Int32
	var totalInvoked atomic.Int32

	c.SetOnStatusChange(func(_ string, _, _ Status) {
		now := inFlight.Add(1)
		// Track the peak — we'll compare against callbackSlotCount.
		for {
			peak := peakInFlight.Load()
			if now <= peak || peakInFlight.CompareAndSwap(peak, now) {
				break
			}
		}
		<-release
		inFlight.Add(-1)
		totalInvoked.Add(1)
	})

	// Track goroutine baseline before saturating. We'll verify it
	// returns to within an acceptable delta of baseline once we
	// release the callbacks.
	baseline := runtime.NumGoroutine()

	// Drive 100 transitions, far exceeding the 16-slot cap. Each call
	// to applyResult with a new outcome will (eventually) flip status
	// and fire the callback.
	for i := 0; i < numTargets; i++ {
		c.applyResult(nodeID(i), time.Millisecond, 200, true, false)
	}

	// Give the runtime a moment to schedule the goroutines.
	time.Sleep(50 * time.Millisecond)

	// The semaphore must hold the line: at most callbackSlotCount
	// callbacks in-flight, regardless of how many transitions fire.
	peak := peakInFlight.Load()
	if peak > int32(callbackSlotCount) {
		t.Errorf("in-flight callbacks exceeded slot cap: peak=%d cap=%d",
			peak, callbackSlotCount)
	}
	if peak < 1 {
		t.Errorf("no callbacks fired at all (peak=%d) — slots wired wrong?", peak)
	}
	t.Logf("peak in-flight callbacks: %d (cap %d)", peak, callbackSlotCount)

	// Now verify NO uncontrolled goroutine growth. With 100 transitions
	// and the slow callback blocking, an unbounded design would have
	// ~100 goroutines waiting on `release` simultaneously. Our cap
	// keeps it at most callbackSlotCount above baseline.
	current := runtime.NumGoroutine()
	growth := current - baseline
	if growth > callbackSlotCount+5 { // +5 for goroutine scheduling slack
		t.Errorf("goroutine growth exceeded cap: baseline=%d current=%d growth=%d cap=%d",
			baseline, current, growth, callbackSlotCount)
	}

	// Release the callbacks and wait for them to finish.
	close(release)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if inFlight.Load() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if inFlight.Load() != 0 {
		t.Errorf("callbacks didn't drain: still %d in flight", inFlight.Load())
	}

	// The bound dropped some notifications; that's the documented
	// trade-off. Ensure at least one batch ran (callbackSlotCount
	// should be invoked, give or take race timing).
	invoked := totalInvoked.Load()
	if invoked < int32(callbackSlotCount/2) {
		t.Errorf("suspiciously few callbacks ran: %d", invoked)
	}
	t.Logf("invoked %d / %d transitions (slots dropped the rest)",
		invoked, numTargets)
}

// TestCallbackFanout_FastCallbacksAllFire is the steady-state guard:
// with fast callbacks AND traffic within the slot capacity, every
// transition fires and nothing is dropped.
//
// Note: with a tight loop firing more transitions than the slot count,
// some WILL be dropped — that's the documented contract under
// saturation (TestCallbackFanout_BoundedUnderSaturation). This test
// stays under the cap to verify the no-drop case works as advertised.
func TestCallbackFanout_FastCallbacksAllFire(t *testing.T) {
	c := NewChecker(fastConfig("/"), silentLogger())

	// Stay under callbackSlotCount=16 so every transition gets a slot
	// even when fired in a tight loop with no scheduling pause.
	const numTargets = 10
	targets := make(map[string]string, numTargets)
	for i := 0; i < numTargets; i++ {
		targets[nodeID(i)] = "127.0.0.1:1"
	}
	c.UpdateTargets(targets)

	var fired atomic.Int32
	var wg sync.WaitGroup
	wg.Add(numTargets)
	c.SetOnStatusChange(func(_ string, _, _ Status) {
		fired.Add(1)
		wg.Done()
	})

	for i := 0; i < numTargets; i++ {
		c.applyResult(nodeID(i), time.Millisecond, 200, true, false)
	}

	// Wait for all callbacks to run with a generous deadline. Fast
	// callbacks under cap should drain near-instantly.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("only %d/%d callbacks ran in 2s — cap was %d, traffic was %d (under cap)",
			fired.Load(), numTargets, callbackSlotCount, numTargets)
	}
	if got := fired.Load(); got != int32(numTargets) {
		t.Errorf("fired %d, expected %d (under-cap traffic must not drop)",
			got, numTargets)
	}
}

// TestCallbackFanout_PacedTrafficNoDrops also proves the no-drop path
// at counts EXCEEDING the cap, but with a small pause between calls.
// The pause gives the goroutine pool time to drain a slot before the
// next call. Reflects realistic operation: status changes don't burst
// hundreds-per-millisecond — they trickle as backends flap.
func TestCallbackFanout_PacedTrafficNoDrops(t *testing.T) {
	c := NewChecker(fastConfig("/"), silentLogger())

	const numTargets = 30 // > callbackSlotCount, but paced
	targets := make(map[string]string, numTargets)
	for i := 0; i < numTargets; i++ {
		targets[nodeID(i)] = "127.0.0.1:1"
	}
	c.UpdateTargets(targets)

	var fired atomic.Int32
	var wg sync.WaitGroup
	wg.Add(numTargets)
	c.SetOnStatusChange(func(_ string, _, _ Status) {
		fired.Add(1)
		wg.Done()
	})

	for i := 0; i < numTargets; i++ {
		c.applyResult(nodeID(i), time.Millisecond, 200, true, false)
		// 1ms pause is more than enough — fast callbacks finish in
		// nanoseconds, so a slot is always free by next iteration.
		time.Sleep(1 * time.Millisecond)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("paced traffic dropped notifications: %d/%d fired",
			fired.Load(), numTargets)
	}
	if got := fired.Load(); got != int32(numTargets) {
		t.Errorf("paced traffic should be lossless: %d/%d", got, numTargets)
	}
}

// TestCallbackFanout_PanicDoesNotLeakSlot verifies the deferred
// slot-release survives a panicking user callback. Without the
// recover-and-release pair, a single panic would permanently consume
// a slot, eventually starving the cap.
func TestCallbackFanout_PanicDoesNotLeakSlot(t *testing.T) {
	c := NewChecker(fastConfig("/"), silentLogger())
	c.UpdateTargets(map[string]string{"n": "127.0.0.1:1"})

	var fired atomic.Int32
	c.SetOnStatusChange(func(_ string, _, _ Status) {
		fired.Add(1)
		if fired.Load() == 1 {
			panic("simulated user error")
		}
	})

	// First transition: panics. The deferred receive in fireStatusChange
	// must release the slot; recover prevents the process going down.
	c.applyResult("n", time.Millisecond, 200, true, false) // → Healthy
	time.Sleep(50 * time.Millisecond)

	// If the slot leaked, the next callback wouldn't fire (slot full).
	// We force another transition: 500 → Unhealthy threshold = 3, so
	// fire three failures.
	cfg := c.config
	cfg.UnhealthyThreshold = 1
	c.config = cfg
	c.applyResult("n", time.Millisecond, 500, false, false) // → Unhealthy

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if fired.Load() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if fired.Load() < 2 {
		t.Errorf("second callback didn't fire — panic leaked the slot? fired=%d",
			fired.Load())
	}
}

// nodeID generates a deterministic per-index node identifier. Used to
// keep test setup small and readable when minting many targets.
func nodeID(i int) string {
	return "node-" + itoa(i)
}

// itoa avoids importing strconv just for the test helper.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	digits := []byte{}
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}
