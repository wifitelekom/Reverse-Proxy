package gossip

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"p2p-loadbalancer/pkg/types"
)

// =====================================================================
// SelfAwareness unit tests
// =====================================================================

func TestSelfAwareness_StartsAtZero(t *testing.T) {
	s := NewSelfAwareness(8)
	if s.Score() != 0 {
		t.Errorf("expected initial score 0, got %d", s.Score())
	}
}

func TestSelfAwareness_RecordFailure_CapsAtMax(t *testing.T) {
	s := NewSelfAwareness(3)
	for i := 0; i < 10; i++ {
		s.RecordFailure()
	}
	if s.Score() != 3 {
		t.Errorf("score must cap at max=3, got %d", s.Score())
	}
}

func TestSelfAwareness_RecordSuccess_FloorsAtZero(t *testing.T) {
	s := NewSelfAwareness(8)
	for i := 0; i < 5; i++ {
		s.RecordSuccess()
	}
	if s.Score() != 0 {
		t.Errorf("score must floor at 0, got %d", s.Score())
	}
}

func TestSelfAwareness_ScaleTimeout_Endpoints(t *testing.T) {
	s := NewSelfAwareness(8)
	min := 500 * time.Millisecond
	max := 4 * time.Second

	// noop=0 → min
	if got := s.ScaleTimeout(min, max); got != min {
		t.Errorf("noop=0: expected %v, got %v", min, got)
	}

	// Push to max
	for i := 0; i < 8; i++ {
		s.RecordFailure()
	}
	if got := s.ScaleTimeout(min, max); got != max {
		t.Errorf("noop=max: expected %v, got %v", max, got)
	}
}

func TestSelfAwareness_ScaleTimeout_LinearMidpoint(t *testing.T) {
	// At noop=4 (half of max=8), timeout should be exactly min + (max-min)/2.
	s := NewSelfAwareness(8)
	for i := 0; i < 4; i++ {
		s.RecordFailure()
	}
	min := 500 * time.Millisecond
	max := 4500 * time.Millisecond // span 4s
	want := min + 2*time.Second    // halfway = +2s
	got := s.ScaleTimeout(min, max)
	if got != want {
		t.Errorf("midpoint: expected %v, got %v", want, got)
	}
}

func TestSelfAwareness_ScaleTimeout_Disabled(t *testing.T) {
	// max=0 → scaling disabled, always returns min regardless of noop.
	s := NewSelfAwareness(0)
	s.RecordFailure() // would normally increment
	min := 500 * time.Millisecond
	max := 4 * time.Second
	if got := s.ScaleTimeout(min, max); got != min {
		t.Errorf("disabled: expected %v, got %v", min, got)
	}
}

// =====================================================================
// Dogpile formula unit tests
// =====================================================================

func TestDogpileTimeout_NoConfirmationsReturnsMax(t *testing.T) {
	// Without any independent vote there's no reason to shorten the window.
	got := dogpileTimeout(3*time.Second, 24*time.Second, 0, 3)
	if got != 24*time.Second {
		t.Errorf("expected max=24s, got %v", got)
	}
}

func TestDogpileTimeout_MonotonicDecrease(t *testing.T) {
	// Each additional confirmation must shrink (or hold) the timeout — never
	// extend it. This is the core invariant of the Dogpile mechanism.
	min := 1 * time.Second
	max := 30 * time.Second
	prev := time.Duration(math.MaxInt64)
	for c := 0; c < 50; c++ {
		got := dogpileTimeout(min, max, c, 3)
		if got > prev {
			t.Errorf("c=%d: timeout grew (%v > %v)", c, got, prev)
		}
		prev = got
	}
}

func TestDogpileTimeout_RespectsMin(t *testing.T) {
	// Pick parameters where the raw formula would go below min, so the
	// floor must engage. Formula at c=1000, K=3, max=2s:
	//   log(4)/log(1004) ≈ 0.2 → 0.4s — below our 1s floor.
	min := 1 * time.Second
	max := 2 * time.Second
	got := dogpileTimeout(min, max, 1000, 3)
	if got != min {
		t.Errorf("expected floor=%v, got %v", min, got)
	}
}

func TestDogpileTimeout_FormulaCorrectness(t *testing.T) {
	// Spot-check the formula: timeout = max * log(K+1) / log(c+K+1)
	min := 1 * time.Second
	max := 24 * time.Second
	k := 3
	c := 3 // confirmations

	expectedRatio := math.Log(float64(k+1)) / math.Log(float64(c+k+1)) // log(4)/log(7)
	expected := time.Duration(float64(max) * expectedRatio)
	got := dogpileTimeout(min, max, c, k)

	// Allow 1ms tolerance for floating point conversion.
	diff := got - expected
	if diff < 0 {
		diff = -diff
	}
	if diff > time.Millisecond {
		t.Errorf("c=%d k=%d: expected %v, got %v (diff %v)", c, k, expected, got, diff)
	}
}

// =====================================================================
// MembershipList.ConfirmSuspect tests
// =====================================================================

func TestConfirmSuspect_TransitionsAliveToSuspect(t *testing.T) {
	ml := NewMembershipList("self", silentLogger())
	ml.AddOrUpdate(NodeInfo{ID: "n", Addr: "x", State: StateAlive, Incarnation: 1})

	votes, ok := ml.ConfirmSuspect("n", "voter-1",
		func(int) time.Duration { return 1 * time.Hour }, // long enough to not fire
		func(string) {},
	)
	if !ok {
		t.Fatal("ConfirmSuspect returned ok=false")
	}
	if votes != 1 {
		t.Errorf("expected 1 vote, got %d", votes)
	}
	got, _ := ml.GetMember("n")
	if got.State != StateSuspect {
		t.Errorf("expected Suspect, got %v", got.State)
	}
}

func TestConfirmSuspect_DedupesConfirmer(t *testing.T) {
	ml := NewMembershipList("self", silentLogger())
	ml.AddOrUpdate(NodeInfo{ID: "n", Addr: "x", State: StateAlive, Incarnation: 1})

	noFire := func(int) time.Duration { return 1 * time.Hour }
	ml.ConfirmSuspect("n", "voter-1", noFire, func(string) {})
	ml.ConfirmSuspect("n", "voter-1", noFire, func(string) {}) // duplicate
	votes, _ := ml.ConfirmSuspect("n", "voter-1", noFire, func(string) {})
	if votes != 1 {
		t.Errorf("duplicate confirmer should not increase votes; got %d", votes)
	}
}

func TestConfirmSuspect_DistinctConfirmersAccumulate(t *testing.T) {
	ml := NewMembershipList("self", silentLogger())
	ml.AddOrUpdate(NodeInfo{ID: "n", Addr: "x", State: StateAlive, Incarnation: 1})
	noFire := func(int) time.Duration { return 1 * time.Hour }

	ml.ConfirmSuspect("n", "voter-1", noFire, func(string) {})
	ml.ConfirmSuspect("n", "voter-2", noFire, func(string) {})
	votes, _ := ml.ConfirmSuspect("n", "voter-3", noFire, func(string) {})
	if votes != 3 {
		t.Errorf("expected 3 distinct votes, got %d", votes)
	}
}

func TestConfirmSuspect_IgnoresDeadNode(t *testing.T) {
	ml := NewMembershipList("self", silentLogger())
	ml.AddOrUpdate(NodeInfo{ID: "n", Addr: "x", State: StateDead, Incarnation: 1})

	_, ok := ml.ConfirmSuspect("n", "voter", func(int) time.Duration { return time.Hour }, nil)
	if ok {
		t.Error("ConfirmSuspect on Dead node should be ignored")
	}
}

func TestConfirmSuspect_IgnoresUnknownNode(t *testing.T) {
	ml := NewMembershipList("self", silentLogger())
	_, ok := ml.ConfirmSuspect("ghost", "voter", func(int) time.Duration { return time.Hour }, nil)
	if ok {
		t.Error("ConfirmSuspect on unknown node should be ignored")
	}
}

func TestConfirmSuspect_TimerFiresAndCallbackInvoked(t *testing.T) {
	ml := NewMembershipList("self", silentLogger())
	ml.AddOrUpdate(NodeInfo{ID: "n", Addr: "x", State: StateAlive, Incarnation: 1})

	called := make(chan string, 1)
	ml.ConfirmSuspect("n", "voter",
		func(int) time.Duration { return 30 * time.Millisecond },
		func(id string) { called <- id },
	)

	select {
	case id := <-called:
		if id != "n" {
			t.Errorf("callback got id=%q, want n", id)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("dead callback not invoked after timer expiry")
	}

	got, _ := ml.GetMember("n")
	if got.State != StateDead {
		t.Errorf("expected Dead after timer fire, got %v", got.State)
	}
}

func TestConfirmSuspect_RescheduleShrinksTimer(t *testing.T) {
	// Start with a long timeout, then add another confirmer with a short
	// timeout. The dead callback should fire on the *short* schedule.
	ml := NewMembershipList("self", silentLogger())
	ml.AddOrUpdate(NodeInfo{ID: "n", Addr: "x", State: StateAlive, Incarnation: 1})

	called := make(chan struct{}, 1)
	deadCb := func(string) { called <- struct{}{} }

	// First vote: 5s window
	ml.ConfirmSuspect("n", "voter-1",
		func(int) time.Duration { return 5 * time.Second },
		deadCb,
	)
	// Second vote shrinks window to 30ms
	ml.ConfirmSuspect("n", "voter-2",
		func(int) time.Duration { return 30 * time.Millisecond },
		deadCb,
	)

	select {
	case <-called:
		// good — fired on the shorter schedule
	case <-time.After(500 * time.Millisecond):
		t.Fatal("rescheduled timer did not fire on shorter window")
	}
}

func TestAddOrUpdate_AliveClearsConfirmers(t *testing.T) {
	// A revived node must start fresh — no stale Dogpile votes can carry
	// across an Alive transition.
	ml := NewMembershipList("self", silentLogger())
	ml.AddOrUpdate(NodeInfo{ID: "n", Addr: "x", State: StateAlive, Incarnation: 1})
	ml.ConfirmSuspect("n", "voter-1", func(int) time.Duration { return time.Hour }, nil)
	ml.ConfirmSuspect("n", "voter-2", func(int) time.Duration { return time.Hour }, nil)

	// Higher incarnation Alive: counts as revival
	ml.AddOrUpdate(NodeInfo{ID: "n", Addr: "x", State: StateAlive, Incarnation: 2})

	// Now confirm again — votes must start at 1, not 3.
	votes, ok := ml.ConfirmSuspect("n", "voter-1", func(int) time.Duration { return time.Hour }, nil)
	if !ok {
		t.Fatal("ConfirmSuspect returned not-ok")
	}
	if votes != 1 {
		t.Errorf("expected reset to 1 vote after revival, got %d", votes)
	}
}

// =====================================================================
// Protocol integration: Lifeguard end-to-end
// =====================================================================

// lifeguardCfg returns a fast Lifeguard-enabled config for protocol tests.
func lifeguardCfg(gossipAddr, nodeID string) types.ProtocolConfig {
	cfg := fastCfg(gossipAddr, nodeID)
	cfg.LifeguardEnabled = true
	cfg.SelfAwarenessMax = 4
	cfg.PingTimeoutMin = 50 * time.Millisecond
	cfg.PingTimeoutMax = 400 * time.Millisecond
	cfg.IndirectTimeoutMin = 100 * time.Millisecond
	cfg.IndirectTimeoutMax = 800 * time.Millisecond
	cfg.SuspectTimeoutMin = 200 * time.Millisecond
	cfg.SuspectTimeoutMax = 2 * time.Second
	return cfg
}

// TestProtocol_Lifeguard_BuddyNotification verifies that when A marks B
// suspect, B receives a direct (Buddy) notification and self-defends —
// all without waiting for B to be reached via gossip fan-out.
func TestProtocol_Lifeguard_BuddyNotification(t *testing.T) {
	logger := silentLogger()

	addrA := freeUDPAddr(t)
	addrB := freeUDPAddr(t)

	mlA := NewMembershipList("node-a", logger)
	mlB := NewMembershipList("node-b", logger)

	nodeA := NodeInfo{ID: "node-a", Addr: addrA, ServiceAddr: addrA, State: StateAlive, Incarnation: 1}
	nodeB := NodeInfo{ID: "node-b", Addr: addrB, ServiceAddr: addrB, State: StateAlive, Incarnation: 5}

	mlA.AddOrUpdate(nodeA)
	mlA.AddOrUpdate(nodeB)
	mlB.AddOrUpdate(nodeA)
	mlB.AddOrUpdate(nodeB)

	tA, err := NewTransport(addrA, 100, logger)
	if err != nil {
		t.Fatalf("transport A: %v", err)
	}
	tB, err := NewTransport(addrB, 100, logger)
	if err != nil {
		t.Fatalf("transport B: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tA.Start(ctx)
	tB.Start(ctx)

	// Both protocols Lifeguard-enabled, but only B handles messages — A
	// drives the suspectNode call manually.
	protoA := NewProtocol(lifeguardCfg(addrA, "node-a"), mlA, tA, nodeA, logger)
	protoB := NewProtocol(lifeguardCfg(addrB, "node-b"), mlB, tB, nodeB, logger)

	// Only start B's message loop — we don't want A to actually run rounds.
	go protoB.handleMessages(ctx)
	_ = protoA // not started; we'll invoke its method directly

	// Sanity: B's incarnation starts at 5
	infoBefore, _ := mlB.GetMember("node-b")
	if infoBefore.Incarnation != 5 {
		t.Fatalf("setup: B incarnation %d want 5", infoBefore.Incarnation)
	}

	// Action: A marks B suspect. The Buddy System should send a SUSPECT
	// directly to B's address; B's handleSuspectMsg detects "that's me"
	// and calls Defend, which bumps incarnation.
	protoA.suspectNode("node-b")

	// Wait briefly for B to process the buddy packet.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		info, _ := mlB.GetMember("node-b")
		if info.Incarnation > 5 {
			t.Logf("B defended: incarnation %d → %d ✓", 5, info.Incarnation)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	infoAfter, _ := mlB.GetMember("node-b")
	t.Fatalf("B did not defend itself after Buddy SUSPECT: incarnation still %d", infoAfter.Incarnation)
}

// TestProtocol_Lifeguard_DogpileShrinksSuspectWindow verifies that when
// multiple distinct nodes confirm a suspicion, the suspect timeout
// shrinks per the Dogpile formula and the node is declared dead faster
// than it would be under the longest (max) window.
func TestProtocol_Lifeguard_DogpileShrinksSuspectWindow(t *testing.T) {
	logger := silentLogger()

	addrA := freeUDPAddr(t)
	mlA := NewMembershipList("node-a", logger)
	nodeA := NodeInfo{ID: "node-a", Addr: addrA, State: StateAlive, Incarnation: 1}
	target := NodeInfo{ID: "target", Addr: "127.0.0.1:1", State: StateAlive, Incarnation: 1}
	mlA.AddOrUpdate(nodeA)
	mlA.AddOrUpdate(target)

	tA, err := NewTransport(addrA, 100, logger)
	if err != nil {
		t.Fatalf("transport A: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tA.Start(ctx)

	// Bracket the test around the formula's actual reach. With K=3 and the
	// Lifeguard log-formula, even many confirmations only pull the timeout
	// to ~50% of max in practical ranges. So we use a smallish max (500ms)
	// and prove the dead transition lands well below it under pressure.
	cfg := lifeguardCfg(addrA, "node-a")
	cfg.SuspectTimeoutMin = 30 * time.Millisecond
	cfg.SuspectTimeoutMax = 500 * time.Millisecond
	cfg.IndirectNodes = 3

	proto := NewProtocol(cfg, mlA, tA, nodeA, logger)

	var deadAt atomic.Int64
	go func() {
		for {
			info, _ := mlA.GetMember("target")
			if info.State == StateDead && deadAt.Load() == 0 {
				deadAt.Store(time.Now().UnixNano())
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	start := time.Now()
	proto.suspectNode("target") // self vote
	for i := 0; i < 10; i++ {
		mlA.ConfirmSuspect("target",
			"voter-"+string(rune('A'+i)),
			proto.computeSuspectTimeout,
			proto.gossipDead,
		)
	}

	// 11 votes → formula gives ~256ms (≈51% of max=500ms). Allow 800ms
	// ceiling for CI jitter, but the key invariant we test is "shorter
	// than max would have been".
	if !waitFor(func() bool { return deadAt.Load() != 0 }, 800*time.Millisecond) {
		t.Fatalf("target did not reach Dead within budget under Dogpile pressure")
	}
	elapsed := time.Duration(deadAt.Load() - start.UnixNano())
	t.Logf("Dogpile: dead in %v (max would have been %v)", elapsed, cfg.SuspectTimeoutMax)
	if elapsed >= cfg.SuspectTimeoutMax {
		t.Errorf("Dogpile failed to shrink: elapsed %v >= max %v", elapsed, cfg.SuspectTimeoutMax)
	}
}

// TestProtocol_Lifeguard_SelfAwarenessGrowsTimeout exercises the full
// runProtocolRound path: failing pings should drive noop up, which in
// turn raises the next round's pingTimeout.
func TestProtocol_Lifeguard_SelfAwarenessGrowsTimeout(t *testing.T) {
	logger := silentLogger()

	addrA := freeUDPAddr(t)
	addrSink := freeUDPAddr(t) // no listener — every ping times out

	mlA := NewMembershipList("node-a", logger)
	nodeA := NodeInfo{ID: "node-a", Addr: addrA, State: StateAlive, Incarnation: 1}
	sink := NodeInfo{ID: "sink", Addr: addrSink, State: StateAlive, Incarnation: 1}
	mlA.AddOrUpdate(nodeA)
	mlA.AddOrUpdate(sink)

	tA, err := NewTransport(addrA, 100, logger)
	if err != nil {
		t.Fatalf("transport A: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tA.Start(ctx)

	cfg := lifeguardCfg(addrA, "node-a")
	cfg.SelfAwarenessMax = 3 // small max → quick to saturate
	proto := NewProtocol(cfg, mlA, tA, nodeA, logger)

	if got := proto.selfAwareness.Score(); got != 0 {
		t.Fatalf("initial noop %d != 0", got)
	}

	// Force several failing rounds. We can't easily drive the full loop in
	// a unit test, but RecordFailure on direct timeout is what gets invoked
	// — call runProtocolRound a few times; the sink never replies.
	for i := 0; i < 5; i++ {
		proto.runProtocolRound(ctx)
	}

	score := proto.selfAwareness.Score()
	if score == 0 {
		t.Errorf("expected noop > 0 after 5 failed rounds, got 0")
	}
	if score > cfg.SelfAwarenessMax {
		t.Errorf("noop exceeded max: %d > %d", score, cfg.SelfAwarenessMax)
	}
	t.Logf("noop after failed rounds: %d (max %d)", score, cfg.SelfAwarenessMax)

	// And now the scaled timeout must reflect the elevated noop.
	scaled := proto.pingTimeout()
	if scaled <= cfg.PingTimeoutMin {
		t.Errorf("scaled ping timeout did not grow: %v <= min %v", scaled, cfg.PingTimeoutMin)
	}
}

// =====================================================================
// helpers
// =====================================================================

// waitFor polls predicate until it returns true or timeout elapses.
// Tests use this instead of fixed sleeps to stay robust under CI jitter.
func waitFor(pred func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return pred()
}
