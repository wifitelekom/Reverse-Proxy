package healthcheck

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =====================================================================
// Helpers
// =====================================================================

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// fastConfig builds a Config with extremely short intervals/timeouts so
// tests run in milliseconds rather than seconds. Keep the threshold values
// small but >1 to actually exercise the threshold gating logic.
func fastConfig(path string) Config {
	return Config{
		Enabled:            true,
		Interval:           20 * time.Millisecond,
		Timeout:            10 * time.Millisecond,
		Path:               path,
		HealthyThreshold:   2,
		UnhealthyThreshold: 2,
		DegradedLatency:    5 * time.Millisecond,
	}
}

// hostPort strips http:// from an httptest.Server URL to get just "host:port".
func hostPort(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse %s: %v", srv.URL, err)
	}
	return u.Host
}

// waitFor polls predicate until true or budget elapses. Returns the
// terminal value of the predicate.
func waitFor(t *testing.T, budget time.Duration, pred func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return pred()
}

// =====================================================================
// Config validation
// =====================================================================

func TestConfig_Validate_DisabledIsAlwaysOK(t *testing.T) {
	// A zero-value config is fine as long as Enabled stays false — that's
	// the "feature off" path callers rely on.
	if err := (Config{}).Validate(); err != nil {
		t.Errorf("disabled config should validate: %v", err)
	}
}

func TestConfig_Validate_DefaultsAreValid(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	if err := cfg.Validate(); err != nil {
		t.Errorf("DefaultConfig invalid: %v", err)
	}
}

func TestConfig_Validate_RejectsBadValues(t *testing.T) {
	// Each line corrupts exactly one field — the validator must catch it.
	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"interval<=0", func(c *Config) { c.Interval = 0 }, "interval"},
		{"timeout<=0", func(c *Config) { c.Timeout = 0 }, "timeout"},
		{"timeout>=interval", func(c *Config) { c.Timeout = c.Interval }, "must be <"},
		{"unhealthy_threshold<=0", func(c *Config) { c.UnhealthyThreshold = 0 }, "unhealthy_threshold"},
		{"healthy_threshold<=0", func(c *Config) { c.HealthyThreshold = 0 }, "healthy_threshold"},
		{"empty_path", func(c *Config) { c.Path = "" }, "path"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Enabled = true
			tc.mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error missing %q: %v", tc.want, err)
			}
		})
	}
}

// =====================================================================
// Disabled checker
// =====================================================================

func TestDisabledChecker_AllNodesHealthy(t *testing.T) {
	// When disabled, IsHealthy must be true for every name — even ones
	// the checker has never heard of. This is the contract the LB relies on.
	c := NewChecker(Config{}, silentLogger())
	for _, id := range []string{"node-a", "ghost", ""} {
		if !c.IsHealthy(id) {
			t.Errorf("disabled: IsHealthy(%q)=false, want true", id)
		}
	}
}

func TestDisabledChecker_StartIsNoop(t *testing.T) {
	// Start on a disabled checker must not spin up a goroutine. We can't
	// observe goroutines directly here, but we can verify the call returns
	// promptly and no targets get probed.
	c := NewChecker(Config{}, silentLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("disabled checker should not have probed: %s", r.URL.Path)
	}))
	defer srv.Close()

	c.UpdateTargets(map[string]string{"node-a": hostPort(t, srv)})
	time.Sleep(50 * time.Millisecond)
}

// =====================================================================
// Single-check outcomes (using applyResult directly via real HTTP)
// =====================================================================

func TestCheckNode_HealthyOn200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()

	c := NewChecker(fastConfig("/"), silentLogger())
	c.UpdateTargets(map[string]string{"n": hostPort(t, srv)})
	c.checkNode(context.Background(), "n", hostPort(t, srv))

	h, ok := c.GetHealth("n")
	if !ok {
		t.Fatal("no record")
	}
	if h.Status != StatusHealthy {
		t.Errorf("got %v, want Healthy", h.Status)
	}
	if h.HTTPStatusCode != 200 {
		t.Errorf("status code: %d", h.HTTPStatusCode)
	}
}

func TestCheckNode_DegradedOn429(t *testing.T) {
	// 429 is a backpressure signal — node is up but throttling. Should be
	// classified as Degraded, NOT failure (don't yank from rotation).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
	}))
	defer srv.Close()

	c := NewChecker(fastConfig("/"), silentLogger())
	c.UpdateTargets(map[string]string{"n": hostPort(t, srv)})
	c.checkNode(context.Background(), "n", hostPort(t, srv))

	h, _ := c.GetHealth("n")
	if h.Status != StatusDegraded {
		t.Errorf("429 should be Degraded, got %v", h.Status)
	}
	if h.ConsecutiveFails != 0 {
		t.Errorf("Degraded must not increment fails, got %d", h.ConsecutiveFails)
	}
}

func TestCheckNode_DegradedOnSlow200(t *testing.T) {
	// 2xx that takes longer than DegradedLatency is Degraded — the node
	// is responsive but probably overloaded.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(8 * time.Millisecond) // > DegradedLatency=5ms
		w.WriteHeader(200)
	}))
	defer srv.Close()

	c := NewChecker(fastConfig("/"), silentLogger())
	c.UpdateTargets(map[string]string{"n": hostPort(t, srv)})
	c.checkNode(context.Background(), "n", hostPort(t, srv))

	h, _ := c.GetHealth("n")
	if h.Status != StatusDegraded {
		t.Errorf("slow 200 should be Degraded, got %v (response_time=%v)", h.Status, h.ResponseTime)
	}
}

// =====================================================================
// Threshold transitions
// =====================================================================

func TestThreshold_HealthyToUnhealthyRequiresStreak(t *testing.T) {
	// One failure shouldn't kick a node out — the whole point of the
	// threshold is to absorb transient blips.
	var status atomic.Int32
	status.Store(200)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	defer srv.Close()

	cfg := fastConfig("/")
	cfg.UnhealthyThreshold = 3
	c := NewChecker(cfg, silentLogger())
	c.UpdateTargets(map[string]string{"n": hostPort(t, srv)})
	addr := hostPort(t, srv)

	// First a success to set Healthy.
	c.checkNode(context.Background(), "n", addr)
	if h, _ := c.GetHealth("n"); h.Status != StatusHealthy {
		t.Fatalf("setup: expected Healthy, got %v", h.Status)
	}

	// Now flip to failures.
	status.Store(500)
	c.checkNode(context.Background(), "n", addr)
	if h, _ := c.GetHealth("n"); h.Status == StatusUnhealthy {
		t.Errorf("after 1 failure (threshold=3) status is already Unhealthy")
	}
	c.checkNode(context.Background(), "n", addr)
	if h, _ := c.GetHealth("n"); h.Status == StatusUnhealthy {
		t.Errorf("after 2 failures (threshold=3) status is already Unhealthy")
	}
	c.checkNode(context.Background(), "n", addr)
	if h, _ := c.GetHealth("n"); h.Status != StatusUnhealthy {
		t.Errorf("after 3 failures status should be Unhealthy, got %v", h.Status)
	}
}

func TestThreshold_UnhealthyToHealthyRequiresStreak(t *testing.T) {
	// Recovery is gated by HealthyThreshold consecutive successes — a
	// flapping node won't be eagerly returned to rotation.
	var status atomic.Int32
	status.Store(500)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	defer srv.Close()

	cfg := fastConfig("/")
	cfg.UnhealthyThreshold = 1 // get unhealthy quickly
	cfg.HealthyThreshold = 3
	c := NewChecker(cfg, silentLogger())
	c.UpdateTargets(map[string]string{"n": hostPort(t, srv)})
	addr := hostPort(t, srv)

	// One failure to enter Unhealthy (threshold=1).
	c.checkNode(context.Background(), "n", addr)
	if h, _ := c.GetHealth("n"); h.Status != StatusUnhealthy {
		t.Fatalf("setup: expected Unhealthy, got %v", h.Status)
	}

	// Recovery: 2 successes shouldn't flip yet (threshold=3).
	status.Store(200)
	c.checkNode(context.Background(), "n", addr)
	if h, _ := c.GetHealth("n"); h.Status == StatusHealthy {
		t.Errorf("recovered after 1 success — too eager")
	}
	c.checkNode(context.Background(), "n", addr)
	if h, _ := c.GetHealth("n"); h.Status == StatusHealthy {
		t.Errorf("recovered after 2 successes — too eager (threshold=3)")
	}
	// Third success: should flip.
	c.checkNode(context.Background(), "n", addr)
	if h, _ := c.GetHealth("n"); h.Status != StatusHealthy {
		t.Errorf("after 3 successes expected Healthy, got %v", h.Status)
	}
}

func TestThreshold_FailureBreaksRecoveryStreak(t *testing.T) {
	// A single failure mid-recovery must reset ConsecutiveSuccess so the
	// node has to start the streak over.
	var status atomic.Int32
	status.Store(500)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	defer srv.Close()

	cfg := fastConfig("/")
	cfg.UnhealthyThreshold = 1
	cfg.HealthyThreshold = 3
	c := NewChecker(cfg, silentLogger())
	c.UpdateTargets(map[string]string{"n": hostPort(t, srv)})
	addr := hostPort(t, srv)

	c.checkNode(context.Background(), "n", addr) // Unhealthy
	status.Store(200)
	c.checkNode(context.Background(), "n", addr) // succ #1
	c.checkNode(context.Background(), "n", addr) // succ #2
	status.Store(500)
	c.checkNode(context.Background(), "n", addr) // failure → reset
	status.Store(200)
	c.checkNode(context.Background(), "n", addr) // succ #1 again

	if h, _ := c.GetHealth("n"); h.Status == StatusHealthy {
		t.Errorf("recovery not reset after failure: status=%v conSucc=%d", h.Status, h.ConsecutiveSuccess)
	}
}

// =====================================================================
// IsHealthy contract
// =====================================================================

func TestIsHealthy_UnknownNodeIDReturnsTrue(t *testing.T) {
	// LB might call IsHealthy on a node before UpdateTargets sees it.
	// Returning false there would 503 perfectly good gossip-alive nodes —
	// the safer default is true (let gossip's view through).
	c := NewChecker(fastConfig("/"), silentLogger())
	if !c.IsHealthy("never-seen") {
		t.Error("unknown nodeID should return true (defer to gossip)")
	}
}

func TestIsHealthy_UnknownStatusReturnsTrue(t *testing.T) {
	// A target that's been added but not yet probed sits in StatusUnknown;
	// it must be eligible during this grace period.
	c := NewChecker(fastConfig("/"), silentLogger())
	c.UpdateTargets(map[string]string{"n": "127.0.0.1:1"})
	if !c.IsHealthy("n") {
		t.Error("Unknown status must be eligible (grace period)")
	}
}

func TestIsHealthy_DegradedReturnsTrue(t *testing.T) {
	// Degraded backends still take traffic — they're slow, not broken.
	c := NewChecker(fastConfig("/"), silentLogger())
	c.UpdateTargets(map[string]string{"n": "127.0.0.1:1"})
	c.applyResult("n", time.Second, 200, false, true) // force Degraded
	if !c.IsHealthy("n") {
		t.Error("Degraded should be eligible")
	}
}

func TestIsHealthy_UnhealthyReturnsFalse(t *testing.T) {
	c := NewChecker(fastConfig("/"), silentLogger())
	c.UpdateTargets(map[string]string{"n": "127.0.0.1:1"})
	// Force Unhealthy by exceeding the threshold.
	for i := 0; i < c.config.UnhealthyThreshold; i++ {
		c.applyResult("n", time.Second, 500, false, false)
	}
	if c.IsHealthy("n") {
		t.Error("Unhealthy must be ineligible")
	}
}

// =====================================================================
// UpdateTargets dynamics
// =====================================================================

func TestUpdateTargets_AddsAndRemoves(t *testing.T) {
	c := NewChecker(fastConfig("/"), silentLogger())

	c.UpdateTargets(map[string]string{"a": "x:1", "b": "x:2"})
	if all := c.GetAllHealth(); len(all) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(all))
	}

	// Remove "a", add "c".
	c.UpdateTargets(map[string]string{"b": "x:2", "c": "x:3"})
	all := c.GetAllHealth()
	if _, has := all["a"]; has {
		t.Error("removed node 'a' is still tracked")
	}
	if _, has := all["c"]; !has {
		t.Error("new node 'c' missing")
	}
	if len(all) != 2 {
		t.Errorf("expected 2 entries, got %d", len(all))
	}
}

func TestUpdateTargets_PreservesExistingStatus(t *testing.T) {
	// Re-stating an existing target shouldn't reset its status — health
	// state must survive gossip churn that doesn't actually change anything.
	c := NewChecker(fastConfig("/"), silentLogger())
	c.UpdateTargets(map[string]string{"n": "x:1"})
	c.applyResult("n", time.Millisecond, 200, true, false) // → Healthy

	c.UpdateTargets(map[string]string{"n": "x:1", "m": "x:2"})

	h, _ := c.GetHealth("n")
	if h.Status != StatusHealthy {
		t.Errorf("status reset on UpdateTargets: %v", h.Status)
	}
}

// =====================================================================
// Status change callback
// =====================================================================

func TestOnStatusChange_FiresOnTransition(t *testing.T) {
	c := NewChecker(fastConfig("/"), silentLogger())
	c.UpdateTargets(map[string]string{"n": "x:1"})

	type evt struct{ old, new Status }
	ch := make(chan evt, 4)
	c.SetOnStatusChange(func(_ string, old, new Status) {
		ch <- evt{old, new}
	})

	// Unknown → Healthy
	c.applyResult("n", time.Millisecond, 200, true, false)

	select {
	case e := <-ch:
		if e.old != StatusUnknown || e.new != StatusHealthy {
			t.Errorf("transition: %v → %v (want Unknown → Healthy)", e.old, e.new)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("callback never fired")
	}
}

func TestOnStatusChange_NoFireOnSameStatus(t *testing.T) {
	// Repeated identical readings shouldn't spam the callback.
	c := NewChecker(fastConfig("/"), silentLogger())
	c.UpdateTargets(map[string]string{"n": "x:1"})

	var hits atomic.Int32
	c.SetOnStatusChange(func(_ string, _, _ Status) { hits.Add(1) })

	c.applyResult("n", time.Millisecond, 200, true, false) // Unknown→Healthy: fires
	c.applyResult("n", time.Millisecond, 200, true, false) // Healthy→Healthy: silent
	c.applyResult("n", time.Millisecond, 200, true, false) // Healthy→Healthy: silent

	time.Sleep(50 * time.Millisecond)
	if got := hits.Load(); got != 1 {
		t.Errorf("callback fired %d times, want exactly 1", got)
	}
}

// =====================================================================
// End-to-end: Start runs the loop
// =====================================================================

func TestStart_PeriodicallyChecks(t *testing.T) {
	var probes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	cfg := fastConfig("/")
	cfg.Interval = 25 * time.Millisecond
	c := NewChecker(cfg, silentLogger())
	c.UpdateTargets(map[string]string{"n": hostPort(t, srv)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	// Within ~120ms the ticker should fire 3-5 times.
	if !waitFor(t, 200*time.Millisecond, func() bool { return probes.Load() >= 3 }) {
		t.Fatalf("only %d probes in 200ms — ticker not running?", probes.Load())
	}
}

func TestStart_StopsOnContextCancel(t *testing.T) {
	var probes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	cfg := fastConfig("/")
	cfg.Interval = 15 * time.Millisecond
	c := NewChecker(cfg, silentLogger())
	c.UpdateTargets(map[string]string{"n": hostPort(t, srv)})

	ctx, cancel := context.WithCancel(context.Background())
	c.Start(ctx)
	time.Sleep(60 * time.Millisecond)
	cancel()

	frozen := probes.Load()
	time.Sleep(80 * time.Millisecond)
	after := probes.Load()
	// Allow at most 1 extra probe in flight when cancel landed.
	if after-frozen > 1 {
		t.Errorf("probes continued after cancel: %d → %d", frozen, after)
	}
}

// =====================================================================
// Concurrent UpdateTargets vs check loop
// =====================================================================

func TestConcurrent_NoRaceBetweenUpdateAndChecks(t *testing.T) {
	// Hammer UpdateTargets while checks run. The race detector (go test
	// -race) is the real assertion here; we also verify the loop survives.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()

	c := NewChecker(fastConfig("/"), silentLogger())
	addr := hostPort(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(150 * time.Millisecond)
			n := 0
			for time.Now().Before(deadline) {
				targets := map[string]string{}
				for j := 0; j < (n%5)+1; j++ {
					targets[string(rune('a'+j))] = addr
				}
				c.UpdateTargets(targets)
				n++
			}
		}()
	}
	wg.Wait()
}
