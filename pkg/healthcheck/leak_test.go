package healthcheck

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// TestMain enforces zero goroutine leaks across every test in this
// package. The audit's H-1 finding (unbounded OnStatusChange goroutine
// fanout) was closed in commit C; before the fix this file used
// per-test VerifyNone calls because TestMain coverage would have
// flagged the known leak as a failure. Now that the bound is in
// place — and the saturation test deterministically drains its
// blocked callbacks before returning — TestMain coverage is the
// stronger contract.
//
// goleak.VerifyTestMain runs after every test exits. Any goroutine
// traceable to this package that's still running at that point is a
// regression and fails the suite.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// TestLeak_DisabledCheckerStartsNoGoroutines is preserved for its
// targeted contract: a disabled Checker's Start must be a true no-op,
// not just "drains within budget". TestMain alone would catch a leak
// here, but per-test VerifyNone confirms the no-op happens within the
// test boundary rather than during cleanup.
func TestLeak_DisabledCheckerStartsNoGoroutines(t *testing.T) {
	defer goleak.VerifyNone(t)

	c := NewChecker(Config{}, silentLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	// Wait a tick to give any (incorrectly) launched goroutine time to
	// register with the runtime before we verify.
	time.Sleep(20 * time.Millisecond)
}

// TestLeak_StartStopRespectsContext: an enabled checker, started and
// then canceled, must drain to zero goroutines. Per-test VerifyNone
// keeps this assertion local to the canceling Window — relying on
// TestMain alone would let a delayed exit slip through.
func TestLeak_StartStopRespectsContext(t *testing.T) {
	defer goleak.VerifyNone(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()

	cfg := fastConfig("/")
	cfg.Interval = 25 * time.Millisecond
	c := NewChecker(cfg, silentLogger())
	c.UpdateTargets(map[string]string{"n": hostPort(t, srv)})

	ctx, cancel := context.WithCancel(context.Background())
	c.Start(ctx)
	time.Sleep(80 * time.Millisecond) // run a few ticks
	cancel()

	// Allow the loop's select to see ctx.Done and return. Without this
	// pause goleak might catch the goroutine still executing — it's a
	// cooperative shutdown, not an instant one.
	time.Sleep(50 * time.Millisecond)
}
