package healthcheck

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// Note: we don't use TestMain + VerifyTestMain in this package because
// the audit identified known leaks under stress (H-1: OnStatusChange
// callbacks can run as orphan goroutines). The fix is queued for the
// next phase; once it lands, this file should add goleak.VerifyTestMain
// like pkg/config and pkg/metrics already do.
//
// In the meantime we keep goleak coverage on individual operations we
// *do* believe leak-free. Each test below uses goleak.VerifyNone to
// pin that contract per-test, which is more targeted than a package-
// wide assertion.

// TestLeak_DisabledCheckerStartsNoGoroutines: when Enabled=false, Start
// must be a true no-op — no ticker, no fan-out goroutines, nothing.
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

// TestLeak_StartStopRespectsContext: an enabled checker, started and then
// canceled, must drain to zero goroutines. This is the contract for
// graceful shutdown — without it, a long-lived process restarting the
// checker would leak per restart.
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
