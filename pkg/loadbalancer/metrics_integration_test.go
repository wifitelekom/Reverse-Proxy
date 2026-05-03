package loadbalancer

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/metrics"
)

// =====================================================================
// /_lb/metrics admin endpoint
// =====================================================================

func TestAdminMetricsEndpoint_ServesPrometheus(t *testing.T) {
	// With metrics attached, /_lb/metrics returns text/plain Prometheus
	// exposition. We verify by scraping for a known metric name.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(makeTestNode("only", backend))

	m := metrics.New()
	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, m)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}

	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	// Drive one request through so RequestsTotal has a non-zero value.
	resp, err := http.Get(lbServer.URL + "/")
	if err != nil {
		t.Fatalf("warmup: %v", err)
	}
	resp.Body.Close()

	// Now scrape.
	scrape, err := http.Get(lbServer.URL + "/_lb/metrics")
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer scrape.Body.Close()
	if scrape.StatusCode != 200 {
		t.Fatalf("scrape status %d", scrape.StatusCode)
	}
	body, _ := io.ReadAll(scrape.Body)
	bodyStr := string(body)

	// Look for our metrics by name. The exact label set depends on the
	// runtime backend ID, so we just check name + a "1" count.
	for _, want := range []string{
		"lb_requests_total{",
		"lb_request_duration_seconds_count{",
		"lb_backend_pool_size",
	} {
		if !strings.Contains(bodyStr, want) {
			t.Errorf("scrape missing %q", want)
		}
	}
}

func TestAdminMetricsEndpoint_DisabledReturns503(t *testing.T) {
	// With metrics nil, /_lb/metrics must respond 503 with a clear marker —
	// matching the disabled-checker pattern at /_lb/health.
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/_lb/metrics")
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "metrics_disabled") {
		t.Errorf("body missing marker: %q", body)
	}
}

func TestAdminMetricsEndpoint_DoesNotProxyToBackend(t *testing.T) {
	// /_lb/metrics is reserved — even with a backend listening, the
	// request must not be forwarded.
	var leaked atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked.Store(true)
		w.WriteHeader(200)
	}))
	defer backend.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(makeTestNode("only", backend))

	m := metrics.New()
	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, m)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer srv.Close()

	resp, _ := http.Get(srv.URL + "/_lb/metrics")
	if resp != nil {
		resp.Body.Close()
	}
	if leaked.Load() {
		t.Error("/_lb/metrics leaked to backend")
	}
}

// =====================================================================
// Admin rate limiter
// =====================================================================

func TestAdminRateLimiter_429AfterBurstExhausted(t *testing.T) {
	// With metrics enabled, the LB sets up a token-bucket limiter on
	// admin paths. Hammer past the burst capacity and verify some
	// requests come back as 429.
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	m := metrics.New()
	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, m)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer srv.Close()

	// Burst is AdminRateLimitBurst (20); fire 50 in a tight loop. With
	// rate=10rps, refill in this window is small, so we expect ~30 of
	// them to be rate-limited.
	const total = 50
	codes := map[int]int{}
	for i := 0; i < total; i++ {
		resp, err := http.Get(srv.URL + "/_lb/metrics")
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
		codes[resp.StatusCode]++
	}
	if codes[429] == 0 {
		t.Errorf("rate limiter never engaged; codes=%v", codes)
	}
	// At least the burst (20) should have made it through with 200.
	if codes[200] < AdminRateLimitBurst-2 {
		t.Errorf("burst undercounted: 200=%d, want >= %d", codes[200], AdminRateLimitBurst-2)
	}
	t.Logf("admin scrape distribution under flood: %v", codes)
}

func TestAdminRateLimiter_OffWhenMetricsDisabled(t *testing.T) {
	// Without metrics, no limiter should be installed. A flood of
	// /_lb/health requests should NOT 429 — they all return 503 (because
	// health-check is also disabled in this test).
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil) // metrics nil
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer srv.Close()

	codes := map[int]int{}
	for i := 0; i < 100; i++ {
		resp, err := http.Get(srv.URL + "/_lb/health")
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
		codes[resp.StatusCode]++
	}
	if codes[429] != 0 {
		t.Errorf("limiter active when metrics disabled: codes=%v", codes)
	}
}

func TestAdminRateLimiter_RecoversAfterCooldown(t *testing.T) {
	// Drain the bucket, wait long enough for the bucket to refill,
	// confirm we can scrape again. This catches a limiter that's stuck.
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	m := metrics.New()
	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, m)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer srv.Close()

	// Drain.
	for i := 0; i < AdminRateLimitBurst+5; i++ {
		http.Get(srv.URL + "/_lb/metrics")
	}

	// At AdminRateLimitRPS=10, even one token should refill in 100ms.
	// Wait a bit longer for headroom.
	time.Sleep(300 * time.Millisecond)

	resp, err := http.Get(srv.URL + "/_lb/metrics")
	if err != nil {
		t.Fatalf("post-cooldown: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 429 {
		t.Error("limiter still throttling after cooldown — bucket stuck?")
	}
}
