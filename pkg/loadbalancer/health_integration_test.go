package loadbalancer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/healthcheck"
)

// hcFastConfig is the same fast-cycle config used in the healthcheck
// package's own tests, repeated here to avoid coupling test files across
// packages. Interval is short so transitions land within the test budget.
func hcFastConfig() healthcheck.Config {
	return healthcheck.Config{
		Enabled:            true,
		Interval:           20 * time.Millisecond,
		Timeout:            10 * time.Millisecond,
		Path:               "/_health",
		HealthyThreshold:   1,
		UnhealthyThreshold: 1,
		DegradedLatency:    50 * time.Millisecond,
	}
}

// TestHealthFilter_SkipsUnhealthyBackends proves the filter works
// end-to-end: 2 backends, one consistently fails its /_health probe.
// After the checker has had time to mark it Unhealthy, every request
// must land on the healthy one.
func TestHealthFilter_SkipsUnhealthyBackends(t *testing.T) {
	var goodReqs, badReqs atomic.Int32

	// "good" backend: healthy probe + normal responses
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_health" {
			w.WriteHeader(200)
			return
		}
		goodReqs.Add(1)
		fmt.Fprint(w, "from-good")
	}))
	defer good.Close()

	// "bad" backend: probe always fails (500), normal responses still 200
	// to prove the filter, not the proxy, is what skips it.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_health" {
			w.WriteHeader(500)
			return
		}
		badReqs.Add(1)
		fmt.Fprint(w, "from-bad")
	}))
	defer bad.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(makeTestNode("good", good))
	ml.AddOrUpdate(makeTestNode("bad", bad))

	hc := healthcheck.NewChecker(hcFastConfig(), logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hc.Start(ctx)

	cfg := defaultLBConfig(":0")
	lb, lbErr := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, hc, nil)
	if lbErr != nil {
		t.Fatalf("NewLoadBalancer: %v", lbErr)
	}

	// Seed the checker explicitly so we don't have to wait for the LB's
	// startup sync (which only runs inside lb.Start, not when we use
	// httptest as a stand-in handler).
	lb.getOrSyncBackends(ml.GetAliveMembers())

	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	// Wait until the checker has classified "bad" as Unhealthy. Without
	// this synchronization the first few requests could leak through
	// during the Unknown grace period.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if h, ok := hc.GetHealth("bad"); ok && h.Status == healthcheck.StatusUnhealthy {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if h, _ := hc.GetHealth("bad"); h.Status != healthcheck.StatusUnhealthy {
		t.Fatalf("'bad' never marked Unhealthy: got %v", h.Status)
	}

	// Now hammer the LB. RoundRobin would normally split 50/50; with the
	// filter, every request should hit "good".
	const N = 30
	for i := 0; i < N; i++ {
		resp, err := http.Get(lbServer.URL + "/anything")
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != "from-good" {
			t.Errorf("request %d went to bad backend: %s", i, body)
		}
	}

	if goodReqs.Load() != int32(N) {
		t.Errorf("good backend got %d requests, want %d", goodReqs.Load(), N)
	}
	if badReqs.Load() != 0 {
		t.Errorf("bad backend got %d requests (filter failed)", badReqs.Load())
	}
}

// TestHealthFilter_FallbackWhenAllUnhealthy proves graceful degradation:
// if every backend is Unhealthy, the LB must still try them rather than
// 503'ing. The reasoning is that an across-the-board health failure is
// often a checker problem (e.g. /_health endpoint broke), not a real
// outage — better to send traffic and hope.
func TestHealthFilter_FallbackWhenAllUnhealthy(t *testing.T) {
	var serviceReqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_health" {
			w.WriteHeader(500) // probe fails — node will be Unhealthy
			return
		}
		serviceReqs.Add(1)
		fmt.Fprint(w, "ok") // but actual service still works
	}))
	defer srv.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(makeTestNode("only", srv))

	hc := healthcheck.NewChecker(hcFastConfig(), logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hc.Start(ctx)

	cfg := defaultLBConfig(":0")
	lb, lbErr := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, hc, nil)
	if lbErr != nil {
		t.Fatalf("NewLoadBalancer: %v", lbErr)
	}
	lb.getOrSyncBackends(ml.GetAliveMembers()) // seed checker
	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	// Wait until the only backend is Unhealthy.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if h, ok := hc.GetHealth("only"); ok && h.Status == healthcheck.StatusUnhealthy {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if h, _ := hc.GetHealth("only"); h.Status != healthcheck.StatusUnhealthy {
		t.Fatalf("'only' never marked Unhealthy (filter has nothing to skip): got %v", h.Status)
	}

	// Send a request — fallback should send it to the (still-functional)
	// backend rather than returning 503.
	resp, err := http.Get(lbServer.URL + "/payload")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("expected 200 via fallback, got %d (body=%q)", resp.StatusCode, body)
	}
	if serviceReqs.Load() != 1 {
		t.Errorf("backend received %d service requests, want 1", serviceReqs.Load())
	}
}

// TestAdminEndpoint_HealthDump verifies /_lb/health returns the JSON view
// of the checker's state without proxying the request.
func TestAdminEndpoint_HealthDump(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_health" {
			w.WriteHeader(200)
			return
		}
		t.Errorf("admin endpoint should not have proxied: %s", r.URL.Path)
	}))
	defer srv.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(makeTestNode("only", srv))

	hc := healthcheck.NewChecker(hcFastConfig(), logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hc.Start(ctx)

	cfg := defaultLBConfig(":0")
	lb, lbErr := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, hc, nil)
	if lbErr != nil {
		t.Fatalf("NewLoadBalancer: %v", lbErr)
	}

	// Trigger pool sync so the checker learns about the target.
	lb.getOrSyncBackends(ml.GetAliveMembers())

	// Wait for at least one health entry to populate.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := hc.GetHealth("only"); ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	resp, err := http.Get(lbServer.URL + "/_lb/health")
	if err != nil {
		t.Fatalf("admin request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("admin endpoint status %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type: %q want application/json", ct)
	}

	var out map[string]healthcheck.NodeHealth
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := out["only"]; !ok {
		t.Errorf("admin response missing 'only': %+v", out)
	}
}

// TestAdminEndpoint_DisabledChecker proves /_lb/health returns a clear
// "feature off" signal rather than an empty 200 — operators need to be
// able to tell at a glance whether the checker is even running.
func TestAdminEndpoint_DisabledChecker(t *testing.T) {
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	cfg := defaultLBConfig(":0")
	lb, lbErr := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if lbErr != nil {
		t.Fatalf("NewLoadBalancer: %v", lbErr)
	}

	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	resp, err := http.Get(lbServer.URL + "/_lb/health")
	if err != nil {
		t.Fatalf("admin request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when disabled, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) == "" {
		t.Error("disabled response body is empty (caller can't tell)")
	}
}

// TestAdminEndpoint_DoesNotProxy makes sure backends never see /_lb/*
// requests — that prefix is reserved for the LB's own use.
func TestAdminEndpoint_DoesNotProxy(t *testing.T) {
	var probedBackend atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probedBackend.Store(true)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(makeTestNode("only", srv))

	cfg := defaultLBConfig(":0")
	lb, lbErr := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if lbErr != nil {
		t.Fatalf("NewLoadBalancer: %v", lbErr)
	}
	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	// /_lb/health (no checker → 503) and /_lb/whatever (404) must NOT
	// reach the backend.
	for _, path := range []string{"/_lb/health", "/_lb/anything"} {
		resp, err := http.Get(lbServer.URL + path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		resp.Body.Close()
	}
	if probedBackend.Load() {
		t.Error("/_lb/* path leaked to backend")
	}
}
