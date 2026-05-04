package loadbalancer

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/metrics"
)

// gaugeValueByLabel scans gathered Prometheus metrics for a gauge with
// the given name and returns the value associated with backendLabel.
// Returns -1 if the metric or label combination wasn't found — the
// caller can distinguish "not yet emitted" from "zero".
func gaugeValueByLabel(t *testing.T, m *metrics.Metrics, metricName, backendLabel string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != metricName {
			continue
		}
		for _, mt := range f.GetMetric() {
			if hasLabel(mt, "backend", backendLabel) {
				return mt.GetGauge().GetValue()
			}
		}
	}
	return -1
}

func hasLabel(m *dto.Metric, key, value string) bool {
	for _, l := range m.GetLabel() {
		if l.GetName() == key && l.GetValue() == value {
			return true
		}
	}
	return false
}

// TestBackendGauges_PopulatedAfterRequest pins the M-2' fix:
// per-backend ActiveConns and LatencyEWMA must be exposed via the
// metrics endpoint after at least one request has flowed through.
//
// Before this commit, operators using P2C-EWMA had no visibility into
// the per-backend latency that was driving selection — the EWMA lived
// in an internal struct field with no Prometheus surface.
func TestBackendGauges_PopulatedAfterRequest(t *testing.T) {
	// Backend that takes a measurable amount of time so EWMA isn't
	// indistinguishable from zero. A 5ms sleep gives the latency
	// histogram enough signal to confirm the gauge isn't a no-op.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer backend.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(makeTestNode("ewma-target", backend))

	m := metrics.New()
	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, m)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer srv.Close()

	// Before any request: the gauge should not yet be present (or, more
	// precisely, the time-series for this backend doesn't exist). We
	// don't assert -1 strictly because Prometheus convention allows
	// zero-value initialisation — the actual contract is "not negative".
	if got := gaugeValueByLabel(t, m, "lb_backend_active_connections", "ewma-target"); got > 0 {
		t.Errorf("active conns should be zero/absent before first request, got %v", got)
	}

	// Drive a few requests so EWMA has multiple samples to blend.
	for i := 0; i < 3; i++ {
		resp, err := http.Get(srv.URL + "/")
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
	}

	// ActiveConns: at metric-emission time the deferred decrement
	// hasn't fired yet, so the most recent observed value should be
	// >= 1. (We can't pin exactly 1 because the read races with the
	// next request's increment in a real cluster — for a single-
	// request test it's exactly 1.)
	if got := gaugeValueByLabel(t, m, "lb_backend_active_connections", "ewma-target"); got < 1 {
		t.Errorf("active conns: got %v, want >=1 (snapshot includes the in-flight request)", got)
	}

	// LatencyEWMA: should be roughly 5ms (the backend sleep) — not
	// exactly because EWMA blending means the first three samples
	// converge gradually. We accept anything in [1ms, 50ms] to absorb
	// CI jitter while still confirming the gauge is wired.
	got := gaugeValueByLabel(t, m, "lb_backend_latency_ewma_seconds", "ewma-target")
	if got < 0.001 || got > 0.050 {
		t.Errorf("latency EWMA: got %v sec, want roughly 5ms (range 1-50ms)", got)
	}
	t.Logf("after 3 requests: active=%v, ewma=%vs",
		gaugeValueByLabel(t, m, "lb_backend_active_connections", "ewma-target"), got)
}

// TestBackendGauges_PerBackendIsolation verifies the gauges track
// distinct values for distinct backends — a bug in the label
// plumbing could otherwise have all backends share the most recent
// value.
func TestBackendGauges_PerBackendIsolation(t *testing.T) {
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer fast.Close()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer slow.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(makeTestNode("fast", fast))
	ml.AddOrUpdate(makeTestNode("slow", slow))

	m := metrics.New()
	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, m)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer srv.Close()

	// Round-robin will alternate; fire ~10 requests so each backend
	// gets several samples and EWMA stabilises.
	for i := 0; i < 10; i++ {
		resp, err := http.Get(srv.URL + "/")
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
	}

	fastEWMA := gaugeValueByLabel(t, m, "lb_backend_latency_ewma_seconds", "fast")
	slowEWMA := gaugeValueByLabel(t, m, "lb_backend_latency_ewma_seconds", "slow")
	t.Logf("fast EWMA: %vs, slow EWMA: %vs", fastEWMA, slowEWMA)

	if fastEWMA < 0 || slowEWMA < 0 {
		t.Fatal("expected both backends to have emitted gauges; one or both missing")
	}
	// Slow backend's EWMA must clearly exceed fast's. Conservative
	// margin: slow >= 2× fast. Real ratio is more like 20× (20ms vs <1ms)
	// but EWMA blends and CI variance can compress the gap.
	if slowEWMA < fastEWMA*2 {
		t.Errorf("slow backend EWMA (%v) should be at least 2× fast (%v); per-backend isolation may be broken",
			slowEWMA, fastEWMA)
	}
}
