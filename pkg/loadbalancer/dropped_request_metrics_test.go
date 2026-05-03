package loadbalancer

import (
	"net/http"
	"net/http/httptest"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/metrics"
)

// =====================================================================
// Pre-select 503 instrumentation
// =====================================================================
//
// Before this commit, both early-return 503 paths in handleRequest were
// invisible to metrics:
//   - "no alive backends" — gossip empty
//   - "no backend selected" — every alive backend had a malformed addr
//
// dashboards relying on lb_requests_total under-counted the LB's true
// ingress rate. The new lb_requests_dropped_total counter (labels:
// reason) closes that gap.

// dropCount returns the value of lb_requests_dropped_total for the
// given reason label. Returns 0 if the label hasn't been observed yet.
// Reads via the registry's Gather() to mirror what Prometheus actually
// scrapes — testing against the public surface, not internal state.
func dropCount(t *testing.T, m *metrics.Metrics, reason string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != "lb_requests_dropped_total" {
			continue
		}
		for _, mt := range f.GetMetric() {
			for _, l := range mt.GetLabel() {
				if l.GetName() == "reason" && l.GetValue() == reason {
					return mt.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// TestDroppedMetric_NoAliveBackends fires a request against an LB with
// an empty membership list. The 503 must increment
// lb_requests_dropped_total{reason="no_alive_backends"}.
func TestDroppedMetric_NoAliveBackends(t *testing.T) {
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	// No AddOrUpdate — the LB sees zero alive members.

	m := metrics.New()
	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, m)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status: got %d want 503", resp.StatusCode)
	}

	if got := dropCount(t, m, "no_alive_backends"); got != 1 {
		t.Errorf("no_alive_backends counter: got %v want 1", got)
	}
	// Cross-check: the regular request counter should NOT have been
	// touched (no backend label to attribute to).
	families, _ := m.Registry().Gather()
	for _, f := range families {
		if f.GetName() == "lb_requests_total" {
			for _, mt := range f.GetMetric() {
				if mt.GetCounter().GetValue() > 0 {
					t.Errorf("lb_requests_total leaked a count for pre-select 503: %+v",
						labelValues(mt.GetLabel()))
				}
			}
		}
	}
}

// TestDroppedMetric_AllBackendsMalformed reaches the second 503 path —
// gossip says alive, but every NodeInfo has a malformed ServiceAddr so
// newBackend returns nil for each, leaving the selector with an empty
// list. Should bump no_valid_backends.
func TestDroppedMetric_AllBackendsMalformed(t *testing.T) {
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	// Two "alive" nodes whose ServiceAddr won't parse as a URL host.
	ml.AddOrUpdate(gossip.NodeInfo{
		ID: "bad-1", Addr: "127.0.0.1:9001",
		ServiceAddr: "address with spaces", // url.Parse fails
		State:       gossip.StateAlive,
		Incarnation: 1,
	})
	ml.AddOrUpdate(gossip.NodeInfo{
		ID: "bad-2", Addr: "127.0.0.1:9002",
		ServiceAddr: "another bad addr",
		State:       gossip.StateAlive,
		Incarnation: 1,
	})

	m := metrics.New()
	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, m)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status: got %d want 503", resp.StatusCode)
	}

	if got := dropCount(t, m, "no_valid_backends"); got != 1 {
		t.Errorf("no_valid_backends counter: got %v want 1", got)
	}
	if got := dropCount(t, m, "no_alive_backends"); got != 0 {
		t.Errorf("wrong reason fired: no_alive_backends=%v (should be 0)", got)
	}
}

// TestDroppedMetric_HappyPathDoesNotIncrement is the regression guard:
// successful proxied requests must not touch the dropped counter.
func TestDroppedMetric_HappyPathDoesNotIncrement(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer backend.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(makeTestNode("good", backend))

	m := metrics.New()
	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, m)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer srv.Close()

	for i := 0; i < 5; i++ {
		resp, err := http.Get(srv.URL + "/")
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
	}

	if got := dropCount(t, m, "no_alive_backends"); got != 0 {
		t.Errorf("happy-path requests bumped no_alive_backends: %v", got)
	}
	if got := dropCount(t, m, "no_valid_backends"); got != 0 {
		t.Errorf("happy-path requests bumped no_valid_backends: %v", got)
	}
}

// labelValues stringifies a metric's labels for diagnostic output.
func labelValues(labels []*dto.LabelPair) map[string]string {
	out := make(map[string]string, len(labels))
	for _, l := range labels {
		out[l.GetName()] = l.GetValue()
	}
	return out
}
