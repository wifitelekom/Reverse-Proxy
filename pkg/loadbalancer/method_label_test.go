package loadbalancer

import (
	"net/http"
	"net/http/httptest"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/metrics"
)

// TestRequestMetric_BogusMethodBucketsToUnknown is the integration-level
// proof for M-1' (HTTP method label cardinality bound).
//
// Before the fix, an attacker sending request lines like
// `HACK /path HTTP/1.1` or `\xFF /path` would create one new
// lb_requests_total time-series per unique method, exhausting the
// metrics registry. With MethodLabel in place, every non-allowlisted
// token collapses into a single `method="unknown"` bucket.
func TestRequestMetric_BogusMethodBucketsToUnknown(t *testing.T) {
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

	// Fire one request per "method", spanning real and bogus tokens.
	// Each unique bogus value would create its own time-series before the fix.
	bogusMethods := []string{"HACK", "PROBE", "GETX", "Z", "FUZZ"}
	allowedMethods := []string{"GET", "POST", "DELETE"}

	for _, m := range bogusMethods {
		req, _ := http.NewRequest(m, srv.URL+"/", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			// Some methods (e.g. unfamiliar ones) may be rejected by the
			// stdlib client; fall through and continue. We're testing the
			// metric path, which only fires when the request actually
			// reaches handleRequest — failed dials don't count.
			continue
		}
		resp.Body.Close()
	}
	for _, m := range allowedMethods {
		req, _ := http.NewRequest(m, srv.URL+"/", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("legitimate %s request failed: %v", m, err)
		}
		resp.Body.Close()
	}

	// Gather and inspect the lb_requests_total time-series.
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	var requestsTotal *dto.MetricFamily
	for _, f := range families {
		if f.GetName() == "lb_requests_total" {
			requestsTotal = f
			break
		}
	}
	if requestsTotal == nil {
		t.Fatal("lb_requests_total not exposed — instrumentation regressed?")
	}

	// Collect every distinct method label seen in the registry.
	seenMethods := make(map[string]bool)
	for _, mt := range requestsTotal.GetMetric() {
		for _, l := range mt.GetLabel() {
			if l.GetName() == "method" {
				seenMethods[l.GetValue()] = true
			}
		}
	}

	// The labels we expect: the legitimate methods that got through,
	// plus exactly ONE "unknown" bucket for the bogus ones.
	if !seenMethods["unknown"] {
		t.Errorf("expected an 'unknown' method bucket from bogus requests; saw: %v", keys(seenMethods))
	}
	for _, m := range allowedMethods {
		if !seenMethods[m] {
			t.Errorf("expected legitimate method %q to pass through; saw: %v", m, keys(seenMethods))
		}
	}
	// Critically: NONE of the bogus tokens should appear as their own label.
	for _, m := range bogusMethods {
		if seenMethods[m] {
			t.Errorf("bogus method %q leaked into the registry as its own series", m)
		}
	}
}

// keys is a small helper for diagnostic logs.
func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// Stable order helps when reading a failure message.
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}
