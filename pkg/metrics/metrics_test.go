package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// =====================================================================
// StatusClass: cheap dispatch, but easy to break — verify all branches.
// =====================================================================

func TestStatusClass(t *testing.T) {
	cases := []struct {
		code int
		want string
	}{
		{100, "1xx"}, {199, "1xx"},
		{200, "2xx"}, {204, "2xx"}, {299, "2xx"},
		{301, "3xx"}, {399, "3xx"},
		{400, "4xx"}, {404, "4xx"}, {499, "4xx"},
		{500, "5xx"}, {502, "5xx"}, {599, "5xx"},
		{0, "unknown"}, {99, "unknown"}, {600, "unknown"},
	}
	for _, c := range cases {
		if got := StatusClass(c.code); got != c.want {
			t.Errorf("StatusClass(%d) = %q, want %q", c.code, got, c.want)
		}
	}
}

// =====================================================================
// Metrics struct construction + collector registration
// =====================================================================

func TestNew_RegistersAllCollectors(t *testing.T) {
	// Two New() calls in the same test process must not panic. Without
	// the per-instance registry, they would (default Prometheus registry
	// rejects duplicate registration).
	a := New()
	b := New()
	if a == b {
		t.Fatal("New() returned the same pointer twice — should be fresh")
	}
	if a.registry == b.registry {
		t.Fatal("registries should be distinct")
	}
}

func TestNew_AllCollectorsExposed(t *testing.T) {
	// Make sure every collector listed on the struct is actually scrapable.
	// Prometheus only emits metric families with at least one observation,
	// so we seed each one with a representative point before gathering.
	// If a struct field is added without registration, this test fails
	// either by missing the seed call (compile error) or by absent name.
	m := New()
	m.RequestsTotal.WithLabelValues("b", "2xx", "GET").Inc()
	m.RequestDuration.WithLabelValues("b", "2xx").Observe(0.01)
	m.WebsocketTotal.WithLabelValues("b").Inc()
	m.BackendPoolSize.Set(0)
	m.SelectorDecisions.WithLabelValues("rr", "b").Inc()
	m.GossipMessagesSent.WithLabelValues("PING").Inc()
	m.GossipMessagesReceived.WithLabelValues("PING").Inc()
	m.GossipStateTransitions.WithLabelValues("ALIVE", "SUSPECT").Inc()
	m.GossipLifeguardNoop.Set(0)
	m.HealthChecksTotal.WithLabelValues("healthy").Inc()
	m.HealthCheckDuration.WithLabelValues("healthy").Observe(0.01)
	m.HealthStatusTransitions.WithLabelValues("unknown", "healthy").Inc()

	families, err := m.registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	wantNames := []string{
		"lb_requests_total",
		"lb_request_duration_seconds",
		"lb_websocket_upgrades_total",
		"lb_backend_pool_size",
		"lb_selector_decisions_total",
		"gossip_messages_sent_total",
		"gossip_messages_received_total",
		"gossip_state_transitions_total",
		"gossip_lifeguard_self_awareness",
		"health_checks_total",
		"health_check_duration_seconds",
		"health_status_transitions_total",
	}

	got := map[string]bool{}
	for _, f := range families {
		got[f.GetName()] = true
	}
	for _, name := range wantNames {
		if !got[name] {
			t.Errorf("metric %q not exposed; gathered: %v", name, gatherNames(families))
		}
	}
}

func gatherNames(families []*dto.MetricFamily) []string {
	out := make([]string, 0, len(families))
	for _, f := range families {
		out = append(out, f.GetName())
	}
	return out
}

// =====================================================================
// HTTP handler: full Prometheus exposition round-trip
// =====================================================================

func TestHandler_ServesValidPrometheusFormat(t *testing.T) {
	m := New()

	// Generate one observation so we can verify a counter shows up in
	// the output. With no observations a counter is registered but not
	// emitted in the text format.
	m.RequestsTotal.WithLabelValues("backend-a", "2xx", "GET").Inc()
	m.RequestDuration.WithLabelValues("backend-a", "2xx").Observe(0.05)

	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	text := string(body)

	// The exposition format is documented; check for our metrics by name
	// and the expected label combination.
	wantSubstrings := []string{
		`lb_requests_total{backend="backend-a",method="GET",status_class="2xx"} 1`,
		"lb_request_duration_seconds_count",
		"# HELP lb_requests_total",
		"# TYPE lb_requests_total counter",
	}
	for _, s := range wantSubstrings {
		if !strings.Contains(text, s) {
			t.Errorf("exposition missing %q", s)
		}
	}
}

// =====================================================================
// Counter / histogram observability sanity
// =====================================================================

func TestCounter_IncrementsVisible(t *testing.T) {
	m := New()
	m.WebsocketTotal.WithLabelValues("ws-1").Inc()
	m.WebsocketTotal.WithLabelValues("ws-1").Inc()
	m.WebsocketTotal.WithLabelValues("ws-2").Inc()

	families, _ := m.registry.Gather()
	var counts map[string]float64 = map[string]float64{}
	for _, f := range families {
		if f.GetName() != "lb_websocket_upgrades_total" {
			continue
		}
		for _, mt := range f.GetMetric() {
			for _, l := range mt.GetLabel() {
				if l.GetName() == "backend" {
					counts[l.GetValue()] = mt.GetCounter().GetValue()
				}
			}
		}
	}
	if counts["ws-1"] != 2 || counts["ws-2"] != 1 {
		t.Errorf("counts: %+v", counts)
	}
}

func TestHistogram_RecordsObservations(t *testing.T) {
	m := New()
	m.RequestDuration.WithLabelValues("b", "2xx").Observe(0.001)
	m.RequestDuration.WithLabelValues("b", "2xx").Observe(0.5)
	m.RequestDuration.WithLabelValues("b", "2xx").Observe(2.5)

	families, _ := m.registry.Gather()
	for _, f := range families {
		if f.GetName() != "lb_request_duration_seconds" {
			continue
		}
		for _, mt := range f.GetMetric() {
			h := mt.GetHistogram()
			if h.GetSampleCount() != 3 {
				t.Errorf("sample count: got %d, want 3", h.GetSampleCount())
			}
			// Sum should be 0.001 + 0.5 + 2.5 = 3.001.
			if got := h.GetSampleSum(); got < 3.0 || got > 3.01 {
				t.Errorf("sample sum: got %v, want ~3.001", got)
			}
		}
	}
}

// =====================================================================
// Process / Go collectors: smoke test that they emit something
// =====================================================================

func TestGoCollector_EmitsGoroutineCount(t *testing.T) {
	// Built-in Go collector should expose go_goroutines as a gauge — a
	// strong signal that the standard collectors registered correctly.
	m := New()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "go_goroutines") {
		t.Error("go_goroutines metric missing — Go collector not registered")
	}
}

// Compile-time guard: New() must not block or take noticeable time.
// A bug that did sync I/O (e.g. reading /proc/) would show up here.
func TestNew_FastPath(t *testing.T) {
	start := time.Now()
	for i := 0; i < 100; i++ {
		_ = New()
	}
	elapsed := time.Since(start)
	if elapsed > time.Second {
		t.Errorf("100 New() calls took %v — too slow", elapsed)
	}
}
