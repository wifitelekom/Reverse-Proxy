// Package metrics exposes Prometheus instrumentation for the load balancer
// and gossip subsystem.
//
// Design notes:
//
// We use a constructor + struct rather than package-level globals so:
//   - Tests can spin up multiple LBs in the same process without
//     panicking on duplicate registration.
//   - The set of metrics is explicit and discoverable (one place to look).
//   - Disabling metrics is a single nil check at every call site.
//
// Cardinality discipline: every label here has a small, bounded value
// space. We intentionally do NOT label by URL path, query string, or
// client IP — those would create unbounded cardinality and hose the
// Prometheus server. NodeID is bounded by the operator's cluster size,
// which is the unit of capacity planning anyway.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns every Prometheus collector the load balancer exposes.
// A nil *Metrics is the "feature disabled" state — callers gate every
// observation on a nil check, so the disabled path has zero overhead.
type Metrics struct {
	registry *prometheus.Registry

	// HTTP request flow ------------------------------------------------
	RequestsTotal     *prometheus.CounterVec   // labels: backend, status_class, method
	RequestDuration   *prometheus.HistogramVec // labels: backend, status_class
	RequestsDropped   *prometheus.CounterVec   // labels: reason — pre-select 5xx
	WebsocketTotal    *prometheus.CounterVec   // labels: backend
	BackendPoolSize   prometheus.Gauge         // current alive-backend count
	SelectorDecisions *prometheus.CounterVec   // labels: strategy, backend

	// Gossip flow -----------------------------------------------------
	GossipMessagesSent     *prometheus.CounterVec // labels: type
	GossipMessagesReceived *prometheus.CounterVec // labels: type
	GossipMessagesRejected *prometheus.CounterVec // labels: reason
	GossipStateTransitions *prometheus.CounterVec // labels: from, to
	GossipLifeguardNoop    prometheus.Gauge       // current self-awareness score

	// Health-check flow -----------------------------------------------
	HealthChecksTotal      *prometheus.CounterVec   // labels: result (healthy|degraded|unhealthy|error)
	HealthCheckDuration    *prometheus.HistogramVec // labels: result
	HealthStatusTransitions *prometheus.CounterVec  // labels: from, to
}

// New builds a fresh registry and the full set of collectors. Every
// collector is registered against the package-private registry so we
// can serve metrics from a known endpoint without polluting the
// Prometheus default registry (which would conflict with anything else
// in the binary that uses promauto).
func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		registry: reg,

		RequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lb",
				Name:      "requests_total",
				Help:      "Number of HTTP requests proxied, partitioned by backend / response class / method.",
			},
			[]string{"backend", "status_class", "method"},
		),
		RequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "lb",
				Name:      "request_duration_seconds",
				Help:      "End-to-end proxy latency from request receipt to backend response complete.",
				// Buckets cover sub-millisecond local calls up to several
				// seconds for slow backends — fits both web and gRPC.
				Buckets: []float64{
					0.001, 0.005, 0.01, 0.025, 0.05, 0.1,
					0.25, 0.5, 1, 2.5, 5, 10,
				},
			},
			[]string{"backend", "status_class"},
		),
		RequestsDropped: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lb",
				Name:      "requests_dropped_total",
				Help:      "Number of incoming requests dropped before any backend was selected, by reason. These are NOT counted in lb_requests_total because there is no backend label to attribute them to. Sum of (requests_total + requests_dropped_total) gives true ingress rate.",
			},
			[]string{"reason"},
		),
		WebsocketTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lb",
				Name:      "websocket_upgrades_total",
				Help:      "Number of WebSocket upgrade handshakes proxied (one per session, not per frame).",
			},
			[]string{"backend"},
		),
		BackendPoolSize: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "lb",
				Name:      "backend_pool_size",
				Help:      "Current number of alive backends known to the load balancer.",
			},
		),
		SelectorDecisions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lb",
				Name:      "selector_decisions_total",
				Help:      "Number of times the configured selector chose each backend. Useful for verifying P2C/least-conn distribution.",
			},
			[]string{"strategy", "backend"},
		),

		GossipMessagesSent: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "gossip",
				Name:      "messages_sent_total",
				Help:      "Number of SWIM messages sent, partitioned by type.",
			},
			[]string{"type"},
		),
		GossipMessagesReceived: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "gossip",
				Name:      "messages_received_total",
				Help:      "Number of SWIM messages received, partitioned by type.",
			},
			[]string{"type"},
		),
		GossipMessagesRejected: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "gossip",
				Name:      "messages_rejected_total",
				Help:      "Number of SWIM messages dropped before processing, partitioned by reason. A non-zero source_mismatch count indicates possible impersonation attempts on the gossip ring.",
			},
			[]string{"reason"},
		),
		GossipStateTransitions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "gossip",
				Name:      "state_transitions_total",
				Help:      "Node state transitions in the membership table (alive/suspect/dead).",
			},
			[]string{"from", "to"},
		),
		GossipLifeguardNoop: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "gossip",
				Name:      "lifeguard_self_awareness",
				Help:      "Current Lifeguard self-awareness (noop) score; 0 = healthy, climbing toward max under stress.",
			},
		),

		HealthChecksTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "health",
				Name:      "checks_total",
				Help:      "HTTP health probes performed, partitioned by outcome.",
			},
			[]string{"result"},
		),
		HealthCheckDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "health",
				Name:      "check_duration_seconds",
				Help:      "Latency of each HTTP health probe.",
				Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
			},
			[]string{"result"},
		),
		HealthStatusTransitions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "health",
				Name:      "status_transitions_total",
				Help:      "Backend health-status transitions; complements the gossip state machine.",
			},
			[]string{"from", "to"},
		),
	}

	reg.MustRegister(
		m.RequestsTotal,
		m.RequestDuration,
		m.RequestsDropped,
		m.WebsocketTotal,
		m.BackendPoolSize,
		m.SelectorDecisions,
		m.GossipMessagesSent,
		m.GossipMessagesReceived,
		m.GossipMessagesRejected,
		m.GossipStateTransitions,
		m.GossipLifeguardNoop,
		m.HealthChecksTotal,
		m.HealthCheckDuration,
		m.HealthStatusTransitions,
	)

	// Built-in process and Go-runtime collectors give us GC pause times,
	// open FDs, goroutine count, etc. for free. Cheap and high-value.
	reg.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)

	return m
}

// Handler returns the http.Handler that serves /metrics in the Prometheus
// exposition format. Keep it scoped to this package's registry so we
// don't expose anything the rest of the binary stuffs into the default.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		// Hide the framework-internal "Process started up" warning from
		// older Prometheus clients; modern scrapers ignore it anyway.
		EnableOpenMetrics: true,
	})
}

// Registry exposes the underlying registry for tests that want to gather
// metrics directly without going through the HTTP endpoint.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

// StatusClass converts an HTTP status code into one of the five
// well-known classes ("1xx", "2xx", ...). Cardinality-controlled —
// using the raw integer would explode label combinations.
func StatusClass(code int) string {
	switch {
	case code >= 100 && code < 200:
		return "1xx"
	case code >= 200 && code < 300:
		return "2xx"
	case code >= 300 && code < 400:
		return "3xx"
	case code >= 400 && code < 500:
		return "4xx"
	case code >= 500 && code < 600:
		return "5xx"
	default:
		return "unknown"
	}
}

// knownHTTPMethods is the allowlist of HTTP method tokens that may
// appear directly as a Prometheus `method` label. Anything outside
// this set is bucketed into "unknown" by MethodLabel — without
// the bucket, an attacker could send arbitrary tokens
// (e.g. "HACK /path HTTP/1.1") and create unbounded metric series,
// blowing up the registry.
//
// The allowlist tracks RFC 9110 §9 (the formerly RFC 7231 list) plus
// the WebDAV verbs that appear in practice. Adding a new entry is
// cheap; removing one risks losing a customer's legitimate traffic.
var knownHTTPMethods = map[string]struct{}{
	"GET":     {},
	"HEAD":    {},
	"POST":    {},
	"PUT":     {},
	"DELETE":  {},
	"PATCH":   {},
	"OPTIONS": {},
	"TRACE":   {},
	"CONNECT": {},
	// WebDAV — common enough to whitelist; remove if you prefer strict RFC 9110.
	"PROPFIND":  {},
	"PROPPATCH": {},
	"MKCOL":     {},
	"COPY":      {},
	"MOVE":      {},
	"LOCK":      {},
	"UNLOCK":    {},
}

// MethodLabel normalises an HTTP method for use as a Prometheus label.
// Methods in the IETF/IANA-curated allowlist pass through unchanged;
// anything else is bucketed into "unknown" so attacker-controlled
// request lines cannot inflate the registry's series count.
//
// Empty input also returns "unknown" — defensive against handlers
// that haven't populated Request.Method (none in our hot path do,
// but the contract is robust to misuse).
func MethodLabel(method string) string {
	if _, ok := knownHTTPMethods[method]; ok {
		return method
	}
	return "unknown"
}
