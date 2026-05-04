package loadbalancer

import (
	"bufio"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/healthcheck"
	"p2p-loadbalancer/pkg/metrics"
	"p2p-loadbalancer/pkg/types"

	"golang.org/x/time/rate"
)

// Backend represents one backend node from the load balancer's perspective.
// The struct is persistent: ActiveConns, LatencyEWMA, and the proxy
// instance all survive across requests so connection counts and latency
// EWMA aren't lost between calls.
//
// LatencyEWMA holds the exponentially-weighted moving average of recent
// response times in nanoseconds, updated atomically by RecordLatency. It
// is read by latency-aware selectors (P2CEWMA). A value of 0 means "no
// measurements yet" — selectors should treat that as missing data rather
// than as zero latency.
//
// FIELD ORDER IS LOAD-BEARING. The sync/atomic package documents that
// 64-bit atomics on 32-bit ARM/x86 require their address to be 8-byte
// aligned, otherwise atomic.LoadInt64 / CompareAndSwapInt64 panic. Go
// guarantees the first field of a heap-allocated struct is 8-byte
// aligned, so by placing LatencyEWMA at offset 0 we guarantee correctness
// across every supported GOARCH. ActiveConns (int32) is then placed
// adjacent — being only 4-byte-aligned itself, it can sit anywhere
// without hazard.
//
// Future maintainers: do NOT reorder these fields without also updating
// TestBackend_LatencyEWMAAtOffsetZero, which pins this contract via
// unsafe.Offsetof.
type Backend struct {
	LatencyEWMA int64 // atomic — EWMA response time in nanoseconds (must remain at offset 0)
	ActiveConns int32 // atomic — concurrent HTTP request count
	URL         *url.URL
	NodeID      string
	proxy       *httputil.ReverseProxy
}

// ewmaAlpha is the smoothing factor for the latency EWMA. α=0.5 weights
// the latest sample equally with the entire history, which gives the
// metric enough responsiveness to react to a degraded backend within a
// few requests without overreacting to a single slow tail.
const ewmaAlpha = 0.5

// RecordLatency updates the backend's EWMA latency in nanoseconds.
//
// First sample initializes the EWMA directly. Subsequent samples blend
// with the existing value:  ewma = α*new + (1-α)*old.
//
// CAS loop handles concurrent writers without locking; reads via
// atomic.LoadInt64 are race-free against this writer.
func (b *Backend) RecordLatency(d time.Duration) {
	ns := d.Nanoseconds()
	for {
		old := atomic.LoadInt64(&b.LatencyEWMA)
		var next int64
		if old == 0 {
			next = ns
		} else {
			next = int64(ewmaAlpha*float64(ns) + (1-ewmaAlpha)*float64(old))
		}
		if atomic.CompareAndSwapInt64(&b.LatencyEWMA, old, next) {
			return
		}
	}
}

// Selector is the interface every load-balancing strategy implements.
// RoundRobin ve LeastConnections bu interface'i uygular.
type Selector interface {
	Select(backends []*Backend) *Backend
}

// adminPathPrefix marks routes the LB itself answers (rather than proxying
// to a backend). Operators must avoid colliding paths in their backends.
const adminPathPrefix = "/_lb/"

// LoadBalancer is an HTTP reverse proxy backed by gossip membership.
type LoadBalancer struct {
	config      types.LBConfig
	membership  *gossip.MembershipList
	selector    Selector
	server      *http.Server
	logger      *slog.Logger
	backendPool map[string]*Backend // NodeID → persistent Backend instance
	sortedCache []*Backend          // pre-sorted list; rebuilt when membership changes
	poolMu      sync.RWMutex        // guards backendPool + sortedCache

	// healthChecker is nil when health checks are disabled. The hot path
	// checks for nil rather than calling a noop method to keep overhead
	// at zero for the common case.
	healthChecker *healthcheck.Checker

	// backendTLS is the *tls.Config used for LB→backend HTTPS, or nil if
	// backend TLS is disabled. Built once at LoadBalancer construction so
	// every newBackend reuses the same config (and same root pool).
	backendTLS *tls.Config

	// frontendCertManager is nil when frontend TLS is off. Owns the live
	// certificate served to clients and supports atomic hot-reload.
	frontendCertManager *CertManager

	// metrics is nil when Prometheus instrumentation is disabled. Every
	// observation is gated on a nil check so the disabled path stays
	// allocation-free.
	metrics *metrics.Metrics

	// adminLimiter throttles /_lb/* admin endpoints to keep monitoring
	// scrapers from competing with proxied traffic. Nil disables the
	// limiter (the historical default).
	adminLimiter *rate.Limiter

	// adminAllowedNets is the parsed CIDR allowlist for /_lb/* access.
	// Empty slice means "no CIDR restriction"; nil and empty are
	// equivalent. Built once at construction so the request path stays
	// allocation-free.
	adminAllowedNets []*net.IPNet

	// adminBearerToken is the optional shared secret expected in the
	// Authorization header on admin requests. Empty string means
	// "no token check". Compared in constant time to defeat timing
	// oracles.
	adminBearerToken string
}

// AdminRateLimit are the defaults applied when the LB enables rate
// limiting on admin endpoints. Generous enough that any sane Prometheus
// or health-monitoring scrape (every few seconds) sails through, tight
// enough that a runaway client can't pin a CPU.
const (
	AdminRateLimitRPS   = 10
	AdminRateLimitBurst = 20
)

// statusRecordingWriter wraps an http.ResponseWriter to capture the status
// code written by the upstream backend (via httputil.ReverseProxy). The
// stdlib doesn't expose the chosen status, so we observe it ourselves.
//
// Hijack passthrough is mandatory: WebSocket upgrades use Hijack, and a
// wrapper that doesn't implement http.Hijacker would silently break them.
// The same goes for Flusher (gRPC streaming) — without it, FlushInterval
// has nothing to call into.
type statusRecordingWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusRecordingWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status = code
		w.wrote = true
	}
	w.ResponseWriter.WriteHeader(code)
}

// Write captures the implicit 200 case: handlers that just call Write
// without WriteHeader implicitly send 200 OK.
func (w *statusRecordingWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.status = http.StatusOK
		w.wrote = true
	}
	return w.ResponseWriter.Write(b)
}

// Flush, Hijack: pass through so streaming and WebSocket keep working.
func (w *statusRecordingWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusRecordingWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errHijackUnsupported
}

// NewLoadBalancer creates a new LoadBalancer.
//
// healthChecker may be nil — that's the "feature disabled" mode and behaves
// exactly like the historical proxy. When non-nil, the LB filters backends
// through it before each Select call and exposes /_lb/health for observability.
//
// TLS configuration is read from cfg.TLS and cfg.BackendTLS. NewLoadBalancer
// returns an error if either is enabled but malformed (bad cert files,
// missing CAs, etc.) — failing fast at startup is preferable to a half-
// configured proxy that 502s on the first handshake.
func NewLoadBalancer(
	cfg types.LBConfig,
	ml *gossip.MembershipList,
	selector Selector,
	logger *slog.Logger,
	healthChecker *healthcheck.Checker,
	m *metrics.Metrics,
) (*LoadBalancer, error) {
	lb := &LoadBalancer{
		config:        cfg,
		membership:    ml,
		selector:      selector,
		logger:        logger,
		backendPool:   make(map[string]*Backend),
		healthChecker: healthChecker,
		metrics:       m,
	}
	// Admin endpoints are always rate-limited when the metrics package
	// is the consumer; otherwise we'd let a misbehaving scraper steal
	// goroutines from real traffic. The limit applies to /_lb/* only.
	if m != nil {
		lb.adminLimiter = rate.NewLimiter(rate.Limit(AdminRateLimitRPS), AdminRateLimitBurst)
	}

	lb.server = &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      http.HandlerFunc(lb.handleRequest),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Frontend TLS setup. Build the cert manager + tls.Config eagerly so
	// startup fails immediately on bad cert files.
	if cfg.TLS.Enabled {
		cm, err := NewCertManager(cfg.TLS.CertFile, cfg.TLS.KeyFile, logger)
		if err != nil {
			return nil, fmt.Errorf("frontend TLS: %w", err)
		}
		tlsCfg, err := BuildTLSConfig(cfg.TLS, cm)
		if err != nil {
			return nil, fmt.Errorf("frontend TLS: %w", err)
		}
		lb.server.TLSConfig = tlsCfg
		lb.frontendCertManager = cm
	}

	// Backend TLS setup — independent from frontend.
	if cfg.BackendTLS.Enabled {
		btls, err := BuildBackendTLSConfig(cfg.BackendTLS)
		if err != nil {
			return nil, fmt.Errorf("backend TLS: %w", err)
		}
		lb.backendTLS = btls
		if cfg.BackendTLS.SkipVerify {
			logger.Warn("backend TLS configured with InsecureSkipVerify — DO NOT USE IN PRODUCTION")
		}
	}

	// Admin access control. Pre-parse CIDRs so a malformed entry fails
	// startup rather than silently disabling the gate (an empty parsed
	// list looks the same as "feature off"). An operator who typos a
	// CIDR deserves a loud error, not a security regression.
	for _, raw := range cfg.Admin.AllowCIDRs {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		_, n, err := net.ParseCIDR(raw)
		if err != nil {
			return nil, fmt.Errorf("admin: bad CIDR %q: %w", raw, err)
		}
		lb.adminAllowedNets = append(lb.adminAllowedNets, n)
	}
	lb.adminBearerToken = cfg.Admin.BearerToken
	if len(lb.adminAllowedNets) > 0 || lb.adminBearerToken != "" {
		logger.Info("admin endpoints access-controlled",
			"cidr_rules", len(lb.adminAllowedNets),
			"bearer_token", lb.adminBearerToken != "")
	}

	return lb, nil
}

// Start launches the HTTP server — blocking.
// The server graceful-shuts down when ctx is canceled.
//
// When health-checking is enabled the LB also runs a low-frequency
// background refresh that resyncs the checker's target list from gossip
// membership. Without this, a checker only learns about backends through
// the per-request slow path of getOrSyncBackends — meaning a quiet LB
// (no traffic) would never start probing newly-joined nodes.
//
// When TLS is enabled, ListenAndServeTLS is used instead. Cert/key paths
// are passed empty because the *tls.Config's GetCertificate callback
// supplies them — that callback reads from the CertManager so hot-reload
// works.
func (lb *LoadBalancer) Start(ctx context.Context) error {
	// shutdownBudget is the time window we give in-flight requests to
	// finish during graceful shutdown. The previous hardcoded 5s was
	// silently truncating gRPC server-streaming RPCs and WebSocket
	// sessions; honoring the configured value lets operators tune the
	// trade-off between fast restarts and avoiding mid-stream tears.
	//
	// Defensive fallback: a hand-constructed types.LBConfig (one not
	// run through pkg/config.ValidateLB) might leave this zero. We pick
	// a sane default rather than panicking — context.WithTimeout(0)
	// would cancel immediately and silently skip every in-flight request.
	shutdownBudget := lb.config.ShutdownTimeout
	if shutdownBudget <= 0 {
		shutdownBudget = 30 * time.Second
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownBudget)
		defer cancel()
		lb.server.Shutdown(shutdownCtx)
	}()

	if lb.healthChecker != nil {
		lb.getOrSyncBackends(lb.membership.GetAliveMembers())
		go lb.refreshTargetsLoop(ctx)
	}

	// Auto-reload runs only when TLS is enabled and the operator opted in.
	if lb.frontendCertManager != nil && lb.config.TLS.AutoReload {
		lb.frontendCertManager.StartAutoReload(ctx, lb.config.TLS.ReloadInterval)
		lb.logger.Info("TLS auto-reload enabled", "interval", lb.config.TLS.ReloadInterval)
	}

	lb.logger.Info("load balancer starting",
		"addr", lb.config.ListenAddr,
		"strategy", lb.config.Strategy,
		"tls", lb.config.TLS.Enabled,
	)

	var err error
	if lb.config.TLS.Enabled {
		// Empty cert/key paths: tls.Config.GetCertificate handles loading.
		err = lb.server.ListenAndServeTLS("", "")
	} else {
		err = lb.server.ListenAndServe()
	}
	if err != http.ErrServerClosed {
		return err
	}
	return nil
}

// refreshTargetsLoop periodically resyncs the backend pool from gossip
// membership. Only runs when a health checker is attached. Tick is 1s,
// chosen to be short enough that a newly-joined backend is probed within
// one health-check interval, and long enough that the lock contention on
// poolMu is negligible.
func (lb *LoadBalancer) refreshTargetsLoop(ctx context.Context) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			lb.getOrSyncBackends(lb.membership.GetAliveMembers())
		}
	}
}

// handleRequest processes each incoming HTTP request.
// Her request'te membership listesi taze okunur, backend havuzu senkronize edilir.
func (lb *LoadBalancer) handleRequest(w http.ResponseWriter, r *http.Request) {
	// Admin endpoints take priority over proxying. The /_lb/ prefix is
	// reserved for the LB itself; backends must not collide.
	if strings.HasPrefix(r.URL.Path, adminPathPrefix) {
		lb.handleAdmin(w, r)
		return
	}

	aliveNodes := lb.membership.GetAliveMembers()
	if len(aliveNodes) == 0 {
		lb.logger.Error("no healthy backends available")
		// These pre-select 503s aren't counted in lb_requests_total
		// (no backend label). Track them in a separate counter so
		// dashboards can see the true ingress rate.
		if lb.metrics != nil {
			lb.metrics.RequestsDropped.WithLabelValues("no_alive_backends").Inc()
		}
		http.Error(w, "no healthy backends available", http.StatusServiceUnavailable)
		return
	}

	// Pull from the persistent backend pool — ActiveConns is preserved.
	backends := lb.getOrSyncBackends(aliveNodes)

	// Health filter: if a checker is attached, drop Unhealthy backends.
	// Graceful degradation: if the filter empties the list (e.g. all checks
	// transiently failing), fall back to the gossip-alive set rather than
	// 503'ing — better to send to a possibly-broken backend than to nothing.
	if lb.healthChecker != nil {
		filtered := lb.filterHealthy(backends)
		if len(filtered) == 0 {
			lb.logger.Warn("no healthy backends — falling back to gossip-alive set",
				"gossip_alive", len(backends))
		} else {
			backends = filtered
		}
	}

	selected := lb.selector.Select(backends)
	if selected == nil {
		// Reachable when every alive NodeInfo has a malformed
		// ServiceAddr (newBackend returned nil for each), so the pool
		// is empty even though gossip says alive. Distinct enough from
		// "no alive backends" to warrant its own reason label.
		if lb.metrics != nil {
			lb.metrics.RequestsDropped.WithLabelValues("no_valid_backends").Inc()
		}
		http.Error(w, "no backend selected", http.StatusServiceUnavailable)
		return
	}

	atomic.AddInt32(&selected.ActiveConns, 1)
	defer atomic.AddInt32(&selected.ActiveConns, -1)

	if lb.metrics != nil {
		lb.metrics.SelectorDecisions.WithLabelValues(lb.config.Strategy, selected.NodeID).Inc()
	}

	// Per-request INFO logging is intentionally suppressed: metrics
	// (RequestsTotal / RequestDuration) carry the same signal at far
	// lower cost and without flooding logs at high RPS. Operators who
	// need per-request visibility should use access logs at the edge or
	// switch the level to Debug.
	lb.logger.Debug("routing request",
		"backend", selected.NodeID,
		"active_conns", atomic.LoadInt32(&selected.ActiveConns),
	)

	// WebSocket: detect the upgrade handshake and run our explicit
	// hijack-based proxy instead of the standard ReverseProxy. The two
	// can both technically handle WebSocket since Go 1.12, but the
	// explicit path lets us preserve ActiveConns for the entire lifetime
	// of the long-lived connection and bypass the http.Server's
	// WriteTimeout (which would otherwise kill connections after 15s).
	if IsWebSocketUpgrade(r) {
		if lb.metrics != nil {
			lb.metrics.WebsocketTotal.WithLabelValues(selected.NodeID).Inc()
		}
		lb.proxyWebSocket(w, r, selected)
		return
	}

	// Measure end-to-end backend response time and capture status code so
	// metrics can label by response class. We always record (not just
	// when a latency-aware selector is active) so the metric is available
	// for observability and so switching strategies at runtime — should
	// that ever land — would have data ready immediately.
	rec := &statusRecordingWriter{ResponseWriter: w}
	start := time.Now()
	selected.proxy.ServeHTTP(rec, r)
	elapsed := time.Since(start)
	selected.RecordLatency(elapsed)

	if lb.metrics != nil {
		// status==0 means the proxy never wrote a header — typically the
		// client disconnected. Treat as 5xx for accounting purposes.
		statusCode := rec.status
		if statusCode == 0 {
			statusCode = http.StatusBadGateway
		}
		class := metrics.StatusClass(statusCode)
		// MethodLabel buckets non-standard HTTP method tokens into
		// "unknown" so an attacker-controlled request line can't
		// inflate the metric registry's cardinality.
		method := metrics.MethodLabel(r.Method)
		lb.metrics.RequestsTotal.WithLabelValues(selected.NodeID, class, method).Inc()
		lb.metrics.RequestDuration.WithLabelValues(selected.NodeID, class).Observe(elapsed.Seconds())

		// Per-backend live gauges. We snapshot AT EMISSION TIME — the
		// ActiveConns reading still includes this request (the deferred
		// decrement hasn't fired yet), and LatencyEWMA reflects the
		// just-recorded sample. Both are the values the next selector
		// invocation would see, which is what an operator wants to
		// observe when debugging "why is this backend losing traffic?".
		//
		// Bounded cardinality: the {backend} label is bounded by cluster
		// size. Stale series for departed backends fade through Prometheus'
		// staleness window (default 5m) — we deliberately do NOT delete
		// gauges on backend removal, because gossip can flap and a
		// re-appearing backend should keep its lineage in the registry.
		lb.metrics.BackendActiveConns.WithLabelValues(selected.NodeID).
			Set(float64(atomic.LoadInt32(&selected.ActiveConns)))
		lb.metrics.BackendLatencyEWMA.WithLabelValues(selected.NodeID).
			Set(float64(atomic.LoadInt64(&selected.LatencyEWMA)) / 1e9)
	}
}

// getOrSyncBackends backend havuzunu aliveNodes ile senkronize eder.
//
// Fast path (membership unchanged): RLock + cache check, zero allocation → O(N).
// Slow path (membership changed): write lock, rebuild the pool, sort by
// NodeID, update the cache. Sorting happens here once — Selector.Select()
// does NOT sort, so per-call selection is O(1).
func (lb *LoadBalancer) getOrSyncBackends(nodes []gossip.NodeInfo) []*Backend {
	// Fast path: if membership is unchanged, return sortedCache (no allocation).
	lb.poolMu.RLock()
	if lb.sameAsCache(nodes) {
		cached := lb.sortedCache
		lb.poolMu.RUnlock()
		return cached
	}
	lb.poolMu.RUnlock()

	// Slow path: take the write lock, rebuild the pool.
	lb.poolMu.Lock()
	defer lb.poolMu.Unlock()

	// Double-check after acquiring the write lock: another goroutine
	// may have rebuilt the cache between our RLock release and Lock.
	if lb.sameAsCache(nodes) {
		return lb.sortedCache
	}

	newPool := make(map[string]*Backend, len(nodes))
	result := make([]*Backend, 0, len(nodes))

	for _, node := range nodes {
		if existing, ok := lb.backendPool[node.ID]; ok {
			// Mevcut Backend — ActiveConns ve ReverseProxy korunur
			newPool[node.ID] = existing
			result = append(result, existing)
		} else {
			// New node — build the Backend once and cache it.
			b := lb.newBackend(node)
			if b == nil {
				// URL parse error — skip this node entirely.
				continue
			}
			newPool[node.ID] = b
			result = append(result, b)
		}
	}

	// Sort once on pool rebuild — Select() no longer sorts on every call.
	sort.Slice(result, func(i, j int) bool {
		return result[i].NodeID < result[j].NodeID
	})

	lb.backendPool = newPool
	lb.sortedCache = result

	// Tell the health checker the new target set. This is the slow path —
	// invoked only on actual membership changes — so we can do the map
	// allocation without worrying about per-request overhead.
	if lb.healthChecker != nil {
		targets := make(map[string]string, len(nodes))
		for _, n := range nodes {
			targets[n.ID] = n.ServiceAddr
		}
		lb.healthChecker.UpdateTargets(targets)
	}

	if lb.metrics != nil {
		lb.metrics.BackendPoolSize.Set(float64(len(result)))
	}

	return result
}

// filterHealthy returns the subset of backends the health checker rates as
// eligible. Returns a shared slice (no copy) when nothing is filtered, to
// avoid allocation on the steady-state path where every backend is healthy.
func (lb *LoadBalancer) filterHealthy(backends []*Backend) []*Backend {
	// Steady-state fast path: if every backend is eligible, return the
	// caller's slice unchanged.
	allOK := true
	for _, b := range backends {
		if !lb.healthChecker.IsHealthy(b.NodeID) {
			allOK = false
			break
		}
	}
	if allOK {
		return backends
	}

	out := make([]*Backend, 0, len(backends))
	for _, b := range backends {
		if lb.healthChecker.IsHealthy(b.NodeID) {
			out = append(out, b)
		}
	}
	return out
}

// handleAdmin routes /_lb/* paths reserved for the load balancer itself.
// Exposes:
//   - /_lb/health   JSON snapshot of every tracked node's health
//   - /_lb/metrics  Prometheus exposition (when metrics enabled)
//
// Order of gates:
//  1. Access control (CIDR + bearer) — runs first so an unauthorised
//     client cannot consume rate-limit budget and starve legitimate
//     scrapers.
//  2. Rate limit — applies to authorised callers only.
//  3. Path dispatch.
//
// All admin paths share a single rate limiter so a runaway scrape loop
// can't compete with real traffic for goroutine slots. Limiter is nil
// when rate-limiting is disabled — preserves the historical "always
// answer admin requests" behavior.
func (lb *LoadBalancer) handleAdmin(w http.ResponseWriter, r *http.Request) {
	if !lb.authorizeAdmin(r) {
		// 404 rather than 401/403 — refuses to confirm the path even
		// exists to an unauthorised caller. The legitimate operator
		// already knows the path and won't be confused.
		http.NotFound(w, r)
		return
	}
	if lb.adminLimiter != nil && !lb.adminLimiter.Allow() {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}
	switch r.URL.Path {
	case "/_lb/health":
		lb.handleHealthDump(w, r)
	case "/_lb/metrics":
		lb.handleMetricsDump(w, r)
	default:
		http.NotFound(w, r)
	}
}

// authorizeAdmin enforces the configured admin access controls.
// Returns true when the request passes ALL configured checks; an
// empty configuration (no CIDRs, no token) is "off" and always
// returns true (back-compat with deployments that haven't opted in).
//
// The CIDR check uses the parsed r.RemoteAddr — NOT X-Forwarded-For.
// Honouring XFF here would silently widen access to anyone who can
// inject a header, which is exactly the kind of footgun this control
// exists to close. Operators behind a reverse proxy must allowlist
// the proxy's address.
//
// Token comparison uses subtle.ConstantTimeCompare to defeat the
// timing oracle that a naive == would expose. Length mismatches are
// folded into the comparison by padding the input — otherwise a
// short input would short-circuit and reveal the expected length.
func (lb *LoadBalancer) authorizeAdmin(r *http.Request) bool {
	if len(lb.adminAllowedNets) > 0 {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			// RemoteAddr is supposed to be host:port. If the server
			// gave us something else, refuse — better safe than sorry.
			return false
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return false
		}
		allowed := false
		for _, n := range lb.adminAllowedNets {
			if n.Contains(ip) {
				allowed = true
				break
			}
		}
		if !allowed {
			return false
		}
	}
	if lb.adminBearerToken != "" {
		// Expected header form: `Authorization: Bearer <token>`.
		// CutPrefix gates BOTH the scheme presence and the value
		// extraction — if the header is missing, empty, or uses a
		// different scheme (e.g. Basic, lowercase "bearer"), ok is
		// false and we reject. TrimPrefix would silently accept a
		// bare token without the scheme, which is wrong.
		const prefix = "Bearer "
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), prefix)
		if !ok {
			return false
		}
		// Length-pad before comparing so timing reveals nothing about
		// the expected length. ConstantTimeCompare returns 1 only for
		// equal-length, equal-content arguments.
		expected := []byte(lb.adminBearerToken)
		got := []byte(token)
		if len(got) != len(expected) {
			// Constant-time compare against expected anyway, then
			// reject. Avoids a short-circuit timing leak.
			subtle.ConstantTimeCompare(expected, expected)
			return false
		}
		if subtle.ConstantTimeCompare(got, expected) != 1 {
			return false
		}
	}
	return true
}

// handleMetricsDump serves the Prometheus /metrics endpoint or returns
// 503 when metrics are disabled — same convention as handleHealthDump.
func (lb *LoadBalancer) handleMetricsDump(w http.ResponseWriter, r *http.Request) {
	if lb.metrics == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"metrics_disabled"}`))
		return
	}
	lb.metrics.Handler().ServeHTTP(w, r)
}

// handleHealthDump returns a JSON map of nodeID → NodeHealth. When the
// checker is disabled, returns an explicit indication so the operator
// knows it's not just an empty cluster.
func (lb *LoadBalancer) handleHealthDump(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if lb.healthChecker == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "health_check_disabled",
		})
		return
	}
	json.NewEncoder(w).Encode(lb.healthChecker.GetAllHealth())
}

// sameAsCache compares the alive node list against the current pool.
// A match means sortedCache is valid and membership is unchanged.
// Must be called under poolMu (RLock or Lock).
func (lb *LoadBalancer) sameAsCache(nodes []gossip.NodeInfo) bool {
	if len(nodes) != len(lb.backendPool) {
		return false
	}
	for _, n := range nodes {
		if _, ok := lb.backendPool[n.ID]; !ok {
			return false
		}
	}
	return true
}

// newBackend builds a Backend instance for a single NodeInfo.
// The ReverseProxy is created once and reused on every subsequent request.
//
// Scheme selection: when backend TLS is enabled we dial backends over HTTPS,
// otherwise plain HTTP. The TLSClientConfig (built once in NewLoadBalancer)
// is shared by every backend's transport — pooling avoids re-parsing the
// CA bundle and keeps connection reuse working correctly across backends.
func (lb *LoadBalancer) newBackend(node gossip.NodeInfo) *Backend {
	scheme := "http"
	if lb.backendTLS != nil {
		scheme = "https"
	}
	u, err := url.Parse(scheme + "://" + node.ServiceAddr)
	if err != nil {
		lb.logger.Error("invalid service addr", "addr", node.ServiceAddr)
		return nil
	}

	proxy := httputil.NewSingleHostReverseProxy(u)

	// FlushInterval = -1: flush immediately after every write to the client.
	// This is required for streaming workloads:
	//   - gRPC server-streaming and bidirectional-streaming RPCs
	//   - Server-Sent Events (text/event-stream)
	//   - HTTP/2 push of partial response bodies
	//
	// For non-streaming responses the cost is negligible — small responses
	// already buffer the entire body before any flush would occur.
	proxy.FlushInterval = -1

	if lb.backendTLS != nil {
		// Clone the shared config so the transport doesn't accidentally
		// mutate the LoadBalancer's view of it.
		//
		// ForceAttemptHTTP2 = true: Go's http.Transport silently disables
		// HTTP/2 ALPN whenever TLSClientConfig is non-nil ("conservatively
		// disables HTTP/2" per the stdlib docs). For gRPC and any
		// HTTP/2-aware backend, we want negotiation back on. Setting this
		// flag is the documented opt-in.
		proxy.Transport = &http.Transport{
			TLSClientConfig:       lb.backendTLS.Clone(),
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		lb.logger.Error("proxy error", "backend", node.ID, "err", err)
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}

	lb.logger.Info("backend registered", "id", node.ID, "addr", node.ServiceAddr, "scheme", scheme)
	return &Backend{
		URL:    u,
		NodeID: node.ID,
		proxy:  proxy,
	}
}
