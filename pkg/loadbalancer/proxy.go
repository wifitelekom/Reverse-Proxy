package loadbalancer

import (
	"bufio"
	"context"
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

// Backend bir backend node'u temsil eder — load balancer'ın görüşü.
// Kalıcı nesne: ActiveConns, LatencyEWMA ve proxy request'ler arasında korunur.
//
// LatencyEWMA holds the exponentially-weighted moving average of recent
// response times in nanoseconds, updated atomically by RecordLatency. It
// is read by latency-aware selectors (P2CEWMA). A value of 0 means "no
// measurements yet" — selectors should treat that as missing data rather
// than as zero latency.
type Backend struct {
	URL         *url.URL
	NodeID      string
	ActiveConns int32 // atomic — concurrent HTTP request sayısı
	LatencyEWMA int64 // atomic — EWMA response time in nanoseconds
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

// Selector load balancing stratejisinin arayüzü.
// RoundRobin ve LeastConnections bu interface'i uygular.
type Selector interface {
	Select(backends []*Backend) *Backend
}

// adminPathPrefix marks routes the LB itself answers (rather than proxying
// to a backend). Operators must avoid colliding paths in their backends.
const adminPathPrefix = "/_lb/"

// LoadBalancer gossip membership'e dayalı HTTP reverse proxy.
type LoadBalancer struct {
	config      types.LBConfig
	membership  *gossip.MembershipList
	selector    Selector
	server      *http.Server
	logger      *slog.Logger
	backendPool map[string]*Backend // NodeID → kalıcı Backend nesnesi
	sortedCache []*Backend          // pre-sorted liste; membership değişince yenilenir
	poolMu      sync.RWMutex       // backendPool + sortedCache erişim kilidi

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

// NewLoadBalancer yeni bir LoadBalancer oluşturur.
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

	return lb, nil
}

// Start HTTP server'ı başlatır — blocking.
// ctx iptal edildiğinde server graceful shutdown yapar.
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
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

// handleRequest her gelen HTTP request'i işler.
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
		http.Error(w, "no healthy backends available", http.StatusServiceUnavailable)
		return
	}

	// Kalıcı backend havuzundan al — ActiveConns korunur
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
		lb.metrics.RequestsTotal.WithLabelValues(selected.NodeID, class, r.Method).Inc()
		lb.metrics.RequestDuration.WithLabelValues(selected.NodeID, class).Observe(elapsed.Seconds())
	}
}

// getOrSyncBackends backend havuzunu aliveNodes ile senkronize eder.
//
// Hızlı yol (membership değişmedi): RLock ile cache kontrol, allocation yok → O(N).
// Yavaş yol (membership değişti): write lock, pool rebuild, NodeID sort, cache güncelle.
// Sort burada bir kez yapılır — Selector.Select() sıralama yapmaz (O(1) seçim).
func (lb *LoadBalancer) getOrSyncBackends(nodes []gossip.NodeInfo) []*Backend {
	// Hızlı yol: membership değişmemişse sortedCache'i döndür (allocation yok)
	lb.poolMu.RLock()
	if lb.sameAsCache(nodes) {
		cached := lb.sortedCache
		lb.poolMu.RUnlock()
		return cached
	}
	lb.poolMu.RUnlock()

	// Yavaş yol: write lock, pool yeniden oluştur
	lb.poolMu.Lock()
	defer lb.poolMu.Unlock()

	// Double-check: başka goroutine aynı anda rebuild yapmış olabilir
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
			// Yeni node — bir kez oluştur
			b := lb.newBackend(node)
			if b == nil {
				// URL parse hatası — bu node'u atla
				continue
			}
			newPool[node.ID] = b
			result = append(result, b)
		}
	}

	// Pool yenilenince bir kez sırala — Select() artık sıralama yapmaz
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
// All admin paths share a single rate limiter so a runaway scrape loop
// can't compete with real traffic for goroutine slots. Limiter is nil
// when rate-limiting is disabled — preserves the historical "always
// answer admin requests" behavior.
func (lb *LoadBalancer) handleAdmin(w http.ResponseWriter, r *http.Request) {
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

// sameAsCache mevcut alive node listesi ile pool içeriğini karşılaştırır.
// Eşleşme varsa sortedCache geçerlidir ve membership değişmemiştir.
// poolMu kilidi (RLock veya Lock) altında çağrılmalı.
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

// newBackend tek bir NodeInfo için Backend nesnesi oluşturur.
// ReverseProxy bir kez oluşturulur ve sonraki request'lerde yeniden kullanılır.
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
