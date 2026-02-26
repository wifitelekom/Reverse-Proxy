package loadbalancer

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/types"
)

// Backend bir backend node'u temsil eder — load balancer'ın görüşü.
// Kalıcı nesne: ActiveConns ve proxy request'ler arasında korunur.
type Backend struct {
	URL         *url.URL
	NodeID      string
	ActiveConns int32 // atomic — concurrent HTTP request sayısı
	proxy       *httputil.ReverseProxy
}

// Selector load balancing stratejisinin arayüzü.
// RoundRobin ve LeastConnections bu interface'i uygular.
type Selector interface {
	Select(backends []*Backend) *Backend
}

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
}

// NewLoadBalancer yeni bir LoadBalancer oluşturur.
func NewLoadBalancer(
	cfg types.LBConfig,
	ml *gossip.MembershipList,
	selector Selector,
	logger *slog.Logger,
) *LoadBalancer {
	lb := &LoadBalancer{
		config:      cfg,
		membership:  ml,
		selector:    selector,
		logger:      logger,
		backendPool: make(map[string]*Backend),
	}

	lb.server = &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      http.HandlerFunc(lb.handleRequest),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return lb
}

// Start HTTP server'ı başlatır — blocking.
// ctx iptal edildiğinde server graceful shutdown yapar.
func (lb *LoadBalancer) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		lb.server.Shutdown(shutdownCtx)
	}()

	lb.logger.Info("load balancer starting", "addr", lb.config.ListenAddr, "strategy", lb.config.Strategy)
	if err := lb.server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// handleRequest her gelen HTTP request'i işler.
// Her request'te membership listesi taze okunur, backend havuzu senkronize edilir.
func (lb *LoadBalancer) handleRequest(w http.ResponseWriter, r *http.Request) {
	aliveNodes := lb.membership.GetAliveMembers()
	if len(aliveNodes) == 0 {
		lb.logger.Error("no healthy backends available")
		http.Error(w, "no healthy backends available", http.StatusServiceUnavailable)
		return
	}

	// Kalıcı backend havuzundan al — ActiveConns korunur
	backends := lb.getOrSyncBackends(aliveNodes)

	selected := lb.selector.Select(backends)
	if selected == nil {
		http.Error(w, "no backend selected", http.StatusServiceUnavailable)
		return
	}

	atomic.AddInt32(&selected.ActiveConns, 1)
	defer atomic.AddInt32(&selected.ActiveConns, -1)

	lb.logger.Info("routing request",
		"backend", selected.NodeID,
		"url", selected.URL.String(),
		"active_conns", atomic.LoadInt32(&selected.ActiveConns),
		"path", r.URL.Path,
	)

	selected.proxy.ServeHTTP(w, r)
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
	return result
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
func (lb *LoadBalancer) newBackend(node gossip.NodeInfo) *Backend {
	u, err := url.Parse("http://" + node.ServiceAddr)
	if err != nil {
		lb.logger.Error("invalid service addr", "addr", node.ServiceAddr)
		return nil
	}

	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		lb.logger.Error("proxy error", "backend", node.ID, "err", err)
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}

	lb.logger.Info("backend registered", "id", node.ID, "addr", node.ServiceAddr)
	return &Backend{
		URL:    u,
		NodeID: node.ID,
		proxy:  proxy,
	}
}
