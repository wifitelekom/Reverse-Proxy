package loadbalancer

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/types"
)

// Backend bir backend node'u temsil eder — load balancer'ın görüşü.
type Backend struct {
	URL         *url.URL
	NodeID      string
	ActiveConns int32 // atomic — concurrent HTTP request sayısı
	proxy       *httputil.ReverseProxy
}

// Selector load balancing stratejisinin arayüzü.
// RoundRobin ve LeastConnections bu interface'i uygular.
// Selector backend listesini saklamaz — her Select çağrısında liste dışarıdan gelir.
type Selector interface {
	Select(backends []*Backend) *Backend
}

// LoadBalancer gossip membership'e dayalı HTTP reverse proxy.
type LoadBalancer struct {
	config     types.LBConfig
	membership *gossip.MembershipList
	selector   Selector
	server     *http.Server
	logger     *slog.Logger
}

// NewLoadBalancer yeni bir LoadBalancer oluşturur.
func NewLoadBalancer(
	cfg types.LBConfig,
	ml *gossip.MembershipList,
	selector Selector,
	logger *slog.Logger,
) *LoadBalancer {
	lb := &LoadBalancer{
		config:     cfg,
		membership: ml,
		selector:   selector,
		logger:     logger,
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
// Her request'te membership listesi taze olarak okunur — gossip state'i anlık yansır.
func (lb *LoadBalancer) handleRequest(w http.ResponseWriter, r *http.Request) {
	aliveNodes := lb.membership.GetAliveMembers()
	if len(aliveNodes) == 0 {
		lb.logger.Error("no healthy backends available")
		http.Error(w, "no healthy backends available", http.StatusServiceUnavailable)
		return
	}

	backends := lb.nodeInfoToBackends(aliveNodes)
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

// nodeInfoToBackends NodeInfo slice'ını Backend slice'a dönüştürür.
// Her request'te yeniden oluşturulur — gossip state anlık yansır.
// 4-10 node için overhead önemsizdir.
func (lb *LoadBalancer) nodeInfoToBackends(nodes []gossip.NodeInfo) []*Backend {
	backends := make([]*Backend, 0, len(nodes))

	for _, node := range nodes {
		u, err := url.Parse("http://" + node.ServiceAddr)
		if err != nil {
			lb.logger.Error("invalid service addr", "addr", node.ServiceAddr)
			continue
		}

		proxy := httputil.NewSingleHostReverseProxy(u)
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			lb.logger.Error("proxy error", "backend", node.ID, "err", err)
			http.Error(w, "backend unavailable", http.StatusBadGateway)
		}

		backends = append(backends, &Backend{
			URL:    u,
			NodeID: node.ID,
			proxy:  proxy,
		})
	}

	return backends
}
