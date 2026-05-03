package loadbalancer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/types"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func defaultLBConfig(listenAddr string) types.LBConfig {
	return types.LBConfig{
		ListenAddr: listenAddr,
		Strategy:   "roundrobin",
	}
}

// makeTestNode httptest.Server URL'inden bir gossip.NodeInfo oluşturur.
func makeTestNode(id string, server *httptest.Server) gossip.NodeInfo {
	// httptest.Server.URL formatı: "http://127.0.0.1:PORT"
	// ServiceAddr: "127.0.0.1:PORT" (scheme olmadan)
	addr := server.URL[len("http://"):]
	return gossip.NodeInfo{
		ID:          id,
		Addr:        addr,
		ServiceAddr: addr,
		State:       gossip.StateAlive,
		Incarnation: 1,
	}
}

// TestProxy_RoutesRequest: 2 backend httptest.Server var, LB request'i iletmeli.
func TestProxy_RoutesRequest(t *testing.T) {
	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"node":"node-a"}`)
	}))
	defer backendA.Close()

	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"node":"node-b"}`)
	}))
	defer backendB.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(makeTestNode("node-a", backendA))
	ml.AddOrUpdate(makeTestNode("node-b", backendB))

	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}

	// LB'yi test server olarak çalıştır
	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	seen := map[string]bool{}

	for i := 0; i < 10; i++ {
		resp, err := client.Get(lbServer.URL + "/")
		if err != nil {
			t.Fatalf("request %d başarısız: %v", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("beklenen 200, alınan %d", resp.StatusCode)
		}
		t.Logf("request %d: %s", i, body)
		if string(body) != "" {
			seen[string(body)] = true
		}
	}

	// Her iki backend de en az bir kez seçilmiş olmalı (RR adil dağılım)
	if !seen[`{"node":"node-a"}`] || !seen[`{"node":"node-b"}`] {
		t.Errorf("her iki backend de seçilmeli — alınan: %v", seen)
	}
}

// TestProxy_NoBackends503: Membership boş → 503 Service Unavailable.
func TestProxy_NoBackends503(t *testing.T) {
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	// Membership boş — hiç Alive backend yok

	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}

	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	resp, err := http.Get(lbServer.URL + "/")
	if err != nil {
		t.Fatalf("request başarısız: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("beklenen 503, alınan %d", resp.StatusCode)
	}
}

// TestProxy_NilBackendSkipped: Geçersiz ServiceAddr'li node → newBackend nil döner.
// getOrSyncBackends nil'i atlamalı — panic olmamalı.
func TestProxy_NilBackendSkipped(t *testing.T) {
	backendOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"node":"ok"}`)
	}))
	defer backendOK.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)

	// Geçerli backend
	ml.AddOrUpdate(makeTestNode("node-ok", backendOK))

	// Geçersiz ServiceAddr — url.Parse hata verecek (boşluk içeriyor)
	ml.AddOrUpdate(gossip.NodeInfo{
		ID:          "node-bad",
		Addr:        "127.0.0.1:9999",
		ServiceAddr: "invalid addr with spaces",
		State:       gossip.StateAlive,
		Incarnation: 1,
	})

	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}

	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	// Panic olmamalı; geçerli backend 200 dönmeli
	resp, err := http.Get(lbServer.URL + "/")
	if err != nil {
		t.Fatalf("request başarısız: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("beklenen 200, alınan %d (body: %s)", resp.StatusCode, body)
	}
}

// TestProxy_ActiveConnsPreserved: getOrSyncBackends aynı node için aynı Backend pointer dönmeli.
// ActiveConns request'ler arasında korunur (LC için kritik).
func TestProxy_ActiveConnsPreserved(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"node":"node-a"}`)
	}))
	defer backend.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(makeTestNode("node-a", backend))

	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}

	nodes := ml.GetAliveMembers()

	// İlk çağrı — backend oluşturulur
	backends1 := lb.getOrSyncBackends(nodes)
	if len(backends1) == 0 {
		t.Fatal("en az 1 backend bekleniyor")
	}
	b1 := backends1[0]

	// İkinci çağrı — aynı pointer dönmeli
	backends2 := lb.getOrSyncBackends(nodes)
	if len(backends2) == 0 {
		t.Fatal("en az 1 backend bekleniyor")
	}
	b2 := backends2[0]

	if b1 != b2 {
		t.Fatal("aynı node için aynı *Backend pointer dönmeli (pool reuse)")
	}
}

// TestProxy_ContextCancel: Context iptal edilince LB server graceful shutdown yapmalı.
func TestProxy_ContextCancel(t *testing.T) {
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)

	cfg := defaultLBConfig("127.0.0.1:0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	lb.server.Addr = "127.0.0.1:18199" // test için sabit port

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- lb.Start(ctx)
	}()

	// Server başlaması için kısa bekle
	time.Sleep(50 * time.Millisecond)
	cancel() // Context iptal — graceful shutdown tetiklenir

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("beklenmeyen hata: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown 2s içinde tamamlanmadı")
	}
}
