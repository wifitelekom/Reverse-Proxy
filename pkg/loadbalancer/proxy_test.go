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

// makeTestNode builds a gossip.NodeInfo from an httptest.Server URL.
func makeTestNode(id string, server *httptest.Server) gossip.NodeInfo {
	// httptest.Server.URL has the form "http://127.0.0.1:PORT".
	// ServiceAddr drops the scheme: "127.0.0.1:PORT".
	addr := server.URL[len("http://"):]
	return gossip.NodeInfo{
		ID:          id,
		Addr:        addr,
		ServiceAddr: addr,
		State:       gossip.StateAlive,
		Incarnation: 1,
	}
}

// TestProxy_RoutesRequest: with two backends behind the LB, requests
// must be proxied to both.
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

	// Run the LB as a test server.
	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	seen := map[string]bool{}

	for i := 0; i < 10; i++ {
		resp, err := client.Get(lbServer.URL + "/")
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		t.Logf("request %d: %s", i, body)
		if string(body) != "" {
			seen[string(body)] = true
		}
	}

	// Both backends must have been chosen at least once (RR is fair).
	if !seen[`{"node":"node-a"}`] || !seen[`{"node":"node-b"}`] {
		t.Errorf("both backends should have served at least one request; got: %v", seen)
	}
}

// TestProxy_NoBackends503: empty membership → 503 Service Unavailable.
func TestProxy_NoBackends503(t *testing.T) {
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	// No AddOrUpdate — there are no Alive backends.

	cfg := defaultLBConfig(":0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}

	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	resp, err := http.Get(lbServer.URL + "/")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
}

// TestProxy_NilBackendSkipped: a node with an invalid ServiceAddr makes
// newBackend return nil. getOrSyncBackends must skip nil entries — and
// must not panic.
func TestProxy_NilBackendSkipped(t *testing.T) {
	backendOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"node":"ok"}`)
	}))
	defer backendOK.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)

	// A valid backend.
	ml.AddOrUpdate(makeTestNode("node-ok", backendOK))

	// An invalid ServiceAddr — url.Parse will fail (it contains spaces).
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

	// No panic; the valid backend must still answer 200.
	resp, err := http.Get(lbServer.URL + "/")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", resp.StatusCode, body)
	}
}

// TestProxy_ActiveConnsPreserved: getOrSyncBackends must return the
// SAME *Backend pointer for the same nodeID across calls. ActiveConns
// is preserved across requests, which is critical for LeastConn.
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

	// First call — Backend is created.
	backends1 := lb.getOrSyncBackends(nodes)
	if len(backends1) == 0 {
		t.Fatal("expected at least 1 backend")
	}
	b1 := backends1[0]

	// Second call — same pointer must come back.
	backends2 := lb.getOrSyncBackends(nodes)
	if len(backends2) == 0 {
		t.Fatal("expected at least 1 backend")
	}
	b2 := backends2[0]

	if b1 != b2 {
		t.Fatal("same node must return the same *Backend pointer (pool reuse)")
	}
}

// TestProxy_ContextCancel: when the context is canceled, the LB
// server must graceful-shut down.
func TestProxy_ContextCancel(t *testing.T) {
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)

	cfg := defaultLBConfig("127.0.0.1:0")
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	lb.server.Addr = "127.0.0.1:18199" // fixed port for the test

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- lb.Start(ctx)
	}()

	// Brief wait for the server to bind.
	time.Sleep(50 * time.Millisecond)
	cancel() // context canceled — triggers graceful shutdown

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not complete within 2s")
	}
}
