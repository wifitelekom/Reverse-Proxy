package loadbalancer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/types"
)

// =====================================================================
// IsWebSocketUpgrade — header parsing
// =====================================================================

func TestIsWebSocketUpgrade_PositiveCases(t *testing.T) {
	// Each case is a valid Upgrade handshake — the proxy must detect it.
	cases := []struct {
		name string
		conn string
		up   string
	}{
		{"basic", "Upgrade", "websocket"},
		{"multi-token-connection", "keep-alive, Upgrade", "websocket"},
		{"case-insensitive-upgrade", "upgrade", "WebSocket"},
		{"case-insensitive-token", "UPGRADE", "websocket"},
		{"whitespace-around-token", "  Upgrade  , close", "websocket"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			r.Header.Set("Connection", tc.conn)
			r.Header.Set("Upgrade", tc.up)
			if !IsWebSocketUpgrade(r) {
				t.Errorf("expected upgrade detected; conn=%q upgrade=%q", tc.conn, tc.up)
			}
		})
	}
}

func TestIsWebSocketUpgrade_NegativeCases(t *testing.T) {
	cases := []struct {
		name string
		conn string
		up   string
	}{
		{"plain-http", "keep-alive", ""},
		{"upgrade-but-no-connection-token", "keep-alive", "websocket"},
		{"connection-but-wrong-upgrade", "Upgrade", "h2c"},
		{"empty-headers", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			if tc.conn != "" {
				r.Header.Set("Connection", tc.conn)
			}
			if tc.up != "" {
				r.Header.Set("Upgrade", tc.up)
			}
			if IsWebSocketUpgrade(r) {
				t.Errorf("false positive; conn=%q upgrade=%q", tc.conn, tc.up)
			}
		})
	}
}

// =====================================================================
// End-to-end proxy via raw TCP + HTTP/1.1 Upgrade dance
// =====================================================================

// upgradeEchoServer accepts the WebSocket-style handshake, replies 101,
// then echoes any subsequent bytes. We do this at raw TCP level so the
// test stays free of any websocket framing library — what we're really
// testing is the *proxy*, not the WebSocket protocol itself.
//
// Returns the listener (caller should Close it) and the host:port.
func upgradeEchoServer(t *testing.T) (net.Listener, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				// Read the HTTP request line + headers (terminated by blank line).
				for {
					line, err := br.ReadString('\n')
					if err != nil {
						return
					}
					if strings.TrimSpace(line) == "" {
						break
					}
				}
				// Reply with 101 Switching Protocols — minimal, just enough
				// to look like an upgrade.
				_, _ = c.Write([]byte(
					"HTTP/1.1 101 Switching Protocols\r\n" +
						"Upgrade: websocket\r\n" +
						"Connection: Upgrade\r\n" +
						"\r\n"))
				// Now echo: read a chunk, write it back.
				buf := make([]byte, 1024)
				for {
					n, err := br.Read(buf)
					if n > 0 {
						if _, werr := c.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return ln, ln.Addr().String()
}

func TestWebSocket_EndToEndUpgradeAndEcho(t *testing.T) {
	// Topology: raw TCP client → LB → upgradeEchoServer.
	// We exercise the full hijack + bidirectional copy path and verify
	// the bytes we write end up echoed back through the LB.
	ln, backendAddr := upgradeEchoServer(t)
	defer ln.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(gossip.NodeInfo{
		ID:          "echo",
		Addr:        backendAddr,
		ServiceAddr: backendAddr,
		State:       gossip.StateAlive,
		Incarnation: 1,
	})

	cfg := types.LBConfig{ListenAddr: ":0", Strategy: "roundrobin"}
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}

	// Bind LB to an ephemeral port. Don't use httptest because we want
	// the real http.Server with its full Hijacker support.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lbLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("lb listen: %v", err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(lb.handleRequest)}
	go func() { _ = srv.Serve(lbLn) }()
	defer srv.Shutdown(ctx)
	lbAddr := lbLn.Addr().String()

	// Client side: open a raw TCP connection to the LB, send the upgrade
	// request, expect 101 back, then send + receive bytes.
	cc, err := net.DialTimeout("tcp", lbAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	defer cc.Close()
	cc.SetDeadline(time.Now().Add(3 * time.Second))

	req := "GET /chat HTTP/1.1\r\n" +
		"Host: " + lbAddr + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"\r\n"
	if _, err := cc.Write([]byte(req)); err != nil {
		t.Fatalf("write upgrade: %v", err)
	}

	br := bufio.NewReader(cc)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	if !strings.Contains(statusLine, "101") {
		t.Fatalf("expected 101 Switching Protocols; got %q", statusLine)
	}
	// Drain the rest of the headers.
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read header: %v", err)
		}
		if strings.TrimSpace(line) == "" {
			break
		}
	}

	// Bidirectional bytes through the proxy.
	if _, err := cc.Write([]byte("hello")); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	got := make([]byte, 5)
	if _, err := io.ReadFull(br, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("echo mismatch: got %q want %q", got, "hello")
	}
}

func TestWebSocket_BackendDownReturns502(t *testing.T) {
	// Backend address points at a port nobody's listening on. The proxy
	// must detect the dial failure BEFORE hijacking, so a normal HTTP
	// 502 reaches the client.
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(gossip.NodeInfo{
		ID:          "dead",
		Addr:        "127.0.0.1:1", // reserved, no listener
		ServiceAddr: "127.0.0.1:1",
		State:       gossip.StateAlive,
		Incarnation: 1,
	})

	cfg := types.LBConfig{ListenAddr: ":0", Strategy: "roundrobin"}
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/", nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Sec-WebSocket-Version", "13")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status: got %d want 502", resp.StatusCode)
	}
}

func TestWebSocket_ActiveConnsTrackedDuringUpgrade(t *testing.T) {
	// Spin up an upgrade server that *does not* echo immediately, holding
	// the connection open. While it's open, the LB's Backend.ActiveConns
	// must reflect the live connection — that's the signal LeastConn /
	// P2C use to avoid stacking new requests on a busy WebSocket node.
	hold := make(chan struct{})
	defer close(hold)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Read until blank line, then send 101 and block until release.
		br := bufio.NewReader(conn)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if strings.TrimSpace(line) == "" {
				break
			}
		}
		conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\n\r\n"))
		<-hold // hang the connection open until the test releases
	}()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(gossip.NodeInfo{
		ID:          "ws",
		Addr:        ln.Addr().String(),
		ServiceAddr: ln.Addr().String(),
		State:       gossip.StateAlive,
		Incarnation: 1,
	})

	cfg := types.LBConfig{ListenAddr: ":0", Strategy: "roundrobin"}
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}

	lbLn, _ := net.Listen("tcp", "127.0.0.1:0")
	srv := &http.Server{Handler: http.HandlerFunc(lb.handleRequest)}
	go func() { _ = srv.Serve(lbLn) }()
	defer srv.Shutdown(context.Background())

	// Drive an upgrade through the LB and don't read anything more — we
	// just want the connection sitting open.
	cc, err := net.Dial("tcp", lbLn.Addr().String())
	if err != nil {
		t.Fatalf("dial lb: %v", err)
	}
	defer cc.Close()
	cc.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: x\r\nSec-WebSocket-Version: 13\r\n\r\n"))

	// Poll Backend.ActiveConns through the LB's pool.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		nodes := ml.GetAliveMembers()
		backends := lb.getOrSyncBackends(nodes)
		if len(backends) > 0 && atomic.LoadInt32(&backends[0].ActiveConns) > 0 {
			return // success
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("ActiveConns never reflected the open WebSocket")
}

// =====================================================================
// gRPC / streaming readiness
// =====================================================================

func TestStreaming_BackendProxyHasFlushIntervalNegativeOne(t *testing.T) {
	// Verify the ReverseProxy produced by newBackend is configured to
	// flush every write — without this, gRPC server-streaming RPCs and
	// SSE responses would buffer up in the LB until the backend closed.
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	cfg := types.LBConfig{ListenAddr: ":0", Strategy: "roundrobin"}
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}

	b := lb.newBackend(gossip.NodeInfo{
		ID: "x", ServiceAddr: "127.0.0.1:1234",
		State: gossip.StateAlive, Incarnation: 1,
	})
	if b == nil {
		t.Fatal("newBackend returned nil")
	}
	if b.proxy.FlushInterval != -1 {
		t.Errorf("FlushInterval: got %v want -1", b.proxy.FlushInterval)
	}
}

func TestStreaming_ChunkedResponseFlushesIncrementally(t *testing.T) {
	// Simulate gRPC-style streaming by emitting chunks slowly. With
	// FlushInterval = -1 we should see each chunk arrive at the client
	// promptly, not all at once at the end.
	releaseChunk := make(chan struct{})
	allDone := make(chan struct{})

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Need a Flusher to stream incrementally.
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flusher", 500)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)

		// First chunk fires immediately.
		fmt.Fprint(w, "chunk-1\n")
		flusher.Flush()

		// Wait for the test to confirm the first chunk arrived, then
		// emit the second.
		<-releaseChunk
		fmt.Fprint(w, "chunk-2\n")
		flusher.Flush()
		close(allDone)
	}))
	defer backend.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	addr := strings.TrimPrefix(backend.URL, "http://")
	ml.AddOrUpdate(gossip.NodeInfo{
		ID: "stream", Addr: addr, ServiceAddr: addr,
		State: gossip.StateAlive, Incarnation: 1,
	})

	cfg := types.LBConfig{ListenAddr: ":0", Strategy: "roundrobin"}
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	resp, err := http.Get(lbServer.URL + "/stream")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	br := bufio.NewReader(resp.Body)
	first, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read first chunk: %v", err)
	}
	if strings.TrimSpace(first) != "chunk-1" {
		t.Errorf("first chunk: got %q want chunk-1", first)
	}

	// Critical: at this point the backend is still inside the handler,
	// blocked on releaseChunk. If FlushInterval were 0 (the default),
	// we would NOT have read chunk-1 yet — it would be buffered until
	// the response ends. The fact that we got it proves streaming.

	close(releaseChunk)
	second, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read second chunk: %v", err)
	}
	if strings.TrimSpace(second) != "chunk-2" {
		t.Errorf("second chunk: got %q want chunk-2", second)
	}
	<-allDone
}

// =====================================================================
// HTTP/2 backend negotiation (over TLS, via ALPN)
// =====================================================================

func TestHTTP2_BackendNegotiatesViaALPN(t *testing.T) {
	// httptest.NewTLSServer gives us a real HTTPS backend. With our LB
	// configured for backend TLS, the http.Transport's ALPN should
	// negotiate HTTP/2 automatically — Go's stdlib does this without
	// explicit configuration. We assert by checking the backend handler
	// sees HTTP/2.0 as the request protocol.
	var seenProto atomic.Value
	tlsBackend := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenProto.Store(r.Proto)
		w.WriteHeader(200)
	}))
	tlsBackend.EnableHTTP2 = true
	tlsBackend.StartTLS()
	defer tlsBackend.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	addr := tlsBackend.Listener.Addr().String()
	ml.AddOrUpdate(gossip.NodeInfo{
		ID: "h2", Addr: addr, ServiceAddr: addr,
		State: gossip.StateAlive, Incarnation: 1,
	})

	cfg := types.LBConfig{
		ListenAddr: ":0",
		Strategy:   "roundrobin",
		BackendTLS: types.BackendTLSConfig{Enabled: true, SkipVerify: true},
	}
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	lbServer := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	defer lbServer.Close()

	resp, err := http.Get(lbServer.URL + "/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()

	got, _ := seenProto.Load().(string)
	if got != "HTTP/2.0" {
		t.Errorf("backend saw %q, expected HTTP/2.0 — ALPN didn't negotiate", got)
	}
}
