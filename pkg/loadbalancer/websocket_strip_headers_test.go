package loadbalancer

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/types"
)

// =====================================================================
// H-3': WebSocketStripHeaders config
//
// By default, the WebSocket proxy forwards every client header to the
// backend (Cookie, Authorization, Origin, et al.). Operators behind a
// fronting proxy that already handles auth need a way to scrub those
// headers off the backend leg without dropping the handshake. These
// tests pin the new opt-in stripping behaviour and the protections
// around essential handshake headers.
// =====================================================================

// captureUpgradeServer records the request headers it received during
// a WebSocket upgrade, replies 101, then immediately closes. The
// returned recordedHeaders is populated only after the test has
// completed at least one upgrade — read it after the body of the test.
type capturedHandshake struct {
	mu      sync.Mutex
	headers http.Header
}

func (c *capturedHandshake) snapshot() http.Header {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make(http.Header, len(c.headers))
	for k, v := range c.headers {
		cp[k] = append([]string(nil), v...)
	}
	return cp
}

// captureUpgradeServer is a raw-TCP echo of the upgrade headers. The
// design mirrors upgradeEchoServer in websocket_test.go but exposes
// the parsed headers via the capture struct.
func captureUpgradeServer(t *testing.T) (net.Listener, string, *capturedHandshake) {
	t.Helper()
	cap := &capturedHandshake{}
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
				// Skip the request line.
				if _, err := br.ReadString('\n'); err != nil {
					return
				}
				h := http.Header{}
				for {
					line, err := br.ReadString('\n')
					if err != nil {
						return
					}
					line = strings.TrimRight(line, "\r\n")
					if line == "" {
						break
					}
					i := strings.IndexByte(line, ':')
					if i < 0 {
						continue
					}
					name := strings.TrimSpace(line[:i])
					val := strings.TrimSpace(line[i+1:])
					h.Add(name, val)
				}
				cap.mu.Lock()
				cap.headers = h
				cap.mu.Unlock()

				_, _ = c.Write([]byte(
					"HTTP/1.1 101 Switching Protocols\r\n" +
						"Upgrade: websocket\r\n" +
						"Connection: Upgrade\r\n" +
						"\r\n"))
			}(conn)
		}
	}()
	return ln, ln.Addr().String(), cap
}

// runUpgradeOnce drives one WebSocket upgrade through the LB at lbAddr
// with the given client headers. Returns when the LB has finished
// proxying the handshake. We don't need to actually exchange WS frames
// — the captured backend headers are the contract under test.
func runUpgradeOnce(t *testing.T, lbAddr string, extraHeaders map[string]string) {
	t.Helper()
	cc, err := net.DialTimeout("tcp", lbAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	defer cc.Close()
	cc.SetDeadline(time.Now().Add(3 * time.Second))

	var sb strings.Builder
	sb.WriteString("GET /chat HTTP/1.1\r\n")
	sb.WriteString("Host: " + lbAddr + "\r\n")
	sb.WriteString("Upgrade: websocket\r\n")
	sb.WriteString("Connection: Upgrade\r\n")
	sb.WriteString("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n")
	sb.WriteString("Sec-WebSocket-Version: 13\r\n")
	for k, v := range extraHeaders {
		sb.WriteString(k + ": " + v + "\r\n")
	}
	sb.WriteString("\r\n")

	if _, err := cc.Write([]byte(sb.String())); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Read the 101 line so we know the round-trip completed.
	br := bufio.NewReader(cc)
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("read 101: %v", err)
	}
}

// startLB spins up a real *http.Server bound to a loopback ephemeral
// port and returns its addr. We use the real server (not httptest)
// because we need full Hijacker support for the WS path.
func startLB(t *testing.T, lb *LoadBalancer) string {
	t.Helper()
	lbLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("lb listen: %v", err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(lb.handleRequest)}
	go func() { _ = srv.Serve(lbLn) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return lbLn.Addr().String()
}

// TestWebSocketStripHeaders_DefaultForwardsEverything pins the
// back-compat contract: empty config = pass every header through.
// Existing deployments must not see behaviour change.
func TestWebSocketStripHeaders_DefaultForwardsEverything(t *testing.T) {
	ln, backendAddr, cap := captureUpgradeServer(t)
	defer ln.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(gossip.NodeInfo{ID: "b", Addr: backendAddr, ServiceAddr: backendAddr, State: gossip.StateAlive, Incarnation: 1})

	cfg := types.LBConfig{ListenAddr: ":0", Strategy: "roundrobin"}
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	lbAddr := startLB(t, lb)

	runUpgradeOnce(t, lbAddr, map[string]string{
		"Cookie":        "session=abc123",
		"Authorization": "Bearer client-token",
		"Origin":        "https://app.example.com",
		"X-Custom":      "passthrough-me",
	})

	// Allow the backend goroutine a moment to record headers.
	time.Sleep(100 * time.Millisecond)
	got := cap.snapshot()

	for _, h := range []string{"Cookie", "Authorization", "Origin", "X-Custom"} {
		if got.Get(h) == "" {
			t.Errorf("default config: header %q should have been forwarded; saw: %v", h, got)
		}
	}
}

// TestWebSocketStripHeaders_StripsListed proves the headline contract:
// listed headers do not reach the backend, while unrelated headers do.
func TestWebSocketStripHeaders_StripsListed(t *testing.T) {
	ln, backendAddr, cap := captureUpgradeServer(t)
	defer ln.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(gossip.NodeInfo{ID: "b", Addr: backendAddr, ServiceAddr: backendAddr, State: gossip.StateAlive, Incarnation: 1})

	cfg := types.LBConfig{
		ListenAddr:            ":0",
		Strategy:              "roundrobin",
		WebSocketStripHeaders: []string{"Cookie", "Authorization"},
	}
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	lbAddr := startLB(t, lb)

	runUpgradeOnce(t, lbAddr, map[string]string{
		"Cookie":        "session=should-be-stripped",
		"Authorization": "Bearer should-be-stripped",
		"Origin":        "https://app.example.com",
		"X-Trace":       "keep-me",
	})

	time.Sleep(100 * time.Millisecond)
	got := cap.snapshot()

	for _, h := range []string{"Cookie", "Authorization"} {
		if v := got.Get(h); v != "" {
			t.Errorf("header %q should have been stripped, but backend saw %q", h, v)
		}
	}
	for _, h := range []string{"Origin", "X-Trace"} {
		if got.Get(h) == "" {
			t.Errorf("header %q should have been forwarded, but backend did not see it", h)
		}
	}
	// Handshake essentials must still be present — the strip path must
	// not break the WS protocol.
	for _, h := range []string{"Upgrade", "Connection", "Sec-WebSocket-Key", "Sec-WebSocket-Version"} {
		if got.Get(h) == "" {
			t.Errorf("essential handshake header %q missing — strip logic broke the upgrade", h)
		}
	}
}

// TestWebSocketStripHeaders_CaseInsensitive verifies HTTP's case-
// insensitive header rule: "cookie" in the strip list must remove
// "Cookie" from the wire just like "Cookie" would.
func TestWebSocketStripHeaders_CaseInsensitive(t *testing.T) {
	ln, backendAddr, cap := captureUpgradeServer(t)
	defer ln.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(gossip.NodeInfo{ID: "b", Addr: backendAddr, ServiceAddr: backendAddr, State: gossip.StateAlive, Incarnation: 1})

	cfg := types.LBConfig{
		ListenAddr:            ":0",
		Strategy:              "roundrobin",
		WebSocketStripHeaders: []string{"cookie", "AUTHORIZATION"},
	}
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	lbAddr := startLB(t, lb)

	runUpgradeOnce(t, lbAddr, map[string]string{
		"Cookie":        "x=1",
		"Authorization": "Bearer y",
	})

	time.Sleep(100 * time.Millisecond)
	got := cap.snapshot()

	if got.Get("Cookie") != "" {
		t.Errorf("lowercase 'cookie' in strip list should match Cookie; saw %q", got.Get("Cookie"))
	}
	if got.Get("Authorization") != "" {
		t.Errorf("UPPERCASE in strip list should match Authorization; saw %q", got.Get("Authorization"))
	}
}

// TestWebSocketStripHeaders_ProtectedEntriesIgnored verifies the
// safety belt: an operator who lists Upgrade or Sec-WebSocket-Key in
// the strip list (probably a copy/paste mistake) does not get a
// silently broken handshake. We log a warning at startup and refuse
// to honour the entry; a successful upgrade after the bogus config
// proves the protection.
func TestWebSocketStripHeaders_ProtectedEntriesIgnored(t *testing.T) {
	ln, backendAddr, cap := captureUpgradeServer(t)
	defer ln.Close()

	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	ml.AddOrUpdate(gossip.NodeInfo{ID: "b", Addr: backendAddr, ServiceAddr: backendAddr, State: gossip.StateAlive, Incarnation: 1})

	cfg := types.LBConfig{
		ListenAddr: ":0",
		Strategy:   "roundrobin",
		WebSocketStripHeaders: []string{
			"Upgrade",               // would break the upgrade outright
			"Connection",            // ditto
			"Sec-WebSocket-Key",     // ditto
			"Sec-WebSocket-Version", // ditto
			"Host",                  // backend would have no Host
			"Cookie",                // legitimate entry, must still work
		},
	}
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, nil)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	lbAddr := startLB(t, lb)

	runUpgradeOnce(t, lbAddr, map[string]string{
		"Cookie": "should-be-stripped",
	})

	time.Sleep(100 * time.Millisecond)
	got := cap.snapshot()

	// Cookie (the only legitimate entry) actually got stripped.
	if got.Get("Cookie") != "" {
		t.Errorf("legitimate Cookie strip should still apply, saw %q", got.Get("Cookie"))
	}
	// Handshake essentials all survived — proves the protection.
	for _, h := range []string{"Upgrade", "Connection", "Sec-WebSocket-Key", "Sec-WebSocket-Version", "Host"} {
		if got.Get(h) == "" {
			t.Errorf("protected header %q must NOT be strippable; backend missed it", h)
		}
	}
}
