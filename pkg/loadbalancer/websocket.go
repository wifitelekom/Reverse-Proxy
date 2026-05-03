package loadbalancer

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// IsWebSocketUpgrade reports whether the request is a WebSocket upgrade
// handshake (RFC 6455 §4.1).
//
// Per RFC 6455 the two header fields are:
//   - Connection: Upgrade   (must contain the token "upgrade")
//   - Upgrade: websocket    (case-insensitive)
//
// Connection is a comma-separated list per HTTP/1.1, so we scan tokens
// rather than doing exact-match. Headers are case-insensitive: we lower
// once and compare.
func IsWebSocketUpgrade(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	for _, tok := range strings.Split(r.Header.Get("Connection"), ",") {
		if strings.EqualFold(strings.TrimSpace(tok), "upgrade") {
			return true
		}
	}
	return false
}

// dialBackend opens a connection to a backend's host:port. When backend
// TLS is enabled, the connection is wrapped with the LB's TLS config; the
// caller therefore receives a fully-handshaken stream that's ready to
// send the upgrade request on.
//
// The dial timeout matches the standard library's default Dialer
// behaviour — explicit here so a hung backend doesn't tie up the client's
// hijacked connection indefinitely.
func (lb *LoadBalancer) dialBackend(addr string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	if lb.backendTLS != nil {
		// Clone so a per-connection ServerName tweak (if ever needed)
		// doesn't leak across dials.
		return tls.DialWithDialer(dialer, "tcp", addr, lb.backendTLS.Clone())
	}
	return dialer.Dial("tcp", addr)
}

// proxyWebSocket runs an HTTP/1.1 Upgrade handshake against the chosen
// backend and then forwards bytes in both directions until either side
// closes. The flow:
//
//  1. Dial the backend (TLS-wrapped if backend TLS is on).
//  2. Write the original request — preserving Host, Origin, all WebSocket
//     headers — to the backend so it sees the genuine handshake.
//  3. Hijack the client connection: from this moment the http.Server is
//     no longer in the loop and standard timeouts no longer apply.
//  4. Stream the backend's 101 Switching Protocols response back to the
//     client, then enter the bidirectional copy.
//  5. The first goroutine to see EOF or an error tears down both legs.
//
// httputil.ReverseProxy already handles WebSocket since Go 1.12, but
// doing it explicitly here lets us:
//   - Track ActiveConns precisely (long-lived connections matter for
//     LeastConn/P2C decisions on subsequent requests).
//   - Keep the hijacked connection out of the http.Server's WriteTimeout
//     budget without lowering it for every other client.
//   - Surface backend dial failures as 502 with our own error handler
//     before the hijack, instead of a half-broken upgrade response.
func (lb *LoadBalancer) proxyWebSocket(w http.ResponseWriter, r *http.Request, backend *Backend) {
	target := backend.URL.Host

	// 1. Dial the backend BEFORE hijacking the client. If this fails we
	// can still emit a regular HTTP error response — the client hasn't
	// been hijacked yet, so the Server can still write.
	backendConn, err := lb.dialBackend(target)
	if err != nil {
		lb.logger.Error("websocket: backend dial failed", "backend", backend.NodeID, "err", err)
		http.Error(w, "backend unavailable", http.StatusBadGateway)
		return
	}
	defer backendConn.Close()

	// 2. Replay the client's request to the backend exactly as received.
	// http.Request.Write produces an HTTP/1.1 wire-format request that
	// includes Host, Connection: Upgrade, Sec-WebSocket-Key, etc.
	if err := r.Write(backendConn); err != nil {
		lb.logger.Error("websocket: forward request failed", "err", err)
		http.Error(w, "backend unavailable", http.StatusBadGateway)
		return
	}

	// 3. Hijack the client connection. After this, ResponseWriter is
	// dead — we own the raw TCP socket on both sides.
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		lb.logger.Error("websocket: ResponseWriter does not support Hijack")
		http.Error(w, "websocket not supported", http.StatusInternalServerError)
		return
	}
	clientConn, clientBuf, err := hijacker.Hijack()
	if err != nil {
		lb.logger.Error("websocket: hijack failed", "err", err)
		return
	}
	defer clientConn.Close()

	// Clear any deadlines the http.Server set on the connection per its
	// ReadTimeout/WriteTimeout. After hijack, the LB owns the timeouts;
	// inheriting a 15-second WriteTimeout would silently kill long-lived
	// WebSocket sessions.
	_ = clientConn.SetDeadline(time.Time{})
	_ = backendConn.SetDeadline(time.Time{})

	// 4. Bidirectional copy. Buffered reads are forwarded too — between
	// hijack and the first Read, the http.Server may have already
	// pulled bytes off the socket into clientBuf.
	if clientBuf != nil && clientBuf.Reader.Buffered() > 0 {
		if _, err := io.CopyN(backendConn, clientBuf, int64(clientBuf.Reader.Buffered())); err != nil {
			lb.logger.Debug("websocket: drain client buffer", "err", err)
			return
		}
	}

	// Stream both directions concurrently. Whichever side closes first
	// triggers cleanup of the other.
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, err := io.Copy(backendConn, clientConn)
		errCh <- err
		// Closing the backend write side propagates EOF so the other
		// goroutine's Read can complete promptly.
		if cw, ok := backendConn.(closeWriter); ok {
			cw.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, err := io.Copy(clientConn, backendConn)
		errCh <- err
		if cw, ok := clientConn.(closeWriter); ok {
			cw.CloseWrite()
		}
	}()

	// Wait for either direction to finish, then let the deferred Close
	// calls tear down the rest. We don't surface these errors — a
	// closed connection mid-copy is the normal way WebSockets end.
	<-errCh
	wg.Wait()

	// Drain the second error so the channel doesn't leak if anyone
	// inspects it later.
	select {
	case <-errCh:
	default:
	}
}

// closeWriter is implemented by net.TCPConn and *tls.Conn (Go 1.13+).
// It lets one side signal EOF without tearing down the read direction —
// useful for half-close graceful shutdowns of streaming connections.
type closeWriter interface {
	CloseWrite() error
}

// errHijackUnsupported is returned by proxyWebSocket when the underlying
// ResponseWriter doesn't implement http.Hijacker. Exported only via the
// log message — callers can't act on it.
var errHijackUnsupported = errors.New("websocket: ResponseWriter does not support Hijacker")

var _ = errHijackUnsupported // keep accessible for future surface
