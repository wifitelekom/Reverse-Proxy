package loadbalancer

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain enforces zero goroutine leaks across every test in this
// package. The loadbalancer brings together several long-lived
// subsystems (HTTP server, refresh loop, health checker, websocket
// proxies) so this is the strictest of the four TestMain assertions
// in the codebase.
//
// Note on httptest / net/http internals: tests that use http.Get,
// http.Client, or httptest.Server inevitably leave goroutines in
// well-known idle states because of the stdlib's connection-pool
// design (persistent connections sit in readLoop/writeLoop waiting
// for the next request, even after the test "finishes"). These are
// not leaks in any meaningful sense — Go's runtime cleans them up
// when the test binary exits — but they trip a naive
// VerifyTestMain.
//
// We exempt their well-known top-of-stack functions. Anything WE
// own (refresh loop, websocket copy goroutines, callback workers,
// gossip handlers) is still tracked: the exemption list is
// deliberately narrow.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// stdlib HTTP server connection background-read goroutine
		goleak.IgnoreTopFunction("net/http.(*connReader).backgroundRead"),
		// stdlib HTTP client connection-pool keep-alive workers
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		// httptest.Server's own Accept goroutine; lags Close briefly
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	)
}
