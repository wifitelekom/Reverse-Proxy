package gossip

import (
	"context"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"
)

func silentTransportLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return false }

type notTimeoutErr struct{}

func (notTimeoutErr) Error() string   { return "no timeout" }
func (notTimeoutErr) Timeout() bool   { return false }
func (notTimeoutErr) Temporary() bool { return false }

// TestNormalizeAddr pins the ":port" → "127.0.0.1:port" rewrite.
// Windows rejects 0.0.0.0 as a UDP send target, so this rewrite is
// essential for the cross-platform send path.
func TestNormalizeAddr(t *testing.T) {
	if got := normalizeAddr(":9001"); got != "127.0.0.1:9001" {
		t.Fatalf("normalize of ':9001' returned unexpected: %q", got)
	}
	if got := normalizeAddr("127.0.0.1:9001"); got != "127.0.0.1:9001" {
		t.Fatalf("fully-qualified addr should not change, got: %q", got)
	}
}

// TestIsTimeout proves the helper distinguishes net.Error timeouts from
// other errors. The receiveLoop uses this to swallow its 100ms read
// deadline without spamming logs while still surfacing real failures.
func TestIsTimeout(t *testing.T) {
	if !isTimeout(timeoutErr{}) {
		t.Fatal("expected true for a net.Error with Timeout()=true")
	}
	if isTimeout(notTimeoutErr{}) {
		t.Fatal("expected false for an error with Timeout()=false")
	}
	if isTimeout(net.UnknownNetworkError("udp")) {
		t.Fatal("net.UnknownNetworkError must not be classified as timeout")
	}
}

// TestTransportSend_WhenPaused_DropsWithoutError verifies the
// partition-simulation contract: a paused transport drops outbound
// packets silently (no error) so callers can keep firing without
// special-casing the partitioned state.
func TestTransportSend_WhenPaused_DropsWithoutError(t *testing.T) {
	tr := &Transport{
		logger: silentTransportLogger(),
	}
	tr.SetPaused(true)

	err := tr.Send("127.0.0.1:9001", GossipMessage{Type: MsgPing, SenderID: "node-a"})
	if err != nil {
		t.Fatalf("paused Send must not error, got: %v", err)
	}
}

// TestTransportSend_InvalidAddr_ReturnsError confirms the error path:
// when ResolveUDPAddr can't parse the destination, Send must surface
// the error rather than silently dropping the packet.
func TestTransportSend_InvalidAddr_ReturnsError(t *testing.T) {
	addr := freeUDPAddr(t)
	tr, err := NewTransport(addr, 4, silentTransportLogger())
	if err != nil {
		t.Fatalf("could not create transport: %v", err)
	}
	defer tr.conn.Close()

	if err := tr.Send("://bad-addr", GossipMessage{Type: MsgPing}); err == nil {
		t.Fatal("expected an error for an unparseable destination addr")
	}
}

// TestTransportReceive_ChannelClosedOnContextCancel pins the shutdown
// contract: when ctx is canceled, receiveLoop exits and closes recvCh,
// so consumers blocked on Receive() get a clean ok=false signal.
func TestTransportReceive_ChannelClosedOnContextCancel(t *testing.T) {
	addr := freeUDPAddr(t)
	tr, err := NewTransport(addr, 4, silentTransportLogger())
	if err != nil {
		t.Fatalf("could not create transport: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	tr.Start(ctx)
	cancel()

	select {
	case _, ok := <-tr.Receive():
		if ok {
			t.Fatal("recv channel should be closed after context cancel")
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("recv channel did not close within budget")
	}
}
