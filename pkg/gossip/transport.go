package gossip

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"sync/atomic"
	"time"
)

// ReceivedMessage is one UDP datagram delivered up from the transport.
type ReceivedMessage struct {
	From *net.UDPAddr
	Msg  GossipMessage
}

// Transport sends and receives gossip messages over UDP. A single
// goroutine listens; a buffered channel fans messages out to the
// protocol layer. When paused is true both inbound and outbound
// packets are dropped — used for partition simulation in tests.
type Transport struct {
	conn   *net.UDPConn
	recvCh chan ReceivedMessage
	logger *slog.Logger
	paused atomic.Bool // partition-simulation flag
}

// SetPaused pauses or resumes both directions of the transport.
//
//	true  → both inbound and outbound UDP are dropped (bidirectional
//	         partition simulation).
//	false → normal operation.
func (t *Transport) SetPaused(v bool) {
	t.paused.Store(v)
	if v {
		t.logger.Warn("transport paused — gossip send/receive disabled (partition simulation)")
	} else {
		t.logger.Info("transport resumed — gossip send/receive enabled")
	}
}

// NewTransport opens a UDP listener on listenAddr and returns the
// configured transport. recvBuf is the size of the inbound channel
// buffer (recommended: 100); a full buffer drops packets following
// UDP semantics.
func NewTransport(listenAddr string, recvBuf int, logger *slog.Logger) (*Transport, error) {
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	return &Transport{
		conn:   conn,
		recvCh: make(chan ReceivedMessage, recvBuf),
		logger: logger,
	}, nil
}

// Start launches the receive loop, which exits cleanly when ctx is
// canceled. Non-blocking: the goroutine runs in the background and
// the function returns immediately.
func (t *Transport) Start(ctx context.Context) {
	go t.receiveLoop(ctx)
}

// receiveLoop continuously reads UDP packets and pushes them to recvCh.
// Cancellation pattern: SetReadDeadline(100ms) + ctx.Done() check
// allows ctx-driven shutdown without racing on conn.Close().
func (t *Transport) receiveLoop(ctx context.Context) {
	defer close(t.recvCh)
	buf := make([]byte, 65535) // UDP max packet size

	for {
		select {
		case <-ctx.Done():
			t.conn.Close()
			return
		default:
		}

		// 100ms read deadline lets us check ctx without blocking forever.
		t.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

		n, addr, err := t.conn.ReadFromUDP(buf)
		if err != nil {
			if isTimeout(err) {
				continue // deadline expired — loop back to ctx check
			}
			if ctx.Err() != nil {
				return // context canceled
			}
			t.logger.Error("UDP read error", "err", err)
			continue
		}

		// Partition simulation: drop the packet if paused.
		if t.paused.Load() {
			continue
		}

		var msg GossipMessage
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			t.logger.Warn("invalid gossip message", "from", addr, "err", err)
			continue
		}

		// Non-blocking send — drop if the channel is full (UDP semantics).
		select {
		case t.recvCh <- ReceivedMessage{From: addr, Msg: msg}:
		default:
			t.logger.Warn("recv channel full, dropping message", "from", addr)
		}
	}
}

// Send writes one UDP datagram. Thread-safe: net.UDPConn.WriteToUDP
// is concurrent-safe. When the transport is paused, sends are silently
// dropped to maintain bidirectional partition semantics. Address
// normalization: ":port" → "127.0.0.1:port" because Windows UDP
// rejects 0.0.0.0 as a destination.
func (t *Transport) Send(addr string, msg GossipMessage) error {
	addr = normalizeAddr(addr)

	// Partition simulation: drop outbound when paused (bidirectional isolation).
	if t.paused.Load() {
		t.logger.Debug("send dropped (partition active)", "addr", addr)
		return nil
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}

	_, err = t.conn.WriteToUDP(data, udpAddr)
	return err
}

// normalizeAddr converts ":port" to "127.0.0.1:port".
// On Windows, 0.0.0.0 is invalid as a UDP send target.
func normalizeAddr(addr string) string {
	if len(addr) > 0 && addr[0] == ':' {
		return "127.0.0.1" + addr
	}
	return addr
}

// Receive returns the channel of incoming messages.
// The protocol loop drains this via select.
func (t *Transport) Receive() <-chan ReceivedMessage {
	return t.recvCh
}

// isTimeout reports whether err is a net.Error timeout (deadline expired).
func isTimeout(err error) bool {
	netErr, ok := err.(net.Error)
	return ok && netErr.Timeout()
}
