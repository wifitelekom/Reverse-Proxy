package gossip

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"sync/atomic"
	"time"
)

// ReceivedMessage gelen bir UDP paketini temsil eder.
type ReceivedMessage struct {
	From *net.UDPAddr
	Msg  GossipMessage
}

// Transport UDP üzerinden gossip mesajları gönderir/alır.
// Tek bir goroutine ile dinler, fan-out için buffered channel kullanır.
// paused: true iken hem gelen hem giden UDP paketler drop edilir (gerçek partition simülasyonu).
type Transport struct {
	conn   *net.UDPConn
	recvCh chan ReceivedMessage
	logger *slog.Logger
	paused atomic.Bool // partition simülasyonu için
}

// SetPaused gossip alımını ve gönderimini duraklatır/devam ettirir.
// true: hem gelen hem giden UDP drop edilir (çift yönlü partition simülasyonu)
// false: normal çalışma
func (t *Transport) SetPaused(v bool) {
	t.paused.Store(v)
	if v {
		t.logger.Warn("transport paused — gossip send/receive disabled (partition simulation)")
	} else {
		t.logger.Info("transport resumed — gossip send/receive enabled")
	}
}

// NewTransport belirtilen adreste UDP dinleyen transport oluşturur.
// recvBuf: channel buffer boyutu (önerilen: 100)
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

// Start context ile yönetilen receive loop'u başlatır.
// Non-blocking: goroutine arka planda çalışır, fonksiyon hemen döner.
func (t *Transport) Start(ctx context.Context) {
	go t.receiveLoop(ctx)
}

// receiveLoop sürekli UDP paketi bekler.
// Context iptal paterni: SetReadDeadline(100ms) + ctx.Done() kontrolü.
// Bu sayede conn.Close() race condition olmadan yapılır.
func (t *Transport) receiveLoop(ctx context.Context) {
	defer close(t.recvCh)
	buf := make([]byte, 65535) // UDP max boyutu

	for {
		select {
		case <-ctx.Done():
			t.conn.Close()
			return
		default:
		}

		// 100ms deadline: context iptalini bloke olmadan kontrol etmek için
		t.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

		n, addr, err := t.conn.ReadFromUDP(buf)
		if err != nil {
			if isTimeout(err) {
				continue // Deadline timeout — context kontrolüne dön
			}
			if ctx.Err() != nil {
				return // Context iptal edildi
			}
			t.logger.Error("UDP read error", "err", err)
			continue
		}

		// Partition simülasyonu: paused ise paketi drop et
		if t.paused.Load() {
			continue
		}

		var msg GossipMessage
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			t.logger.Warn("invalid gossip message", "from", addr, "err", err)
			continue
		}

		// Non-blocking send — channel dolu ise drop (UDP semantics)
		select {
		case t.recvCh <- ReceivedMessage{From: addr, Msg: msg}:
		default:
			t.logger.Warn("recv channel full, dropping message", "from", addr)
		}
	}
}

// Send tek bir UDP datagram gönderir.
// Thread-safe: UDPConn.WriteToUDP concurrent-safe.
// Partition simülasyonu: paused iken gönderim sessizce drop edilir.
// Adres normalizasyonu: ":port" → "127.0.0.1:port" (Windows UDP hedefi için).
func (t *Transport) Send(addr string, msg GossipMessage) error {
	addr = normalizeAddr(addr)

	// Partition simülasyonu: paused iken outbound da drop et (çift yönlü izolasyon)
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

// normalizeAddr ":port" formatını "127.0.0.1:port" olarak dönüştürür.
// Windows'ta 0.0.0.0 UDP gönderim hedefi olarak geçersizdir.
func normalizeAddr(addr string) string {
	if len(addr) > 0 && addr[0] == ':' {
		return "127.0.0.1" + addr
	}
	return addr
}

// Receive gelen mesajlar için channel döner.
// Protocol loop bu channel'ı select ile dinler.
func (t *Transport) Receive() <-chan ReceivedMessage {
	return t.recvCh
}

// isTimeout net.Error timeout kontrolü.
func isTimeout(err error) bool {
	netErr, ok := err.(net.Error)
	return ok && netErr.Timeout()
}
