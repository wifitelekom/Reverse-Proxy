# HTTP/2, gRPC ve WebSocket Desteği — Geliştirme Planı

## Amaç
Modern protokol desteği ekleyerek projeyi prodüksiyon senaryolarına uygun hale getirmek: HTTP/2 multiplexing, gRPC proxy, WebSocket upgrade.

## Arka Plan
Mevcut durum:
- Sadece HTTP/1.1 (httputil.ReverseProxy)
- `httputil.SingleHostReverseProxy` kullanılıyor
- Backend bağlantıları kısa ömürlü (her request yeni TCP veya keepalive pool)

Eksiklikler:
- **HTTP/2:** Multiplexing yok, head-of-line blocking, header compression yok
- **gRPC:** HTTP/2 tabanlı, mevcut proxy desteklemiyor (trailers, streaming)
- **WebSocket:** Upgrade mekanizması httputil.ReverseProxy ile sorunlu

---

## Faz 1: HTTP/2 Desteği

### Konsept
Go'nun `net/http` paketi HTTP/2'yi TLS ile otomatik destekler. Ancak cleartext HTTP/2 (h2c) ve backend bağlantıları için ek yapılandırma gerekir.

### 1.1 Frontend HTTP/2 (Client → LB)

**TLS ile HTTP/2 (h2):**
Go otomatik olarak destekler. `ListenAndServeTLS` çağrıldığında HTTP/2 varsayılan olarak aktif. Faz 3 (TLS) tamamlandığında otomatik gelir.

**Cleartext HTTP/2 (h2c) — TLS olmadan:**

```go
// pkg/loadbalancer/h2c.go — Yeni dosya

package loadbalancer

import (
    "net/http"
    "golang.org/x/net/http2"      // NOT: ilk dış bağımlılık
    "golang.org/x/net/http2/h2c"
)

// WrapH2C: HTTP handler'ı h2c desteği ile sarar
// h2c = HTTP/2 cleartext (TLS olmadan HTTP/2)
func WrapH2C(handler http.Handler) http.Handler {
    h2s := &http2.Server{}
    return h2c.NewHandler(handler, h2s)
}
```

> **NOT:** `golang.org/x/net` Go'nun resmi extended paketi. Sıfır bağımlılık prensibini teknik olarak bozar ama Go ekosisteminde "quasi-stdlib" kabul edilir. Alternatif: h2c desteğini atla, sadece TLS ile HTTP/2 (sıfır bağımlılık korunur).

### 1.2 Backend HTTP/2 (LB → Backend)

```go
// pkg/loadbalancer/proxy.go — newBackend değişikliği

func (lb *LoadBalancer) newBackend(node gossip.NodeInfo) *Backend {
    // ...
    proxy := httputil.NewSingleHostReverseProxy(target)

    transport := &http.Transport{
        // Mevcut ayarlar korunur
        MaxIdleConns:        100,
        IdleConnTimeout:     30 * time.Second,
        ResponseHeaderTimeout: 2 * time.Second,
    }

    if lb.config.HTTP2Backend {
        // HTTP/2 backend desteği
        // ConfigureTransports: hem h1 hem h2 destekler
        http2.ConfigureTransport(transport)
    }

    proxy.Transport = transport
    // ...
}
```

### 1.3 Config

```go
// pkg/types/config.go
type LBConfig struct {
    // ... mevcut alanlar ...
    EnableH2C    bool // Cleartext HTTP/2 (default: false)
    HTTP2Backend bool // Backend'lere HTTP/2 bağlantı (default: false)
}
```

---

## Faz 2: WebSocket Desteği

### Konsept
WebSocket, HTTP Upgrade mekanizması ile başlar ve sonra TCP bağlantısı bidirectional mesajlaşmaya dönüşür. `httputil.ReverseProxy` WebSocket Upgrade'i düzgün handle etmez çünkü:
1. Hijack mekanizmasını otomatik desteklemez
2. Response body'yi buffer'lar (streaming yok)
3. Hop-by-hop header'ları siler (Connection: Upgrade)

### 2.1 `pkg/loadbalancer/websocket.go` — Yeni dosya

```go
package loadbalancer

import (
    "io"
    "net"
    "net/http"
    "strings"
    "time"
)

// IsWebSocketUpgrade: Request WebSocket upgrade mı kontrol eder
func IsWebSocketUpgrade(r *http.Request) bool {
    return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
           strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

// ProxyWebSocket: WebSocket bağlantısını backend'e proxy'ler
// Hijack + bidirectional copy ile çalışır (httputil.ReverseProxy bypass)
func ProxyWebSocket(w http.ResponseWriter, r *http.Request, target string) error {
    // 1. Backend'e TCP bağlantısı aç
    backendConn, err := net.DialTimeout("tcp", target, 5*time.Second)
    if err != nil {
        http.Error(w, "backend bağlantı hatası", http.StatusBadGateway)
        return err
    }
    defer backendConn.Close()

    // 2. Client bağlantısını hijack et
    hijacker, ok := w.(http.Hijacker)
    if !ok {
        http.Error(w, "hijack desteklenmiyor", http.StatusInternalServerError)
        return errors.New("hijack not supported")
    }
    clientConn, _, err := hijacker.Hijack()
    if err != nil {
        return err
    }
    defer clientConn.Close()

    // 3. Orijinal HTTP request'i backend'e gönder (upgrade dahil)
    if err := r.Write(backendConn); err != nil {
        return err
    }

    // 4. Bidirectional copy (iki goroutine)
    errCh := make(chan error, 2)
    go func() {
        _, err := io.Copy(clientConn, backendConn) // backend → client
        errCh <- err
    }()
    go func() {
        _, err := io.Copy(backendConn, clientConn) // client → backend
        errCh <- err
    }()

    // İlk hata veya bağlantı kapanması
    <-errCh
    return nil
}
```

### 2.2 `pkg/loadbalancer/proxy.go` — WebSocket entegrasyonu

```go
func (lb *LoadBalancer) handleRequest(w http.ResponseWriter, r *http.Request) {
    // ... backend seçimi ...

    // WebSocket Upgrade kontrolü
    if IsWebSocketUpgrade(r) {
        target := backend.URL.Host
        if err := ProxyWebSocket(w, r, target); err != nil {
            lb.logger.Error("websocket proxy hatası", "err", err)
        }
        return
    }

    // Normal HTTP request (mevcut mantık)
    atomic.AddInt32(&backend.ActiveConns, 1)
    defer atomic.AddInt32(&backend.ActiveConns, -1)
    backend.proxy.ServeHTTP(w, r)
}
```

### Not: WebSocket ve ActiveConns
WebSocket bağlantıları uzun ömürlüdür. `ActiveConns`'u WebSocket için de artır/azalt. Bu LeastConn ve P2C algoritmaları için kritik — WebSocket yoğun backend'lere yeni bağlantı yönlendirilmemeli.

---

## Faz 3: gRPC Proxy

### Konsept
gRPC, HTTP/2 üzerinde çalışır ve özel gereksinimleri vardır:
1. HTTP/2 zorunlu (multiplexing, streaming)
2. Trailers desteği (gRPC status kodları trailer'da)
3. Content-Type: application/grpc
4. Bidirectional streaming

### Yaklaşım Seçimi

**Yaklaşım A: Transparent HTTP/2 Proxy (Önerilen)**
- HTTP/2 frame'lerini olduğu gibi ilet
- gRPC-specific logic yok, sadece HTTP/2 proxy
- En basit, en az hata riski

**Yaklaşım B: gRPC-aware Proxy**
- gRPC metadata'sını parse et
- Service/method tabanlı routing
- Çok daha karmaşık, şimdilik gereksiz

### 3.1 `pkg/loadbalancer/grpc.go` — Yeni dosya

```go
package loadbalancer

import (
    "net/http"
    "net/http/httputil"
    "strings"
)

// IsGRPC: Request gRPC mı kontrol eder
func IsGRPC(r *http.Request) bool {
    return strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc")
}

// NewGRPCProxy: gRPC için özelleştirilmiş reverse proxy oluşturur
// Farklar:
// - FlushInterval: -1 (immediate flush, streaming için)
// - Trailer header'ları korunur
// - HTTP/2 transport zorunlu
func NewGRPCProxy(target *url.URL, transport http.RoundTripper) *httputil.ReverseProxy {
    proxy := httputil.NewSingleHostReverseProxy(target)
    proxy.Transport = transport
    proxy.FlushInterval = -1 // Streaming: her byte hemen flush

    // Director: gRPC header'larını koru
    originalDirector := proxy.Director
    proxy.Director = func(req *http.Request) {
        originalDirector(req)
        // gRPC trailer'ları ve content-type'ı koru
        req.Header.Set("Te", "trailers")
    }

    // ModifyResponse: Trailer'ları response'a kopyala
    proxy.ModifyResponse = func(resp *http.Response) error {
        // gRPC status trailers'ı response'a ekle
        return nil
    }

    return proxy
}
```

### 3.2 Backend struct güncelleme

```go
type Backend struct {
    // ... mevcut alanlar ...
    grpcProxy *httputil.ReverseProxy // gRPC için ayrı proxy (FlushInterval=-1)
}
```

### 3.3 `pkg/loadbalancer/proxy.go` — gRPC routing

```go
func (lb *LoadBalancer) handleRequest(w http.ResponseWriter, r *http.Request) {
    // ... backend seçimi ...

    if IsWebSocketUpgrade(r) {
        // ... WebSocket proxy ...
        return
    }

    atomic.AddInt32(&backend.ActiveConns, 1)
    defer atomic.AddInt32(&backend.ActiveConns, -1)

    if IsGRPC(r) {
        backend.grpcProxy.ServeHTTP(w, r)
        return
    }

    // Normal HTTP
    backend.proxy.ServeHTTP(w, r)
}
```

---

## Faz 4: Testler

### `pkg/loadbalancer/websocket_test.go`

```go
// TestIsWebSocketUpgrade_True
// Upgrade: websocket + Connection: Upgrade → true

// TestIsWebSocketUpgrade_False
// Normal HTTP request → false

// TestProxyWebSocket_BidirectionalCopy
// - httptest server: WebSocket echo server (gorilla/websocket veya nhooyr.io/websocket)
// - LB üzerinden WebSocket bağlantısı
// - Mesaj gönder → echo al
// NOT: Test bağımlılığı olarak websocket kütüphanesi gerekebilir
//      veya raw TCP ile test (daha karmaşık ama bağımlılıksız)

// TestProxyWebSocket_BackendDown
// Backend kapalı → 502 Bad Gateway
```

### `pkg/loadbalancer/grpc_test.go`

```go
// TestIsGRPC_True
// Content-Type: application/grpc → true

// TestIsGRPC_False
// Content-Type: application/json → false

// TestGRPCProxy_FlushInterval
// proxy.FlushInterval == -1 doğrulaması

// TestGRPCProxy_TrailersPreserved
// gRPC-style response with trailers → trailer'lar client'a ulaşır
// (net/http test server ile simüle et)
```

### `pkg/loadbalancer/h2c_test.go`

```go
// TestH2C_CleartextHTTP2
// - h2c handler ile LB başlat
// - HTTP/2 cleartext client ile request
// - Proto == "HTTP/2.0" doğrula
```

---

## Karar Noktaları

### Dış bağımlılık kararı

| Seçenek | Bağımlılık | Avantaj | Dezavantaj |
|---------|------------|---------|------------|
| **A: Sadece TLS HTTP/2** | Yok | Sıfır bağımlılık korunur | h2c yok |
| **B: golang.org/x/net** | Quasi-stdlib | h2c + tam HTTP/2 kontrolü | Teknik olarak dış bağımlılık |
| **C: Tam gRPC** | google.golang.org/grpc | Native gRPC proxy | Büyük bağımlılık ağacı |

**Önerim:** Seçenek B — `golang.org/x/net` Go ekosisteminde fiilen stdlib'dir. gRPC proxy'sini transparent HTTP/2 olarak yapmak (`Seçenek B`) yeterlidir, `google.golang.org/grpc`'ye gerek yoktur.

### WebSocket kütüphanesi kararı

Test için WebSocket client gerekli. İki seçenek:
1. **Raw TCP** ile test (bağımlılıksız ama karmaşık)
2. **nhooyr.io/websocket** test bağımlılığı olarak (sadece _test.go'da)

**Önerim:** Raw TCP ile temel test, WebSocket frame parsing'i test scope'unda.

---

## Dosya Değişiklik Özeti

| Dosya | Değişiklik | Yeni/Mevcut |
|-------|------------|-------------|
| `pkg/loadbalancer/h2c.go` | H2C wrapper | **Yeni** |
| `pkg/loadbalancer/websocket.go` | WebSocket proxy (hijack + copy) | **Yeni** |
| `pkg/loadbalancer/grpc.go` | gRPC transparent proxy | **Yeni** |
| `pkg/loadbalancer/h2c_test.go` | H2C testleri | **Yeni** |
| `pkg/loadbalancer/websocket_test.go` | WebSocket testleri | **Yeni** |
| `pkg/loadbalancer/grpc_test.go` | gRPC testleri | **Yeni** |
| `pkg/loadbalancer/proxy.go` | Request routing (WS/gRPC/HTTP) + Backend.grpcProxy | Mevcut |
| `pkg/types/config.go` | EnableH2C, HTTP2Backend | Mevcut |
| `cmd/loadbalancer/main.go` | H2C flag | Mevcut |
| `go.mod` | golang.org/x/net (opsiyonel) | Mevcut |

## Uygulama Sırası
1. **HTTP/2 (TLS ile)** — En az değişiklik, TLS planı ile paralel
2. **WebSocket** — Bağımsız, hijack mekanizması
3. **gRPC** — HTTP/2 bağımlı, Faz 1 sonrası
4. **H2C** — Opsiyonel, golang.org/x/net kararına bağlı

## Başarı Kriterleri
- [ ] Mevcut 26 test hâlâ geçer
- [ ] HTTP/2 + TLS: `curl --http2 https://localhost:8080` çalışır
- [ ] WebSocket: Echo server üzerinden bidirectional mesajlaşma
- [ ] gRPC: Unary RPC transparent proxy ile çalışır
- [ ] gRPC streaming: Server-streaming response flush ediliyor
- [ ] ActiveConns: WebSocket bağlantıları doğru sayılıyor
- [ ] h2c (opsiyonel): Cleartext HTTP/2 çalışır
