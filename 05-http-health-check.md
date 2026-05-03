# HTTP-Seviye Health Check — Geliştirme Planı

## Amaç
Gossip tabanlı hata tespitinin yanına uygulama seviyesi (HTTP) sağlık kontrolü eklemek. Bir node ağ üzerinde erişilebilir ama uygulaması çökmüş/kilitli olabilir — bu senaryoyu gossip tek başına yakalayamaz.

## Arka Plan
Mevcut hata tespiti:
- **SWIM gossip:** UDP ping/ack ile node erişilebilirliği kontrol edilir
- **Kısıtlama:** UDP cevap veriyor ama HTTP servisi çökmüş olabilir (port açık, process alive ama deadlock/OOM/crash loop)
- **Gerçek dünya senaryoları:**
  - Go runtime çalışıyor (UDP handler aktif) ama HTTP handler panic/deadlock
  - Process alive ama disk dolu, veritabanı bağlantısı kopmuş
  - Container çalışıyor ama uygulama readiness sağlayamıyor

**Consul modeli:** Consul hem gossip hem HTTP/TCP/script health check destekler. Bir servis gossip ile alive ama health check failing ise "warning" durumuna geçer.

---

## Faz 1: Health Checker Altyapısı

### 1.1 `pkg/healthcheck/checker.go` — Yeni paket

```go
package healthcheck

import (
    "context"
    "net/http"
    "sync"
    "time"
    "log/slog"
)

// Status: Sağlık kontrolü sonucu
type Status int

const (
    StatusHealthy   Status = iota // HTTP 2xx
    StatusDegraded                 // HTTP 429 veya yavaş yanıt
    StatusUnhealthy                // HTTP 5xx, timeout, connection refused
    StatusUnknown                  // Henüz kontrol edilmemiş
)

// NodeHealth: Bir node'un sağlık durumu
type NodeHealth struct {
    NodeID        string
    Status        Status
    LastCheck     time.Time
    LastSuccess   time.Time
    ResponseTime  time.Duration
    ConsecutiveFails int
    HTTPStatusCode   int
}

// Config: Health check yapılandırması
type Config struct {
    Enabled          bool          // Health check aktif mi (default: false)
    Interval         time.Duration // Kontrol sıklığı (default: 5s)
    Timeout          time.Duration // HTTP request timeout (default: 2s)
    Path             string        // Health check endpoint (default: "/health")
    HealthyThreshold   int         // Kaç başarılı kontrol sonrası Healthy (default: 2)
    UnhealthyThreshold int         // Kaç başarısız kontrol sonrası Unhealthy (default: 3)
    DegradedLatency  time.Duration // Bu süreyi aşan yanıtlar Degraded (default: 1s)
}

// DefaultConfig: Varsayılan yapılandırma
func DefaultConfig() Config {
    return Config{
        Enabled:            false,
        Interval:           5 * time.Second,
        Timeout:            2 * time.Second,
        Path:               "/health",
        HealthyThreshold:   2,
        UnhealthyThreshold: 3,
        DegradedLatency:    1 * time.Second,
    }
}

// Checker: Periyodik HTTP health check yöneticisi
type Checker struct {
    config  Config
    client  *http.Client
    logger  *slog.Logger

    mu      sync.RWMutex
    nodes   map[string]*NodeHealth  // NodeID → sağlık durumu
    targets map[string]string       // NodeID → "host:port" (HTTP service address)

    // Callback: Durum değişikliğinde çağrılır
    onStatusChange func(nodeID string, old, new Status)
}

// NewChecker: Yeni health checker oluşturur
func NewChecker(cfg Config, logger *slog.Logger) *Checker {
    return &Checker{
        config: cfg,
        client: &http.Client{
            Timeout: cfg.Timeout,
            // Redirect takip etme
            CheckRedirect: func(req *http.Request, via []*http.Request) error {
                return http.ErrUseLastResponse
            },
        },
        logger:  logger,
        nodes:   make(map[string]*NodeHealth),
        targets: make(map[string]string),
    }
}

// SetOnStatusChange: Durum değişikliği callback'i ayarla
func (c *Checker) SetOnStatusChange(fn func(nodeID string, old, new Status))

// UpdateTargets: Kontrol edilecek node listesini güncelle
// Gossip membership değiştiğinde çağrılır
func (c *Checker) UpdateTargets(nodes map[string]string)

// GetHealth: Bir node'un sağlık durumunu döndür
func (c *Checker) GetHealth(nodeID string) NodeHealth

// GetAllHealth: Tüm node'ların sağlık durumu
func (c *Checker) GetAllHealth() map[string]NodeHealth

// IsHealthy: Node trafik alabilir mi? (Healthy veya Unknown)
func (c *Checker) IsHealthy(nodeID string) bool

// Start: Periyodik kontrol döngüsünü başlat
func (c *Checker) Start(ctx context.Context)
```

### 1.2 Health Check Döngüsü

```go
// Start içinde:
func (c *Checker) Start(ctx context.Context) {
    ticker := time.NewTicker(c.config.Interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            c.checkAll()
        }
    }
}

func (c *Checker) checkAll() {
    c.mu.RLock()
    targets := make(map[string]string, len(c.targets))
    for k, v := range c.targets {
        targets[k] = v
    }
    c.mu.RUnlock()

    // Paralel kontrol (her node için goroutine)
    var wg sync.WaitGroup
    for nodeID, addr := range targets {
        wg.Add(1)
        go func(id, a string) {
            defer wg.Done()
            c.checkNode(id, a)
        }(nodeID, addr)
    }
    wg.Wait()
}

func (c *Checker) checkNode(nodeID, addr string) {
    url := "http://" + addr + c.config.Path

    start := time.Now()
    resp, err := c.client.Get(url)
    elapsed := time.Since(start)

    c.mu.Lock()
    defer c.mu.Unlock()

    health, exists := c.nodes[nodeID]
    if !exists {
        health = &NodeHealth{NodeID: nodeID, Status: StatusUnknown}
        c.nodes[nodeID] = health
    }

    oldStatus := health.Status
    health.LastCheck = time.Now()
    health.ResponseTime = elapsed

    if err != nil {
        // Connection refused, timeout, DNS error
        health.ConsecutiveFails++
        health.HTTPStatusCode = 0
        if health.ConsecutiveFails >= c.config.UnhealthyThreshold {
            health.Status = StatusUnhealthy
        }
    } else {
        health.HTTPStatusCode = resp.StatusCode
        resp.Body.Close()

        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            if elapsed > c.config.DegradedLatency {
                health.Status = StatusDegraded
            } else {
                health.ConsecutiveFails = 0
                health.LastSuccess = time.Now()
                // HealthyThreshold: Kaç başarılı sonrası Healthy
                health.Status = StatusHealthy
            }
        } else if resp.StatusCode == 429 {
            health.Status = StatusDegraded
        } else {
            health.ConsecutiveFails++
            if health.ConsecutiveFails >= c.config.UnhealthyThreshold {
                health.Status = StatusUnhealthy
            }
        }
    }

    // Durum değişikliği callback
    if oldStatus != health.Status && c.onStatusChange != nil {
        go c.onStatusChange(nodeID, oldStatus, health.Status)
    }
}
```

---

## Faz 2: Load Balancer Entegrasyonu

### 2.1 `pkg/loadbalancer/proxy.go` — Health check filtresi

```go
type LoadBalancer struct {
    // ... mevcut alanlar ...
    healthChecker *healthcheck.Checker // nil ise health check kapalı
}

// handleRequest değişikliği:
func (lb *LoadBalancer) handleRequest(w http.ResponseWriter, r *http.Request) {
    aliveNodes := lb.membership.GetAliveMembers()
    backends := lb.getOrSyncBackends(aliveNodes)

    // Health check filtresi (aktifse)
    if lb.healthChecker != nil {
        backends = lb.filterHealthy(backends)
    }

    if len(backends) == 0 {
        http.Error(w, "kullanılabilir backend yok", http.StatusServiceUnavailable)
        return
    }

    backend := lb.selector.Select(backends)
    // ... mevcut proxy mantığı ...
}

// filterHealthy: Sadece sağlıklı backend'leri döndür
func (lb *LoadBalancer) filterHealthy(backends []*Backend) []*Backend {
    healthy := make([]*Backend, 0, len(backends))
    for _, b := range backends {
        if lb.healthChecker.IsHealthy(b.NodeID) {
            healthy = append(healthy, b)
        }
    }

    // Eğer hiç sağlıklı node yoksa, gossip-alive olanları döndür (graceful degradation)
    // Bu, health check'in yanlış pozitif üretmesi durumunda sistemi çökmekten korur
    if len(healthy) == 0 {
        lb.logger.Warn("hiç sağlıklı backend yok, gossip-alive olanlara fallback")
        return backends
    }

    return healthy
}
```

### 2.2 Membership Senkronizasyonu

Health checker'ın target listesi gossip membership ile senkron olmalı:

```go
// pkg/loadbalancer/proxy.go — getOrSyncBackends slow path'inde:

func (lb *LoadBalancer) getOrSyncBackends(nodes []gossip.NodeInfo) []*Backend {
    // ... mevcut pool rebuild ...

    // Health checker target'larını güncelle
    if lb.healthChecker != nil {
        targets := make(map[string]string, len(nodes))
        for _, n := range nodes {
            targets[n.ID] = n.ServiceAddr
        }
        lb.healthChecker.UpdateTargets(targets)
    }

    // ... return sorted cache ...
}
```

### 2.3 LB Health Endpoint

LB'nin kendi health endpoint'i (tüm backend durumlarını gösterir):

```go
// cmd/loadbalancer/main.go — HTTP handler eklentisi

mux.HandleFunc("/lb/health", func(w http.ResponseWriter, r *http.Request) {
    if healthChecker == nil {
        json.NewEncoder(w).Encode(map[string]string{"status": "health_check_disabled"})
        return
    }
    allHealth := healthChecker.GetAllHealth()
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(allHealth)
})
```

---

## Faz 3: Degraded Mode & Weighted Routing

### Konsept
Degraded (yavaş ama çalışan) backend'leri tamamen çıkarmak yerine, daha az trafik yönlendirmek:

```go
// Selector'a opsiyonel weight bilgisi ekle (gelecek genişleme)
// Şu an için basit yaklaşım: Degraded node'ları P2C/LeastConn'da dezavantajlı yap

// Yaklaşım: Degraded backend'lerin ActiveConns'una sanal ağırlık ekle
// Bu, LeastConn ve P2C'nin doğal olarak degraded node'lardan kaçınmasını sağlar
func (lb *LoadBalancer) adjustedActiveConns(b *Backend) int32 {
    conns := atomic.LoadInt32(&b.ActiveConns)
    if lb.healthChecker != nil {
        health := lb.healthChecker.GetHealth(b.NodeID)
        if health.Status == healthcheck.StatusDegraded {
            conns += 10 // Sanal ağırlık: degraded node'u daha az tercih et
        }
    }
    return conns
}
```

> **Not:** Bu Faz opsiyonel. İlk implementasyonda binary karar (healthy/unhealthy) yeterli.

---

## Faz 4: Konfigürasyon

### 4.1 `pkg/types/config.go`

```go
type LBConfig struct {
    // ... mevcut alanlar ...
    HealthCheck healthcheck.Config
}
```

### 4.2 `cmd/loadbalancer/main.go` — CLI flag'leri

```go
hcEnabled     := flag.Bool("health-check", false, "HTTP health check aktifleştir")
hcInterval    := flag.Duration("health-interval", 5*time.Second, "Health check sıklığı")
hcTimeout     := flag.Duration("health-timeout", 2*time.Second, "Health check timeout")
hcPath        := flag.String("health-path", "/health", "Health check endpoint")
hcUnhealthy   := flag.Int("health-unhealthy-threshold", 3, "Unhealthy threshold")
```

---

## Faz 5: Testler

### `pkg/healthcheck/checker_test.go` — Yeni dosya

```go
// TestChecker_HealthyNode
// - httptest server: 200 OK döner
// - checkNode çağır
// - Status == StatusHealthy

// TestChecker_UnhealthyAfterThreshold
// - httptest server: 500 döner
// - UnhealthyThreshold=3
// - 1. kontrol: Status != Unhealthy (henüz threshold'a ulaşılmadı)
// - 2. kontrol: Status != Unhealthy
// - 3. kontrol: Status == Unhealthy ✓

// TestChecker_DegradedOnSlowResponse
// - httptest server: 200 ama 2s delay
// - DegradedLatency: 1s
// - Status == StatusDegraded

// TestChecker_DegradedOn429
// - httptest server: 429 Too Many Requests
// - Status == StatusDegraded

// TestChecker_RecoveryAfterThreshold
// - Node önce Unhealthy
// - Sonra 200 OK dönmeye başlar
// - HealthyThreshold=2 kontrol sonrası Healthy

// TestChecker_ConnectionRefused
// - Kapalı port
// - ConsecutiveFails artıyor → Unhealthy

// TestChecker_StatusChangeCallback
// - onStatusChange callback çağrılıyor
// - Doğru old/new status parametreleri

// TestChecker_UpdateTargets
// - Yeni node ekleniyor, eski node siliniyor
// - Sadece güncel target'lar kontrol ediliyor

// TestChecker_ParallelChecks
// - 10 node, hepsi paralel kontrol ediliyor
// - Tüm sonuçlar doğru

// TestChecker_UnknownInitialStatus
// - Yeni eklenen node'un Status == StatusUnknown
// - IsHealthy(Unknown) == true (grace period)
```

### `pkg/loadbalancer/proxy_test.go` — Ek testler

```go
// TestProxy_HealthCheckFiltersUnhealthy
// - 3 backend, 1'i Unhealthy
// - Unhealthy backend'e trafik gitmez

// TestProxy_HealthCheckFallbackOnAllUnhealthy
// - Tüm backend'ler Unhealthy
// - Graceful degradation: gossip-alive olanlara fallback
// - 503 dönmez (önemli!)

// TestProxy_HealthCheckDisabled
// - healthChecker == nil
// - Tüm gossip-alive backend'ler kullanılır (mevcut davranış)
```

---

## Gossip vs HTTP Health Check Karşılaştırması

| Özellik | Gossip (SWIM) | HTTP Health Check |
|---------|---------------|-------------------|
| Ne kontrol eder | Ağ erişilebilirliği (UDP) | Uygulama sağlığı (HTTP) |
| Tespit süresi | ~4.5s (ping+indirect+suspect) | configurable (default 15s: 3×5s) |
| Yanlış pozitif | Düşük (Lifeguard ile) | Düşük (threshold ile) |
| Network overhead | Minimal (piggyback) | 1 HTTP request/node/interval |
| Ne yakalar | Network partition, process crash | Deadlock, OOM, dependency failure |
| Birlikte kullanım | **Önerilen:** Gossip → coarse-grained, HTTP → fine-grained |

---

## Dosya Değişiklik Özeti

| Dosya | Değişiklik | Yeni/Mevcut |
|-------|------------|-------------|
| `pkg/healthcheck/checker.go` | Checker struct + tüm logic | **Yeni paket** |
| `pkg/healthcheck/checker_test.go` | Tüm health check testleri | **Yeni** |
| `pkg/loadbalancer/proxy.go` | filterHealthy + healthChecker entegrasyonu | Mevcut |
| `pkg/types/config.go` | HealthCheck config | Mevcut |
| `cmd/loadbalancer/main.go` | Health check flag'leri + /lb/health endpoint | Mevcut |

## Bağımlılıklar
- Sıfır dış bağımlılık (net/http, time, sync hepsi Go stdlib)

## Başarı Kriterleri
- [ ] Mevcut 26 test hâlâ geçer
- [ ] Sağlıklı node'lar trafik alır, unhealthy olanlar almaz
- [ ] Tüm node'lar unhealthy ise graceful degradation (503 dönmez)
- [ ] Threshold mekanizması: Geçici hatalar yanlış pozitif üretmez
- [ ] Degraded: Yavaş node'lar tespit edilir
- [ ] Recovery: Düzelen node'lar tekrar trafik alır
- [ ] `/lb/health` endpoint tüm durumları JSON olarak gösterir
- [ ] Health check kapalıyken davranış değişmez
