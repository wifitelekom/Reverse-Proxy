# Power of Two Choices (P2C) Algoritması — Geliştirme Planı

## Amaç
Çoklu LB instance'larında "herd behavior" sorununu önleyen P2C algoritmasını eklemek. İsteğe bağlı olarak EWMA (Exponentially Weighted Moving Average) ile gecikme tabanlı akıllı routing sağlamak.

## Arka Plan
Mevcut algoritmalar:
- **RoundRobin:** O(1), eşit dağılım ama sunucu kapasitesi/yükü göz ardı edilir
- **LeastConnections:** O(N), en az bağlantılı backend ama birden fazla LB instance'ı aynı anda aynı backend'i seçer ("herd behavior" / thundering herd)

**P2C çözümü:** 2 rastgele backend seç, daha az yüklü olanı kullan. Bu basit değişiklik herd behavior'u matematiksel olarak ortadan kaldırır (O(log log N) max load vs O(log N)).

**Referans:** Michael Mitzenmacher, "The Power of Two Choices in Randomized Load Balancing" (1996)

---

## Faz 1: Temel P2C Selector

### 1.1 `pkg/loadbalancer/p2c.go` — Yeni dosya

```go
package loadbalancer

import (
    "math/rand"
    "sync/atomic"
)

// P2C — Power of Two Choices yük dengeleme algoritması.
// İki rastgele backend seçer, daha az bağlantısı olanı döndürür.
type P2C struct {
    rng *rand.Rand // Deterministic random (test edilebilirlik için seed)
    mu  sync.Mutex // rng thread-safe değil
}

func NewP2C() *P2C {
    return &P2C{
        rng: rand.New(rand.NewSource(time.Now().UnixNano())),
    }
}

// Select: 2 rastgele backend seç, ActiveConns'u düşük olanı döndür.
// N=1 ise tek backend döndürür.
// N=0 ise nil döndürür.
func (p *P2C) Select(backends []*Backend) *Backend {
    n := len(backends)
    if n == 0 {
        return nil
    }
    if n == 1 {
        return backends[0]
    }

    p.mu.Lock()
    i := p.rng.Intn(n)
    j := p.rng.Intn(n - 1)
    if j >= i {
        j++ // i != j garantisi (Fisher-Yates benzeri)
    }
    p.mu.Unlock()

    bi := backends[i]
    bj := backends[j]

    if atomic.LoadInt32(&bi.ActiveConns) <= atomic.LoadInt32(&bj.ActiveConns) {
        return bi
    }
    return bj
}
```

### Neden `rng` mutex ile?
`math/rand.Rand` thread-safe değil. Alternatif: `math/rand/v2` (Go 1.22+) ama minimum Go 1.21 desteğimiz var. Mutex overhead P2C'nin O(1) yapısını bozmaz (uncontended lock ~20ns).

---

## Faz 2: P2C + EWMA (Gecikme Tabanlı)

### Konsept
ActiveConns yerine veya buna ek olarak, her backend'in ortalama response süresi (latency) de karar faktörü olabilir. EWMA bu latency'yi smooth'lar.

### 2.1 `pkg/loadbalancer/proxy.go` — Backend struct güncelleme

```go
type Backend struct {
    // ... mevcut alanlar ...
    LatencyEWMA int64 // Nanosecond cinsinden EWMA latency (atomic)
}
```

### 2.2 EWMA hesaplama

```go
const ewmaDecay = 0.5 // α = 0.5 (son ölçümler ağırlıklı)

// RecordLatency: Bir request tamamlandığında çağrılır
func (b *Backend) RecordLatency(d time.Duration) {
    ns := d.Nanoseconds()
    for {
        old := atomic.LoadInt64(&b.LatencyEWMA)
        if old == 0 {
            // İlk ölçüm
            if atomic.CompareAndSwapInt64(&b.LatencyEWMA, old, ns) {
                return
            }
            continue
        }
        // EWMA = α * new + (1-α) * old
        newVal := int64(ewmaDecay*float64(ns) + (1-ewmaDecay)*float64(old))
        if atomic.CompareAndSwapInt64(&b.LatencyEWMA, old, newVal) {
            return
        }
    }
}
```

### 2.3 `pkg/loadbalancer/p2c.go` — EWMA destekli P2C

```go
type P2CEWMA struct {
    rng *rand.Rand
    mu  sync.Mutex
}

func NewP2CEWMA() *P2CEWMA { /* ... */ }

func (p *P2CEWMA) Select(backends []*Backend) *Backend {
    // Aynı 2-random seçim
    // Karar: daha düşük EWMA latency'si olan backend
    // Eğer her ikisinin LatencyEWMA == 0 (henüz ölçüm yok), ActiveConns'a fallback
}
```

### 2.4 `pkg/loadbalancer/proxy.go` — Latency ölçümü

`handleRequest` method'unda:
```go
func (lb *LoadBalancer) handleRequest(w http.ResponseWriter, r *http.Request) {
    // ... backend seçimi ...
    start := time.Now()
    backend.proxy.ServeHTTP(w, r)
    backend.RecordLatency(time.Since(start))
}
```

---

## Faz 3: Strategy Entegrasyonu

### 3.1 `pkg/types/config.go` — Yeni strategy değerleri
```go
// LBConfig.Strategy kabul edilen değerler:
// "roundrobin"  — Mevcut (default)
// "leastconn"   — Mevcut
// "p2c"         — Power of Two Choices (ActiveConns tabanlı)
// "p2c-ewma"    — P2C + Gecikme tabanlı (EWMA)
```

### 3.2 `cmd/loadbalancer/main.go` — Selector oluşturma
```go
var selector loadbalancer.Selector
switch cfg.Strategy {
case "roundrobin":
    selector = loadbalancer.NewRoundRobin()
case "leastconn":
    selector = loadbalancer.NewLeastConnections()
case "p2c":
    selector = loadbalancer.NewP2C()
case "p2c-ewma":
    selector = loadbalancer.NewP2CEWMA()
default:
    log.Fatalf("bilinmeyen strateji: %s", cfg.Strategy)
}
```

---

## Faz 4: Testler

### `pkg/loadbalancer/p2c_test.go` — Yeni dosya

```go
// TestP2C_SelectsLowerLoad
// 3 backend: [10 conn, 2 conn, 8 conn]
// 1000 iterasyon: 2-conn backend en çok seçilmeli

// TestP2C_Empty
// Boş liste → nil

// TestP2C_SingleBackend
// Tek backend → her zaman o

// TestP2C_NoHerdBehavior
// 2 P2C selector aynı backend listesine bakıyor
// Her ikisi farklı rastgele çiftler seçiyor
// LeastConn'dan farklı olarak aynı backend'e yığılmıyor

// TestP2C_FairDistribution
// Tüm backend'ler eşit yükte → dağılım yaklaşık eşit (±%20)
// (RoundRobin kadar kesin olmaz, stochastic yapı gereği)

// TestP2CEWMA_SelectsLowerLatency
// Backend A: 10ms EWMA, Backend B: 100ms EWMA
// B hiç seçilmemeli (ikisi aynı çiftteyken)

// TestP2CEWMA_FallbackToActiveConns
// LatencyEWMA = 0 (ölçüm yok) → ActiveConns'a fallback

// TestBackend_RecordLatency_EWMA
// Ardışık ölçümler: EWMA smooth'lanmış değer veriyor
// 100ms, 100ms, 200ms → EWMA ≈ 150ms (α=0.5 ile)
```

---

## Dosya Değişiklik Özeti

| Dosya | Değişiklik | Yeni/Mevcut |
|-------|------------|-------------|
| `pkg/loadbalancer/p2c.go` | P2C + P2CEWMA selector'ler | **Yeni** |
| `pkg/loadbalancer/p2c_test.go` | Tüm P2C testleri | **Yeni** |
| `pkg/loadbalancer/proxy.go` | Backend.LatencyEWMA + RecordLatency | Mevcut |
| `pkg/types/config.go` | Strategy dokümantasyonu | Mevcut |
| `cmd/loadbalancer/main.go` | p2c/p2c-ewma case'leri | Mevcut |

## Bağımlılıklar
- Sıfır dış bağımlılık (math/rand, sync, sync/atomic)
- Go 1.21 uyumlu (math/rand/v2 kullanılmaz)

## Başarı Kriterleri
- [ ] Mevcut 26 test hâlâ geçer
- [ ] P2C: Yüksek yüklü backend'leri otomatik kaçınır
- [ ] P2C: Eşit yükte yaklaşık eşit dağılım
- [ ] P2CEWMA: Yüksek latency'li backend'leri kaçınır
- [ ] P2CEWMA: İlk isteklerde (ölçüm yok) ActiveConns'a fallback
- [ ] Herd behavior testi: 2 selector aynı anda aynı backend'e yığılmıyor
- [ ] Simulator stress testi ile gerçek cluster'da doğrulama
