# Konfigürasyon Dosyası Desteği — Geliştirme Planı

## Amaç
CLI flag'lerinin yanına YAML ve TOML konfigürasyon dosyası desteği eklemek. Prodüksiyon ortamında tekrarlanabilir, version-controlled, okunabilir yapılandırma sağlamak.

## Arka Plan
Mevcut durum:
- Tüm yapılandırma CLI flag'leri ile yapılıyor
- Node: `-id=node-a -gossip=:9001 -service=:8001 -seeds=:9002,:9003`
- LB: `-listen=:8080 -strategy=roundrobin -gossip=:9000 -seeds=:9001,:9002`
- Sorunlar:
  - Uzun komut satırları (özellikle TLS, health check gibi yeni özelliklerle)
  - Version control edilemiyor (shell script'lere gömülü)
  - Ortam bazlı farklılıklar yönetilemiyor (dev/staging/prod)
  - Yeni özellikler ekledikçe flag sayısı patlar

**Konfigürasyon hiyerarşisi (öncelik sırası):**
1. CLI flag'leri (en yüksek öncelik)
2. Ortam değişkenleri
3. Konfigürasyon dosyası
4. Varsayılan değerler (en düşük öncelik)

---

## Format Seçimi: YAML mı TOML mı?

| Kriter | YAML | TOML |
|--------|------|------|
| Go stdlib | Yok (dış bağımlılık) | Yok (dış bağımlılık) |
| Okunabilirlik | İyi (indent tabanlı) | İyi (INI benzeri) |
| Nested config | Doğal | Daha az doğal |
| Ecosystem | Kubernetes/Docker/Helm standart | Go projelerinde yaygın (Hugo, Traefik v1) |
| Hata riski | Indent hataları | Minimal |

**Karar:** TOML birincil format. Nedenleri:
1. Go projelerinde daha yaygın
2. Indent hataları yok (YAML'ın en büyük sorunu)
3. `BurntSushi/toml` kütüphanesi Go'nun de-facto standardı
4. Traefik ve Hugo gibi büyük Go projeleri TOML kullanıyor

**Ama:** Sıfır bağımlılık prensibi nedeniyle, önce Go stdlib ile custom parser veya basit JSON desteği, sonra opsiyonel TOML.

### Hibrit yaklaşım (Önerilen):
1. **JSON** — Go stdlib ile doğrudan destek (sıfır bağımlılık)
2. **TOML** — Opsiyonel, `github.com/BurntSushi/toml` ile (tek bağımlılık)
3. Dosya uzantısına göre otomatik parser seçimi

---

## Faz 1: Konfigürasyon Yapısı

### 1.1 `pkg/config/config.go` — Yeni paket

```go
package config

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "time"
)

// NodeConfig: Backend node tam yapılandırması
type NodeConfig struct {
    ID          string       `json:"id" toml:"id"`
    GossipAddr  string       `json:"gossip_addr" toml:"gossip_addr"`
    ServiceAddr string       `json:"service_addr" toml:"service_addr"`
    SeedNodes   []string     `json:"seed_nodes" toml:"seed_nodes"`
    Protocol    ProtocolCfg  `json:"protocol" toml:"protocol"`
    Logging     LoggingCfg   `json:"logging" toml:"logging"`
}

// LBFullConfig: Load balancer tam yapılandırması
type LBFullConfig struct {
    Listen      string       `json:"listen" toml:"listen"`
    Strategy    string       `json:"strategy" toml:"strategy"`
    GossipAddr  string       `json:"gossip_addr" toml:"gossip_addr"`
    SeedNodes   []string     `json:"seed_nodes" toml:"seed_nodes"`
    Protocol    ProtocolCfg  `json:"protocol" toml:"protocol"`
    TLS         TLSCfg       `json:"tls" toml:"tls"`
    BackendTLS  BackendTLSCfg `json:"backend_tls" toml:"backend_tls"`
    HealthCheck HealthCheckCfg `json:"health_check" toml:"health_check"`
    Logging     LoggingCfg   `json:"logging" toml:"logging"`
}

// Alt yapılandırmalar:

type ProtocolCfg struct {
    Interval        Duration `json:"interval" toml:"interval"`                   // "1s"
    PingTimeout     Duration `json:"ping_timeout" toml:"ping_timeout"`           // "500ms"
    IndirectTimeout Duration `json:"indirect_timeout" toml:"indirect_timeout"`   // "1s"
    SuspectTimeout  Duration `json:"suspect_timeout" toml:"suspect_timeout"`     // "3s"
    IndirectNodes   int      `json:"indirect_nodes" toml:"indirect_nodes"`       // 3
    Lifeguard       bool     `json:"lifeguard" toml:"lifeguard"`                 // true
}

type TLSCfg struct {
    Enabled     bool   `json:"enabled" toml:"enabled"`
    CertFile    string `json:"cert_file" toml:"cert_file"`
    KeyFile     string `json:"key_file" toml:"key_file"`
    MinVersion  string `json:"min_version" toml:"min_version"` // "1.2"
    ClientCA    string `json:"client_ca" toml:"client_ca"`
    AutoReload  bool   `json:"auto_reload" toml:"auto_reload"`
}

type BackendTLSCfg struct {
    Enabled    bool   `json:"enabled" toml:"enabled"`
    SkipVerify bool   `json:"skip_verify" toml:"skip_verify"`
    CAFile     string `json:"ca_file" toml:"ca_file"`
}

type HealthCheckCfg struct {
    Enabled            bool     `json:"enabled" toml:"enabled"`
    Interval           Duration `json:"interval" toml:"interval"`     // "5s"
    Timeout            Duration `json:"timeout" toml:"timeout"`       // "2s"
    Path               string   `json:"path" toml:"path"`             // "/health"
    UnhealthyThreshold int      `json:"unhealthy_threshold" toml:"unhealthy_threshold"`
    HealthyThreshold   int      `json:"healthy_threshold" toml:"healthy_threshold"`
}

type LoggingCfg struct {
    Level  string `json:"level" toml:"level"`   // "info", "debug", "warn", "error"
    Format string `json:"format" toml:"format"` // "text", "json"
}
```

### 1.2 Custom Duration Type (JSON uyumlu)

```go
// Duration: JSON/TOML'da "5s", "500ms" gibi string olarak serialize edilir
type Duration struct {
    time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
    return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
    var s string
    if err := json.Unmarshal(b, &s); err != nil {
        return err
    }
    dur, err := time.ParseDuration(s)
    if err != nil {
        return err
    }
    d.Duration = dur
    return nil
}

// TOML için de aynı MarshalText/UnmarshalText
func (d Duration) MarshalText() ([]byte, error) {
    return []byte(d.String()), nil
}

func (d *Duration) UnmarshalText(b []byte) error {
    dur, err := time.ParseDuration(string(b))
    if err != nil {
        return err
    }
    d.Duration = dur
    return nil
}
```

---

## Faz 2: Dosya Yükleme ve Parser

### 2.1 `pkg/config/loader.go`

```go
package config

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
)

// Load: Dosya uzantısına göre konfigürasyon yükle
func Load[T any](path string) (*T, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, fmt.Errorf("dosya okunamadı: %w", err)
    }

    var cfg T
    ext := filepath.Ext(path)

    switch ext {
    case ".json":
        if err := json.Unmarshal(data, &cfg); err != nil {
            return nil, fmt.Errorf("JSON parse hatası: %w", err)
        }
    case ".toml":
        if err := parseTOML(data, &cfg); err != nil {
            return nil, fmt.Errorf("TOML parse hatası: %w", err)
        }
    default:
        return nil, fmt.Errorf("desteklenmeyen format: %s (json veya toml kullanın)", ext)
    }

    return &cfg, nil
}

// parseTOML: Build tag ile opsiyonel TOML desteği
// toml build tag'i yoksa hata döner
```

### 2.2 Build Tag ile Opsiyonel TOML

```go
// pkg/config/toml_enabled.go
//go:build toml

package config

import "github.com/BurntSushi/toml"

func parseTOML(data []byte, v any) error {
    return toml.Unmarshal(data, v)
}
```

```go
// pkg/config/toml_disabled.go
//go:build !toml

package config

import "fmt"

func parseTOML(data []byte, v any) error {
    return fmt.Errorf("TOML desteği bu build'de yok. '-tags toml' ile derleyin")
}
```

Bu yaklaşım sıfır bağımlılık prensibini korur: varsayılan build'de sadece JSON, `-tags toml` ile TOML aktif.

---

## Faz 3: CLI Flag Entegrasyonu (Merge Logic)

### 3.1 `pkg/config/merge.go`

```go
package config

// MergeLBConfig: Config dosyası + CLI flag'leri birleştir
// Öncelik: CLI flag > config dosyası > default
func MergeLBConfig(fileCfg *LBFullConfig, flags CLIFlags) LBFullConfig {
    result := *fileCfg // Dosyadan başla

    // CLI flag ayarlanmışsa üzerine yaz
    if flags.Listen != "" {
        result.Listen = flags.Listen
    }
    if flags.Strategy != "" {
        result.Strategy = flags.Strategy
    }
    if flags.GossipAddr != "" {
        result.GossipAddr = flags.GossipAddr
    }
    if len(flags.SeedNodes) > 0 {
        result.SeedNodes = flags.SeedNodes
    }
    // ... diğer alanlar ...

    return result
}

// CLIFlags: flag paketinden parse edilen değerler
type CLIFlags struct {
    ConfigFile string   // -config flag'i
    Listen     string
    Strategy   string
    GossipAddr string
    SeedNodes  []string
    // ... diğer flag'ler ...
}
```

### 3.2 `cmd/loadbalancer/main.go` — Entegrasyon

```go
func main() {
    // CLI flag'leri parse et
    configFile := flag.String("config", "", "Konfigürasyon dosyası (JSON veya TOML)")
    listen := flag.String("listen", "", "HTTP listen adresi")
    // ... diğer flag'ler ...
    flag.Parse()

    var cfg config.LBFullConfig

    // 1. Config dosyası varsa yükle
    if *configFile != "" {
        loaded, err := config.Load[config.LBFullConfig](*configFile)
        if err != nil {
            log.Fatalf("config yükleme hatası: %v", err)
        }
        cfg = *loaded
    }

    // 2. Varsayılanları uygula (boş alanlar)
    cfg = config.ApplyDefaults(cfg)

    // 3. CLI flag'lerini üzerine yaz
    flags := config.CLIFlags{
        Listen:   *listen,
        Strategy: *strategy,
        // ...
    }
    cfg = config.MergeLBConfig(&cfg, flags)

    // 4. Validate
    if err := config.Validate(cfg); err != nil {
        log.Fatalf("config doğrulama hatası: %v", err)
    }

    // 5. Mevcut types.LBConfig + ProtocolConfig'e dönüştür
    lbCfg, protoCfg := config.ToInternal(cfg)
    // ... mevcut başlatma mantığı ...
}
```

---

## Faz 4: Ortam Değişkenleri Desteği

### 4.1 `pkg/config/env.go`

```go
package config

import (
    "os"
    "strings"
)

// Ortam değişkeni prefix: P2PLB_
// Örnek: P2PLB_LISTEN=:8080, P2PLB_STRATEGY=leastconn
// Nested: P2PLB_TLS_ENABLED=true, P2PLB_PROTOCOL_PING_TIMEOUT=500ms

// ApplyEnv: Ortam değişkenlerini config'e uygula
// Öncelik: CLI > Env > File > Default
func ApplyEnv(cfg *LBFullConfig) {
    if v := os.Getenv("P2PLB_LISTEN"); v != "" {
        cfg.Listen = v
    }
    if v := os.Getenv("P2PLB_STRATEGY"); v != "" {
        cfg.Strategy = v
    }
    if v := os.Getenv("P2PLB_GOSSIP_ADDR"); v != "" {
        cfg.GossipAddr = v
    }
    if v := os.Getenv("P2PLB_SEED_NODES"); v != "" {
        cfg.SeedNodes = strings.Split(v, ",")
    }
    // ... TLS, health check, protocol ...
}
```

---

## Faz 5: Örnek Konfigürasyon Dosyaları

### 5.1 `configs/loadbalancer.json` (Örnek)

```json
{
  "listen": ":8080",
  "strategy": "roundrobin",
  "gossip_addr": ":9000",
  "seed_nodes": [":9001", ":9002", ":9003", ":9004"],

  "protocol": {
    "interval": "1s",
    "ping_timeout": "500ms",
    "indirect_timeout": "1s",
    "suspect_timeout": "3s",
    "indirect_nodes": 3,
    "lifeguard": true
  },

  "tls": {
    "enabled": false,
    "cert_file": "",
    "key_file": "",
    "min_version": "1.2"
  },

  "health_check": {
    "enabled": true,
    "interval": "5s",
    "timeout": "2s",
    "path": "/health",
    "unhealthy_threshold": 3,
    "healthy_threshold": 2
  },

  "logging": {
    "level": "info",
    "format": "text"
  }
}
```

### 5.2 `configs/loadbalancer.toml` (Örnek, -tags toml ile)

```toml
listen = ":8080"
strategy = "p2c"
gossip_addr = ":9000"
seed_nodes = [":9001", ":9002", ":9003", ":9004"]

[protocol]
interval = "1s"
ping_timeout = "500ms"
indirect_timeout = "1s"
suspect_timeout = "3s"
indirect_nodes = 3
lifeguard = true

[tls]
enabled = true
cert_file = "/etc/p2plb/server.crt"
key_file = "/etc/p2plb/server.key"
min_version = "1.3"

[backend_tls]
enabled = false

[health_check]
enabled = true
interval = "5s"
timeout = "2s"
path = "/health"
unhealthy_threshold = 3

[logging]
level = "info"
format = "json"
```

### 5.3 `configs/node.json` (Örnek)

```json
{
  "id": "node-a",
  "gossip_addr": ":9001",
  "service_addr": ":8001",
  "seed_nodes": [":9002", ":9003"],

  "protocol": {
    "interval": "1s",
    "ping_timeout": "500ms",
    "indirect_nodes": 3
  },

  "logging": {
    "level": "info",
    "format": "text"
  }
}
```

---

## Faz 6: Validation

### 6.1 `pkg/config/validate.go`

```go
package config

import (
    "fmt"
    "strings"
)

// Validate: Tüm config değerlerini doğrula
func Validate(cfg LBFullConfig) error {
    var errs []string

    if cfg.Listen == "" {
        errs = append(errs, "listen adresi boş olamaz")
    }

    switch cfg.Strategy {
    case "roundrobin", "leastconn", "p2c", "p2c-ewma":
        // OK
    case "":
        errs = append(errs, "strategy belirtilmeli")
    default:
        errs = append(errs, fmt.Sprintf("bilinmeyen strategy: %s", cfg.Strategy))
    }

    if cfg.GossipAddr == "" {
        errs = append(errs, "gossip_addr boş olamaz")
    }

    // Protocol validation
    if cfg.Protocol.IndirectNodes <= 0 {
        errs = append(errs, "indirect_nodes > 0 olmalı")
    }
    if cfg.Protocol.PingTimeout.Duration >= cfg.Protocol.IndirectTimeout.Duration {
        errs = append(errs, "ping_timeout < indirect_timeout olmalı")
    }

    // TLS validation
    if cfg.TLS.Enabled {
        if cfg.TLS.CertFile == "" {
            errs = append(errs, "TLS aktif ama cert_file belirtilmemiş")
        }
        if cfg.TLS.KeyFile == "" {
            errs = append(errs, "TLS aktif ama key_file belirtilmemiş")
        }
    }

    // Health check validation
    if cfg.HealthCheck.Enabled {
        if cfg.HealthCheck.Interval.Duration <= 0 {
            errs = append(errs, "health_check interval > 0 olmalı")
        }
        if cfg.HealthCheck.UnhealthyThreshold <= 0 {
            errs = append(errs, "unhealthy_threshold > 0 olmalı")
        }
    }

    if len(errs) > 0 {
        return fmt.Errorf("config doğrulama hataları:\n  - %s", strings.Join(errs, "\n  - "))
    }
    return nil
}
```

---

## Faz 7: Testler

### `pkg/config/config_test.go`

```go
// TestLoad_JSON
// - Geçerli JSON dosyası → başarılı parse
// - Tüm alanlar doğru

// TestLoad_InvalidJSON
// - Bozuk JSON → hata mesajı

// TestLoad_UnsupportedFormat
// - .yaml uzantısı → "desteklenmeyen format" hatası

// TestLoad_FileNotFound
// - Olmayan dosya → hata

// TestDuration_MarshalJSON
// - 5*time.Second → "5s"
// - 500*time.Millisecond → "500ms"

// TestDuration_UnmarshalJSON
// - "5s" → 5*time.Second
// - "500ms" → 500*time.Millisecond
// - "invalid" → hata

// TestMergeLBConfig_CLIOverridesFile
// - File: listen=":8080", CLI: listen=":9090"
// - Sonuç: ":9090" (CLI kazanır)

// TestMergeLBConfig_FileDefaultsKept
// - File: strategy="p2c", CLI: strategy="" (ayarlanmamış)
// - Sonuç: "p2c" (dosya değeri korunur)

// TestApplyEnv
// - P2PLB_LISTEN=":7070" ortam değişkeni
// - Config'e uygulanır

// TestValidate_ValidConfig
// - Tam geçerli config → nil hata

// TestValidate_EmptyListen
// - listen="" → hata

// TestValidate_InvalidStrategy
// - strategy="unknown" → hata

// TestValidate_TLSWithoutCert
// - TLS enabled ama cert_file="" → hata

// TestValidate_MultipleErrors
// - Birden fazla hata → hepsi listelenir
```

---

## Dosya Değişiklik Özeti

| Dosya | Değişiklik | Yeni/Mevcut |
|-------|------------|-------------|
| `pkg/config/config.go` | Config struct'lar + Duration type | **Yeni paket** |
| `pkg/config/loader.go` | Load fonksiyonu + parser seçimi | **Yeni** |
| `pkg/config/merge.go` | CLI + env + file merge logic | **Yeni** |
| `pkg/config/env.go` | Ortam değişkeni desteği | **Yeni** |
| `pkg/config/validate.go` | Validation logic | **Yeni** |
| `pkg/config/toml_enabled.go` | TOML parser (build tag: toml) | **Yeni** |
| `pkg/config/toml_disabled.go` | TOML placeholder (default) | **Yeni** |
| `pkg/config/config_test.go` | Tüm config testleri | **Yeni** |
| `configs/loadbalancer.json` | Örnek LB config | **Yeni** |
| `configs/loadbalancer.toml` | Örnek LB config (TOML) | **Yeni** |
| `configs/node.json` | Örnek node config | **Yeni** |
| `cmd/loadbalancer/main.go` | Config dosyası entegrasyonu | Mevcut |
| `cmd/node/main.go` | Config dosyası entegrasyonu | Mevcut |

## Bağımlılıklar
- **Varsayılan:** Sıfır bağımlılık (sadece JSON, Go stdlib)
- **Opsiyonel:** `github.com/BurntSushi/toml` (`-tags toml` ile)

## Geriye Uyumluluk
- `-config` flag'i belirtilmezse mevcut CLI-only davranış korunur
- Mevcut flag'lerin hiçbiri kaldırılmaz veya yeniden adlandırılmaz
- Config dosyası + CLI flag birlikte kullanılabilir

## Başarı Kriterleri
- [ ] Mevcut 26 test hâlâ geçer
- [ ] JSON config dosyası ile LB başlatılabilir
- [ ] CLI flag'leri config dosyasını override eder
- [ ] Ortam değişkenleri çalışır
- [ ] Geçersiz config → anlaşılır hata mesajı
- [ ] Duration "5s" formatında serialize/deserialize
- [ ] `-tags toml` ile TOML desteği aktif
- [ ] `-config` olmadan mevcut davranış korunur (geriye uyumluluk)
