# TLS/HTTPS Desteği — Geliştirme Planı

## Amaç
Prodüksiyon ortamı için zorunlu olan TLS/HTTPS desteğini eklemek. Hem client→LB (frontend TLS) hem LB→backend (backend TLS) bağlantılarını desteklemek. Opsiyonel olarak gossip protokolü için de şifreleme sağlamak.

## Arka Plan
Mevcut durum:
- Client→LB: Düz HTTP (`:8080`)
- LB→Backend: Düz HTTP (`http://` + ServiceAddr)
- Gossip: Düz UDP JSON (şifrelenmemiş)

Prodüksiyon gereksinimleri:
- HTTPS terminasyonu (client→LB)
- Backend'lere opsiyonel TLS (LB→backend)
- Sertifika yönetimi (dosya tabanlı, opsiyonel auto-reload)

---

## Faz 1: Frontend TLS (Client → Load Balancer)

### 1.1 `pkg/types/config.go` — TLS konfigürasyonu

```go
// LBConfig'e eklenecek:
type TLSConfig struct {
    Enabled      bool   // TLS aktif mi (default: false)
    CertFile     string // PEM sertifika dosyası yolu
    KeyFile      string // PEM private key dosyası yolu
    MinVersion   string // Minimum TLS versiyonu: "1.2" veya "1.3" (default: "1.2")

    // Opsiyonel: mTLS (mutual TLS)
    ClientCAFile string // Client CA sertifikası (boşsa mTLS kapalı)

    // Opsiyonel: Auto-reload
    AutoReload   bool          // Sertifika değişikliğinde otomatik yenile (default: false)
    ReloadInterval time.Duration // Kontrol sıklığı (default: 1h)
}

// LBConfig'e ekle:
type LBConfig struct {
    // ... mevcut alanlar ...
    TLS TLSConfig
}
```

### 1.2 `pkg/loadbalancer/tls.go` — Yeni dosya

```go
package loadbalancer

import (
    "crypto/tls"
    "crypto/x509"
    "fmt"
    "os"
    "sync"
    "time"
)

// CertManager: Sertifika yükleme ve opsiyonel auto-reload
type CertManager struct {
    certFile string
    keyFile  string
    mu       sync.RWMutex
    cert     *tls.Certificate
}

// NewCertManager: Sertifikaları yükler, hata varsa döner
func NewCertManager(certFile, keyFile string) (*CertManager, error)

// GetCertificate: tls.Config.GetCertificate callback'i
// RLock ile thread-safe okuma
func (cm *CertManager) GetCertificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error)

// Reload: Sertifikaları diskten yeniden yükler
func (cm *CertManager) Reload() error

// StartAutoReload: Periyodik reload goroutine'i başlatır
func (cm *CertManager) StartAutoReload(ctx context.Context, interval time.Duration)

// BuildTLSConfig: Yapılandırmadan tls.Config oluşturur
func BuildTLSConfig(cfg TLSConfig) (*tls.Config, *CertManager, error) {
    // 1. Sertifika yükle
    // 2. MinVersion parse et (tls.VersionTLS12 veya tls.VersionTLS13)
    // 3. mTLS için ClientCA ekle
    // 4. CipherSuites: güvenli varsayılanlar (Go'nun varsayılanları zaten iyi)
    // 5. GetCertificate callback'i bağla (auto-reload desteği)
}
```

### 1.3 `pkg/loadbalancer/proxy.go` — HTTPS server başlatma

```go
func (lb *LoadBalancer) Start(ctx context.Context) error {
    // ... mevcut setup ...

    if lb.config.TLS.Enabled {
        tlsCfg, certMgr, err := BuildTLSConfig(lb.config.TLS)
        if err != nil {
            return fmt.Errorf("TLS yapılandırma hatası: %w", err)
        }
        lb.server.TLSConfig = tlsCfg

        if lb.config.TLS.AutoReload {
            certMgr.StartAutoReload(ctx, lb.config.TLS.ReloadInterval)
        }

        return lb.server.ListenAndServeTLS("", "")
        // CertFile/KeyFile GetCertificate callback'inden gelir
    }

    return lb.server.ListenAndServe()
}
```

### 1.4 `cmd/loadbalancer/main.go` — CLI flag'leri

```go
// Yeni flag'ler:
tlsEnabled  := flag.Bool("tls", false, "HTTPS aktifleştir")
tlsCert     := flag.String("tls-cert", "", "TLS sertifika dosyası (PEM)")
tlsKey      := flag.String("tls-key", "", "TLS private key dosyası (PEM)")
tlsMinVer   := flag.String("tls-min-version", "1.2", "Minimum TLS versiyonu (1.2 veya 1.3)")
tlsClientCA := flag.String("tls-client-ca", "", "mTLS: Client CA sertifikası (PEM)")
```

---

## Faz 2: Backend TLS (Load Balancer → Backend)

### 2.1 `pkg/loadbalancer/proxy.go` — Backend bağlantı TLS

Mevcut `newBackend()`:
```go
func (lb *LoadBalancer) newBackend(node gossip.NodeInfo) *Backend {
    target, _ := url.Parse("http://" + node.ServiceAddr)
    proxy := httputil.NewSingleHostReverseProxy(target)
    // ...
}
```

Değişiklik:
```go
func (lb *LoadBalancer) newBackend(node gossip.NodeInfo) *Backend {
    scheme := "http"
    if lb.config.BackendTLS.Enabled {
        scheme = "https"
    }
    target, _ := url.Parse(scheme + "://" + node.ServiceAddr)

    proxy := httputil.NewSingleHostReverseProxy(target)

    if lb.config.BackendTLS.Enabled {
        proxy.Transport = &http.Transport{
            TLSClientConfig: &tls.Config{
                InsecureSkipVerify: lb.config.BackendTLS.SkipVerify,
                // Opsiyonel: backend CA sertifikası
                RootCAs: lb.backendCAPool,
            },
            // ... mevcut timeout ayarları ...
        }
    }
    // ...
}
```

### 2.2 `pkg/types/config.go` — Backend TLS config

```go
type BackendTLSConfig struct {
    Enabled    bool   // Backend'lere TLS ile bağlan (default: false)
    SkipVerify bool   // Sertifika doğrulamayı atla (dev/test ortamı, default: false)
    CAFile     string // Backend CA sertifikası (PEM) — boşsa sistem CA'ları
}
```

---

## Faz 3: Gossip Şifreleme (Opsiyonel)

### Konsept
UDP gossip mesajları şu an düz JSON. Hassas ortamlarda şifreleme gerekebilir. İki yaklaşım:

**Yaklaşım A: Symmetric Key (Basit, Önerilen)**
- Tüm cluster node'ları aynı pre-shared key'i paylaşır
- AES-GCM ile mesaj şifreleme (authenticated encryption)
- Key rotation: yeni key gönder, eski key'i belirli süre kabul et

**Yaklaşım B: DTLS (Karmaşık)**
- UDP üzerinde TLS — Go stdlib'de yok, dış bağımlılık gerekir (pion/dtls)
- Sıfır bağımlılık prensibini bozar → **önerilmez**

### 3.1 `pkg/gossip/crypto.go` — Yeni dosya

```go
package gossip

import (
    "crypto/aes"
    "crypto/cipher"
    "crypto/rand"
    "errors"
)

// MessageEncryptor: AES-256-GCM ile gossip mesaj şifreleme
type MessageEncryptor struct {
    gcm    cipher.AEAD
    key    []byte // 32 byte (AES-256)
}

// NewMessageEncryptor: Key'den GCM cipher oluşturur
func NewMessageEncryptor(key []byte) (*MessageEncryptor, error) {
    if len(key) != 32 {
        return nil, errors.New("key 32 byte olmalı (AES-256)")
    }
    block, err := aes.NewCipher(key)
    if err != nil {
        return nil, err
    }
    gcm, err := cipher.NewGCM(block)
    if err != nil {
        return nil, err
    }
    return &MessageEncryptor{gcm: gcm, key: key}, nil
}

// Encrypt: plaintext → nonce || ciphertext
func (e *MessageEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
    nonce := make([]byte, e.gcm.NonceSize())
    if _, err := rand.Read(nonce); err != nil {
        return nil, err
    }
    return e.gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt: nonce || ciphertext → plaintext
func (e *MessageEncryptor) Decrypt(data []byte) ([]byte, error) {
    nonceSize := e.gcm.NonceSize()
    if len(data) < nonceSize {
        return nil, errors.New("veri çok kısa")
    }
    return e.gcm.Open(nil, data[:nonceSize], data[nonceSize:], nil)
}
```

### 3.2 `pkg/gossip/transport.go` — Şifreli gönderim/alım

```go
type Transport struct {
    // ... mevcut alanlar ...
    encryptor *MessageEncryptor // nil ise şifreleme kapalı
}

// Send değişikliği:
func (t *Transport) Send(addr string, msg GossipMessage) error {
    data, _ := json.Marshal(msg)
    if t.encryptor != nil {
        var err error
        data, err = t.encryptor.Encrypt(data)
        if err != nil {
            return err
        }
    }
    // ... WriteToUDP(data) ...
}

// receiveLoop değişikliği:
// JSON unmarshal'dan önce decrypt
```

---

## Faz 4: Test Altyapısı

### `pkg/loadbalancer/tls_test.go` — Yeni dosya

```go
// TestTLS_HTTPSServer
// - Self-signed sertifika oluştur (test helper)
// - HTTPS LB başlat
// - InsecureSkipVerify client ile request gönder
// - 200 OK döner

// TestTLS_InvalidCert
// - Geçersiz sertifika dosyası → Start() hata döner

// TestTLS_MinVersion
// - TLS 1.3 zorunlu, TLS 1.2 client bağlanamaz

// TestTLS_mTLS
// - Client CA ile mutual TLS
// - CA'sız client → bağlantı reddedilir

// TestCertManager_Reload
// - Sertifika dosyasını değiştir
// - Reload() → yeni sertifika aktif

// TestCertManager_GetCertificate
// - Thread-safe erişim (concurrent goroutine'ler)
```

### `pkg/gossip/crypto_test.go` — Yeni dosya

```go
// TestEncryptDecrypt_RoundTrip
// - Encrypt → Decrypt → orijinal mesaj

// TestDecrypt_WrongKey
// - Farklı key ile decrypt → hata

// TestDecrypt_TamperedData
// - Ciphertext değiştirilmiş → GCM authentication hatası

// TestEncrypt_UniqueNonce
// - Aynı plaintext, farklı nonce → farklı ciphertext
```

### Test helper: Self-signed sertifika oluşturma

```go
// testutil/cert.go veya test dosyası içinde
func generateSelfSignedCert(t *testing.T) (certFile, keyFile string) {
    // crypto/x509 + crypto/ecdsa ile geçici sertifika
    // t.TempDir() içine yaz
    // t.Cleanup ile temizle
}
```

---

## Dosya Değişiklik Özeti

| Dosya | Değişiklik | Yeni/Mevcut |
|-------|------------|-------------|
| `pkg/loadbalancer/tls.go` | CertManager + BuildTLSConfig | **Yeni** |
| `pkg/loadbalancer/tls_test.go` | Frontend TLS testleri | **Yeni** |
| `pkg/gossip/crypto.go` | AES-GCM encrypt/decrypt | **Yeni** |
| `pkg/gossip/crypto_test.go` | Şifreleme testleri | **Yeni** |
| `pkg/loadbalancer/proxy.go` | HTTPS start + backend TLS | Mevcut |
| `pkg/gossip/transport.go` | Encrypt/decrypt entegrasyonu | Mevcut |
| `pkg/types/config.go` | TLSConfig + BackendTLSConfig | Mevcut |
| `cmd/loadbalancer/main.go` | TLS CLI flag'leri | Mevcut |

## Bağımlılıklar
- Sıfır dış bağımlılık (crypto/tls, crypto/aes, crypto/x509 hepsi Go stdlib)
- DTLS kullanılmaz (dış bağımlılık gerektirir)

## Güvenlik Notları
- **TLS 1.2 minimum** (1.0/1.1 devre dışı)
- **AES-GCM** authenticated encryption (integrity + confidentiality)
- **Sertifika reload** — zero-downtime rotation
- **mTLS opsiyonel** — service-to-service kimlik doğrulama
- **SkipVerify sadece dev/test** — prodüksiyonda false olmalı

## Başarı Kriterleri
- [ ] Mevcut 26 test hâlâ geçer (TLS kapalıyken davranış değişmez)
- [ ] HTTPS terminasyonu çalışır (curl -k https://localhost:8080)
- [ ] mTLS ile yetkisiz client reddedilir
- [ ] Backend TLS ile LB→backend şifreli bağlantı
- [ ] Gossip şifreleme ile mesajlar AES-GCM ile korunur
- [ ] Sertifika reload zero-downtime çalışır
- [ ] `openssl s_client` ile TLS handshake doğrulanır
