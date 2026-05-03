# Lifeguard Uzantıları — Geliştirme Planı

## Amaç
HashiCorp'un prodüksiyon deneyiminden doğan Lifeguard uzantılarını (Self-Awareness, Dogpile, Buddy System) mevcut SWIM implementasyonuna eklemek. Yanlış pozitifleri ~50x azaltmak, CPU/ağ stresi altında sağlıklı node'ların yanlışlıkla dead ilan edilmesini önlemek.

## Arka Plan
Mevcut SWIM implementasyonumuz sabit timeout'lar kullanıyor (PingTimeout=500ms, IndirectTimeout=1s, SuspectTimeout=3s). CPU tükenmesi, GC duraklaması veya ağ kalabalığı durumunda bu sabit süreler yetersiz kalıyor ve sağlıklı node'lar yanlışlıkla suspect/dead ilan ediliyor.

**Referans:** [arXiv 1707.00788 — Lifeguard Paper](https://arxiv.org/pdf/1707.00788)

---

## Faz 1: Self-Awareness (Öz-Farkındalık)

### Konsept
Node kendi sağlık durumunu ölçer. Eğer kendisi yavaşsa (ACK'ler geç geliyor, kendi ping'leri timeout oluyor), timeout sürelerini dinamik olarak artırır.

### Değişiklikler

#### 1.1 `pkg/gossip/types.go` — Yeni config alanları
```go
// ProtocolConfig'e eklenecek alanlar:
type ProtocolConfig struct {
    // ... mevcut alanlar ...

    // Lifeguard Self-Awareness
    LifeguardEnabled      bool          // Lifeguard uzantılarını aktifleştir (default: true)
    SelfAwarenessMax      int           // Maksimum noop counter (default: 8)
    PingTimeoutMin        time.Duration // Minimum ping timeout (default: 500ms, mevcut PingTimeout)
    PingTimeoutMax        time.Duration // Maksimum ping timeout (default: 4s, 8x min)
    IndirectTimeoutMin    time.Duration // Minimum indirect timeout (default: 1s)
    IndirectTimeoutMax    time.Duration // Maksimum indirect timeout (default: 8s)
    SuspectTimeoutMin     time.Duration // Minimum suspect timeout (default: 3s)
    SuspectTimeoutMax     time.Duration // Maksimum suspect timeout (default: 24s)
}
```

#### 1.2 `pkg/gossip/lifeguard.go` — Yeni dosya
```go
// SelfAwareness, node'un kendi sağlık skoru.
// noop=0 → tamamen sağlıklı, noop=Max → çok yavaş.
type SelfAwareness struct {
    mu       sync.Mutex
    noop     int    // Kaçırılan ACK sayısı (kendi ping'lerimize gelen timeout)
    max      int    // Üst sınır (default: 8)
}

// RecordSuccess: Başarılı bir ACK aldığımızda noop'u 1 azalt
func (s *SelfAwareness) RecordSuccess()

// RecordFailure: Timeout aldığımızda noop'u 1 artır
func (s *SelfAwareness) RecordFailure()

// Score: Mevcut noop değeri (0..max)
func (s *SelfAwareness) Score() int

// ScaleTimeout: noop skoruna göre timeout'u orantılı ölçekle
// timeout = min + (max - min) * (noop / maxNoop)
func (s *SelfAwareness) ScaleTimeout(min, max time.Duration) time.Duration
```

#### 1.3 `pkg/gossip/protocol.go` — Mevcut timeout'ları dinamik yap
**Değişecek methodlar:**

`runProtocolRound()`:
- Sabit `p.config.PingTimeout` yerine `p.selfAwareness.ScaleTimeout(PingTimeoutMin, PingTimeoutMax)` kullan
- Sabit `p.config.IndirectTimeout` yerine `p.selfAwareness.ScaleTimeout(IndirectTimeoutMin, IndirectTimeoutMax)` kullan
- ACK başarılı → `p.selfAwareness.RecordSuccess()`
- ACK timeout → `p.selfAwareness.RecordFailure()`

`suspectNode()`:
- `MarkSuspect` çağrısında `SuspectTimeout` yerine scaled timeout kullan

**Protocol struct'a ekle:**
```go
type Protocol struct {
    // ... mevcut alanlar ...
    selfAwareness *SelfAwareness
}
```

### Testler
- `TestSelfAwareness_ScaleTimeout`: noop=0 → min, noop=max → max, ara değerler orantılı
- `TestSelfAwareness_RecordSuccess_Decrements`: 0'ın altına düşmez
- `TestSelfAwareness_RecordFailure_Increments`: max'ın üstüne çıkmaz
- `TestProtocol_DynamicTimeout`: Timeout'ların gerçekten skor ile ölçeklendiğini doğrula

---

## Faz 2: Dogpile (Bağımsız Doğrulama)

### Konsept
Orijinal SWIM'de suspect süresi sabittir. Dogpile'da süre dinamiktir: bağımsız node'lar arızayı doğruladıkça süre logaritmik olarak azalır. Gerçek arızalar hızlı tespit edilir, geçici sorunlar yanlış pozitif üretmez.

### Değişiklikler

#### 2.1 `pkg/gossip/types.go` — Yeni mesaj tipi
```go
const MsgNack MessageType = "NACK" // Dolaylı ping başarısız olduğunda
```

#### 2.2 `pkg/gossip/membership.go` — Suspect doğrulama sayacı
**nodeEntry struct'a ekle:**
```go
type nodeEntry struct {
    // ... mevcut alanlar ...
    independentSuspicions int  // Kaç farklı node bu node'u suspect olarak doğruladı
    suspicionConfirmers   map[string]bool // Hangi node'lar doğruladı (tekrar sayma)
}
```

**Yeni method:**
```go
// ConfirmSuspicion: Başka bir node'dan gelen suspect doğrulaması.
// Doğrulama sayısına göre suspect timeout'u yeniden hesapla.
// Formula: timeout = max(minTimeout, maxTimeout * log(K+1) / log(confirmations+K+1))
// K = IndirectNodes (fan-out factor)
func (ml *MembershipList) ConfirmSuspicion(nodeID, confirmerID string) int
```

**MarkSuspect değişikliği:**
- Başlangıçta `SuspectTimeoutMax` ile timer başlat (en uzun süre)
- Her `ConfirmSuspicion` çağrısında timer'ı yeniden hesapla ve `Reset()` et

#### 2.3 `pkg/gossip/protocol.go` — NACK işleme
**handlePingReq değişikliği:**
- Dolaylı ping başarısız olursa, orijinal isteyiciye `MsgNack` gönder
- NACK alan node, suspect için `ConfirmSuspicion` çağırır

**Yeni handler:**
```go
func (p *Protocol) handleNack(msg GossipMessage) {
    // msg.TargetID hakkında bağımsız arıza doğrulaması
    p.membership.ConfirmSuspicion(msg.TargetID, msg.SenderID)
}
```

### Testler
- `TestDogpile_TimeoutDecreases`: Her doğrulama timeout'u azaltır
- `TestDogpile_MinTimeoutRespected`: Minimum sürenin altına düşmez
- `TestDogpile_NoDuplicateConfirmation`: Aynı node iki kez sayılmaz
- `TestDogpile_FormulaCorrectness`: Log formülü doğru çalışır

---

## Faz 3: Buddy System (Doğrudan Bildirim)

### Konsept
Bir node suspect ilan edildiğinde, gossip propagasyonunu beklemek yerine doğrudan o node'a bildirim gönderilir. Node hâlâ canlıysa hemen savunma yapabilir (incarnation++).

### Değişiklikler

#### 3.1 `pkg/gossip/protocol.go` — suspectNode değişikliği
```go
func (p *Protocol) suspectNode(nodeID string) {
    // ... mevcut suspect logic ...

    // Buddy System: Suspect node'a doğrudan bildir
    if p.config.LifeguardEnabled {
        target, ok := p.membership.GetMember(nodeID)
        if ok {
            buddyMsg := GossipMessage{
                Type:     MsgSuspect,
                SenderID: p.localInfo.ID,
                TargetID: nodeID,
                Members:  p.membership.GetAllMembers(),
            }
            go p.transport.Send(target.Addr, buddyMsg)
        }
    }
}
```

Bu değişiklik minimal çünkü mevcut `handleSuspectMsg` zaten self-defense mantığına sahip:
```go
// protocol.go'da mevcut:
func (p *Protocol) handleSuspectMsg(msg GossipMessage) {
    if msg.TargetID == p.localInfo.ID {
        p.membership.Defend(&p.localInfo)
        p.announceAlive()
        return
    }
    // ...
}
```

Buddy System sadece bu mesajın daha erken ulaşmasını sağlar (gossip yayılımını beklemek yerine).

### Testler
- `TestBuddySystem_DirectNotification`: Suspect node doğrudan bildirim alır
- `TestBuddySystem_NodeDefends`: Bildirim alan canlı node incarnation artırır

---

## Faz 4: Konfigürasyon ve Geriye Uyumluluk

### DefaultConfig güncelleme
```go
func DefaultConfig() ProtocolConfig {
    return ProtocolConfig{
        // ... mevcut defaults ...
        LifeguardEnabled:   true,
        SelfAwarenessMax:   8,
        PingTimeoutMin:     500 * time.Millisecond,
        PingTimeoutMax:     4 * time.Second,
        IndirectTimeoutMin: 1 * time.Second,
        IndirectTimeoutMax: 8 * time.Second,
        SuspectTimeoutMin:  3 * time.Second,
        SuspectTimeoutMax:  24 * time.Second,
    }
}
```

### Validate güncelleme
- `SelfAwarenessMax > 0`
- `PingTimeoutMin <= PingTimeoutMax`
- `SuspectTimeoutMin <= SuspectTimeoutMax`

### Geriye uyumluluk
- `LifeguardEnabled: false` → mevcut davranış aynen korunur
- Mevcut CLI flag'leri değişmez
- Yeni flag'ler: `-lifeguard=true/false`

---

## Dosya Değişiklik Özeti

| Dosya | Değişiklik | Yeni/Mevcut |
|-------|------------|-------------|
| `pkg/gossip/lifeguard.go` | SelfAwareness struct + methodlar | **Yeni** |
| `pkg/gossip/lifeguard_test.go` | Self-awareness + dogpile + buddy testleri | **Yeni** |
| `pkg/gossip/types.go` | Config alanları + MsgNack | Mevcut |
| `pkg/gossip/membership.go` | nodeEntry alanları + ConfirmSuspicion | Mevcut |
| `pkg/gossip/protocol.go` | Dinamik timeout + NACK + buddy gönderimi | Mevcut |
| `pkg/types/config.go` | Lifeguard config + validation | Mevcut |

## Bağımlılıklar
- Hiçbir dış bağımlılık eklenmez (sıfır bağımlılık prensibi korunur)
- Sadece Go stdlib: `sync`, `time`, `math`

## Başarı Kriterleri
- [ ] Mevcut 26 test hâlâ geçer (geriye uyumluluk)
- [ ] Self-awareness: Yavaş node timeout'ları otomatik artırır
- [ ] Dogpile: Bağımsız doğrulamalar suspect süresini logaritmik azaltır
- [ ] Buddy: Suspect node doğrudan bildirilir, savunma süresi kısalır
- [ ] `LifeguardEnabled: false` ile mevcut davranış korunur
- [ ] Simulator ile yanlış pozitif oranı ölçülür
