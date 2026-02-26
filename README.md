# P2P Gossip Load Balancer

Go ile yazılmış, merkezi koordinatör gerektirmeyen P2P tabanlı HTTP yük dengeleyici. Üyelik tespiti için SWIM (Scalable Weakly-consistent Infection-style Membership) protokolü kullanır; düğümler birbirini gossip mesajlarıyla keşfeder ve izler.

## Mimari

```
┌──────────┐     HTTP      ┌──────────────────┐
│  İstemci │ ────────────▶ │  Load Balancer   │
└──────────┘               │  (gossip üyesi)  │
                           └────────┬─────────┘
                                    │ reverse proxy
               ┌────────────────────┼────────────────────┐
               ▼                    ▼                    ▼
          ┌─────────┐         ┌─────────┐         ┌─────────┐
          │ node-a  │         │ node-b  │         │ node-c  │
          │ :8001   │         │ :8002   │         │ :8003   │
          └────┬────┘         └────┬────┘         └────┬────┘
               │   UDP Gossip (SWIM protokolü)         │
               └──────────────────┴──────────────────┘
```

**Gossip protokolü:** Her saniye rastgele bir üyeye `PING` gönderilir. Yanıt gelmezse K=3 aracıya `PING_REQ` yollanır. Yine yanıt gelmezse düğüm `SUSPECT` → `DEAD` state machine'inden geçer ve LB trafik listesinden çıkarılır.

## Proje Yapısı

```
.
├── cmd/
│   ├── node/           # Backend düğüm binary'si
│   ├── loadbalancer/   # HTTP ters proxy + gossip üyesi
│   └── simulator/      # Hata simülasyonu ve test aracı
├── pkg/
│   ├── types/          # ProtocolConfig, LBConfig, Validate()
│   ├── gossip/
│   │   ├── types.go        # NodeInfo, GossipMessage, MessageType
│   │   ├── membership.go   # MembershipList — SWIM state machine
│   │   ├── transport.go    # UDP gönderim/alım, partition simülasyonu
│   │   └── protocol.go     # SWIM döngüsü (ping/ack/ping-req/suspect/dead)
│   └── loadbalancer/
│       ├── proxy.go        # HTTP reverse proxy, backend havuzu, fast-path cache
│       ├── roundrobin.go   # Round Robin (O(1), pre-sorted)
│       └── leastconn.go    # Least Connections (O(N) atomic scan)
└── scripts/
    ├── run_cluster.sh  # Linux/macOS cluster scripti
    └── run_cluster.bat # Windows cluster scripti (CRLF)
```

## Hızlı Başlangıç

### Gereksinimler

- Go 1.21+
- Windows için: PowerShell veya CMD (ENABLE_DEBUG=1 ortam değişkeniyle)

### Derleme

```bash
# Linux/macOS
./scripts/run_cluster.sh build

# Windows
scripts\run_cluster.bat build
```

Binary'ler `bin/` dizinine yazılır.

### Cluster Başlatma

**Linux/macOS:**
```bash
ENABLE_DEBUG=1 ./scripts/run_cluster.sh start
```

**Windows:**
```cmd
scripts\run_cluster.bat start
```

4 backend düğümü ve 1 load balancer başlar:

| Bileşen      | HTTP    | Gossip UDP     |
|-------------|---------|----------------|
| node-a      | :8001   | localhost:9001 |
| node-b      | :8002   | localhost:9002 |
| node-c      | :8003   | localhost:9003 |
| node-d      | :8004   | localhost:9004 |
| loadbalancer| :8080   | localhost:9000 |

### Doğrulama

```bash
# Tek istek
curl http://localhost:8080/

# Gossip üyelik görünümü
curl http://localhost:8001/members

# Sağlık kontrolü
curl http://localhost:8001/health
```

## Yük Dengeleme Stratejileri

### Round Robin (varsayılan)
Pre-sorted backend listesinden atomic counter ile O(1) seçim. Üyelik değişmediği sürece allocation yok.

### Least Connections
`-strategy=leastconn` bayrağıyla aktifleştirilir. Her `Select()` çağrısında en az aktif bağlantıya sahip backend seçilir. `ActiveConns` atomic counter ile kalıcı backend havuzunda korunur.

```bash
./bin/loadbalancer -listen=:8080 -strategy=leastconn \
  -gossip=localhost:9000 -seeds=localhost:9001,localhost:9002
```

## Hata Simülasyonu (ENABLE_DEBUG=1)

### Düğüm Öldürme
```bash
./bin/simulator -cmd=kill -node=node-c
```

### Network Partition
```bash
# node-b'yi 15 saniye gossip ağından izole et (çift yönlü UDP drop)
./bin/simulator -cmd=partition -node=node-b -duration=15s

# Convergence durumunu kontrol et
./bin/simulator -cmd=metrics
```

### Stress Testi
```bash
# 1000 istek → dağılım std sapması kontrolü (K6 hedefi: <%15)
./bin/simulator -cmd=stress -count=1000
```

### Failover Ölçümü
```bash
# K1 hedefi: node öldükten sonra ≤5s içinde failover
./bin/simulator -cmd=failover -node=node-c
```

## HTTP API

Her backend düğümü şu endpoint'leri sunar:

| Endpoint            | Yöntem | Açıklama                              |
|--------------------|--------|---------------------------------------|
| `/`                | GET    | `{"node":"node-a","status":"ok"}` döner |
| `/health`          | GET    | Sağlık kontrolü                       |
| `/members`         | GET    | Gossip üyelik listesi (JSON)          |
| `/debug/kill`      | POST   | Düğümü 100ms sonra kapat *(ENABLE_DEBUG=1)* |
| `/debug/partition` | POST   | UDP gossip'i duraklat *(ENABLE_DEBUG=1)*, `?duration=15s` |

## SWIM Protokol Parametreleri

| Parametre         | Varsayılan | Açıklama                                  |
|-------------------|-----------|-------------------------------------------|
| T_protocol        | 1s        | Gossip döngü aralığı                      |
| T_ping            | 500ms     | Direkt ping timeout                       |
| T_indirect        | 1s        | Dolaylı ping (PING_REQ) timeout           |
| T_suspect         | 3s        | Suspect → Dead geçiş süresi              |
| K (IndirectNodes) | 3         | PING_REQ için aracı sayısı               |

## Testler

```bash
go test -v ./pkg/gossip/ ./pkg/loadbalancer/
```

26 test: membership state machine, SWIM ping/ack entegrasyon, proxy routing, selector fairness.

## Tasarım Kararları

- **Sıfır external dependency:** Sadece Go standart kütüphanesi.
- **Fast-path backend cache:** `sortedCache []*Backend` — membership değişmediğinde allocation yok, O(N) kontrol + RLock.
- **Kalıcı backend havuzu:** `backendPool map[string]*Backend` — `ActiveConns` request'ler arasında korunur (LeastConn doğruluğu için kritik).
- **Çift yönlü partition:** UDP `Send()` + `receiveLoop()` her ikisi de `paused` kontrol eder — gerçekçi ağ bölünmesi.
- **Windows uyumluluğu:** `normalizeAddr()` `:port` → `127.0.0.1:port` (Windows `wsasendto` hatası önlenir).
