# P2P Gossip Load Balancer

A self-organizing HTTP load balancer that needs no central service registry.
Backends and load balancers discover each other via the SWIM gossip protocol;
HashiCorp's Lifeguard extensions harden it against false positives under
stress. Built in Go, race-clean, with ~210 tests covering everything from
the SWIM state machine to TLS hot-reload.

## What this replaces

In a typical setup, an HTTP load balancer relies on an external service
registry to know which backends exist:

| Stack | Registry | LB |
| --- | --- | --- |
| nginx + Consul | Consul + consul-template | nginx (reload on every change) |
| HAProxy + Consul | Consul template | HAProxy |
| Envoy | xDS control plane (often Istio Pilot) | Envoy |
| Kubernetes | etcd-backed Services | kube-proxy / Service mesh |

This project absorbs the registry tier. Backends gossip themselves into
existence; the LB observes the gossip ring directly. There is no extra
component to deploy, configure, or recover. **It does not replace nginx
for edge duties (TLS termination, WAF, caching, rate limiting per
client) — see [Limitations](#limitations).**

## Architecture

```
┌──────────┐     HTTP/HTTPS   ┌───────────────────┐
│  Client  │ ────────────────▶│  Load Balancer    │
└──────────┘                  │  (gossip member)  │
                              └────────┬──────────┘
                                       │ reverse proxy
              ┌────────────────────────┼────────────────────────┐
              ▼                        ▼                        ▼
         ┌─────────┐              ┌─────────┐              ┌─────────┐
         │ node-a  │              │ node-b  │              │ node-c  │
         │ :8001   │              │ :8002   │              │ :8003   │
         └────┬────┘              └────┬────┘              └────┬────┘
              │   UDP gossip (SWIM + Lifeguard extensions)      │
              └─────────────────────────┴───────────────────────┘
```

Every node — including the load balancer — is a gossip ring member.
Backends advertise themselves; the LB joins as a passive observer.
Membership changes propagate as gossip; the LB applies them to its
backend pool on every request without any external configuration push.

## Feature matrix

Everything is opt-in unless marked otherwise. Defaults preserve the
behaviour of a minimal SWIM + round-robin setup so flipping any switch
on doesn't surprise you.

| Subsystem | Feature | Default |
| --- | --- | --- |
| **Gossip** | SWIM protocol (Das, Gupta, Motivala 2002) | always on |
| | Self-Awareness — local node scales own probe timeouts | off |
| | Dogpile — independent confirmations shrink suspect timer | off |
| | Buddy System — direct suspect notifications for fast self-defense | off |
| | SenderID source validation (anti-spoof) | always on |
| **HTTP** | HTTP/1.1, HTTP/2 (via TLS ALPN) | always on |
| | WebSocket proxy (explicit Hijack + bidirectional copy) | always on |
| | gRPC streaming (FlushInterval=-1 on the reverse proxy) | always on |
| **Selectors** | Round-Robin | default |
| | Least-Connections | `-strategy=leastconn` |
| | Power-of-Two-Choices (Mitzenmacher 1996) | `-strategy=p2c` |
| | P2C + EWMA latency | `-strategy=p2c-ewma` |
| **TLS** | Frontend HTTPS (TLS 1.2 / 1.3) | off |
| | mTLS (client cert verification) | off |
| | Backend TLS (LB → backend HTTPS) | off |
| | Hot-reload of cert/key (atomic.Pointer, no handshake locks) | off |
| **Health** | Periodic HTTP probes with threshold gating | off |
| | Graceful fallback when filter empties (no 503 cascade) | always on when feature is on |
| **Config** | JSON config file | always on |
| | TOML config file (build tag `toml`) | optional |
| | `P2PLB_*` environment variables | always on |
| | CLI flag overrides | always on |
| **Observability** | Prometheus metrics (12 collectors + Go runtime) | off |
| | `/_lb/health` admin endpoint (JSON) | off |
| | `/_lb/metrics` admin endpoint (Prometheus exposition) | off |
| | Token-bucket rate limiter on `/_lb/*` admin paths | on when metrics is on |

## Quickstart

```bash
# Build
./scripts/run_cluster.sh build

# Start a 4-node + 1 LB cluster on localhost
ENABLE_DEBUG=1 ./scripts/run_cluster.sh start
```

| Component       | HTTP   | Gossip UDP       |
| --------------- | ------ | ---------------- |
| node-a          | :8001  | localhost:9001   |
| node-b          | :8002  | localhost:9002   |
| node-c          | :8003  | localhost:9003   |
| node-d          | :8004  | localhost:9004   |
| load balancer   | :8080  | localhost:9000   |

```bash
# Single proxied request
curl http://localhost:8080/

# Backend's view of the cluster
curl http://localhost:8001/members

# Per-backend health (if backend implements it)
curl http://localhost:8001/health
```

## Configuration

Three precedence layers. Highest wins:

1. CLI flags
2. `P2PLB_*` environment variables
3. JSON / TOML config file (`-config <path>`)
4. Built-in defaults

### Config file example (`configs/loadbalancer.json`)

```json
{
  "listen": ":8080",
  "strategy": "p2c",
  "gossip_addr": ":9000",
  "node_id": "lb-1",
  "seed_nodes": [":9001", ":9002"],
  "shutdown_timeout": "30s",
  "protocol": {
    "interval": "1s",
    "lifeguard_enabled": true
  },
  "tls": {
    "enabled": true,
    "cert_file": "/etc/lb/cert.pem",
    "key_file": "/etc/lb/key.pem",
    "min_version": "1.2"
  },
  "health_check": {
    "enabled": true,
    "interval": "5s",
    "path": "/health"
  }
}
```

Sample configs in `configs/` — `loadbalancer.json`, `loadbalancer.toml`,
`loadbalancer-lifeguard.json`, `loadbalancer-healthcheck.json`,
`node.json`.

### Environment variables (subset)

```
P2PLB_LISTEN=:8080
P2PLB_STRATEGY=p2c
P2PLB_GOSSIP_ADDR=:9000
P2PLB_SEED_NODES=:9001,:9002
P2PLB_SHUTDOWN_TIMEOUT=30s
P2PLB_LOG_LEVEL=info
P2PLB_LOG_FORMAT=json
P2PLB_PROTOCOL_PING_TIMEOUT=500ms
```

### CLI flags

```bash
./bin/loadbalancer \
  -config configs/loadbalancer.json \
  -strategy p2c \
  -lifeguard \
  -health-check \
  -metrics \
  -tls -tls-cert /etc/lb/cert.pem -tls-key /etc/lb/key.pem
```

| Flag | Type | Notes |
| --- | --- | --- |
| `-config` | path | JSON; TOML with `-tags toml` |
| `-listen` | addr | `:8080` |
| `-strategy` | string | `roundrobin` / `leastconn` / `p2c` / `p2c-ewma` |
| `-gossip` | addr | UDP gossip listener |
| `-seeds` | csv | Comma-separated seed addresses |
| `-node-id` | string | Gossip identity |
| `-shutdown-timeout` | duration | Graceful shutdown window |
| `-lifeguard` | bool | Enable SWIM Lifeguard extensions |
| `-health-check` | bool | Enable HTTP health probes |
| `-health-check-path` | string | Probe path (default `/health`) |
| `-tls` / `-tls-cert` / `-tls-key` | | Frontend HTTPS |
| `-tls-min-version` | `1.2` / `1.3` | TLS floor |
| `-tls-client-ca` | path | mTLS via client CA bundle |
| `-tls-auto-reload` | bool | Periodic cert reload |
| `-backend-tls` / `-backend-tls-skip-verify` / `-backend-tls-ca` | | LB→backend TLS |
| `-metrics` | bool | Expose Prometheus at `/_lb/metrics` |
| `-log-level` / `-log-format` | string | `debug|info|warn|error` / `text|json` |

## Endpoints

### Backend nodes

| Path | Method | Description |
| --- | --- | --- |
| `/` | GET | Plain JSON identity probe |
| `/health` | GET | Liveness — used by the LB's HTTP health checker |
| `/members` | GET | Backend's gossip view of the cluster |
| `/debug/kill` | POST | Schedules a node shutdown 100ms later. Requires `ENABLE_DEBUG=1` |
| `/debug/partition?duration=15s` | POST | Drops UDP gossip both directions for the duration. Requires `ENABLE_DEBUG=1` |

### Load balancer

The LB proxies any path that does not start with `/_lb/` to the
selected backend. The reserved `/_lb/` prefix serves admin endpoints:

| Path | Description |
| --- | --- |
| `/_lb/health` | JSON snapshot of every tracked backend's health record. Returns 503 with `{"status":"health_check_disabled"}` if the checker isn't running. |
| `/_lb/metrics` | Prometheus exposition format. Returns 503 with `{"status":"metrics_disabled"}` if `-metrics` is off. |

The admin paths share a token-bucket rate limiter (10 rps, burst 20)
when metrics are enabled, so a runaway scraper can't compete with
proxied traffic for goroutine slots.

## Metrics

Twelve subsystem counters plus the standard Go runtime / process
collectors. Every metric uses bounded-cardinality labels (no path,
no client IP).

```
lb_requests_total{backend, status_class, method}            # request counter
lb_request_duration_seconds{backend, status_class}          # histogram
lb_requests_dropped_total{reason}                           # pre-select 503s
lb_websocket_upgrades_total{backend}                        # WebSocket sessions
lb_backend_pool_size                                        # alive backend count
lb_selector_decisions_total{strategy, backend}              # who got chosen

gossip_messages_sent_total{type}
gossip_messages_received_total{type}
gossip_messages_rejected_total{reason}                      # spoofed messages
gossip_state_transitions_total{from, to}
gossip_lifeguard_self_awareness                             # noop score (0..max)

health_checks_total{result}
health_check_duration_seconds{result}
health_status_transitions_total{from, to}
```

`reason="source_mismatch"` on `gossip_messages_rejected_total` is the
clearest "you're being attacked" signal.

## Coexistence with nginx

Two clean topologies depending on what your edge looks like.

**Two-tier (recommended):**
```
Internet → nginx (edge) → ProjectLB (mesh) → backend nodes
                              ↑
                           gossip ring
```
nginx handles public-internet duties (TLS termination, per-client rate
limiting, virtual hosts, caching, gzip/brotli, WAF). ProjectLB does
internal mesh routing. Each tier specializes.

**nginx-bridge:**
A single ProjectLB observer instance exposes `/_lb/health`. A small
script polls that endpoint and rewrites nginx's upstream config when
membership changes. Replaces consul-template with gossip-driven
templating.

## Failure injection

`bin/simulator` ships with five commands for testing:

```bash
# Kill a node (uses /debug/kill on the target)
./bin/simulator -cmd=kill -node=node-c

# Bidirectional UDP partition for a fixed window
./bin/simulator -cmd=partition -node=node-b -duration=15s

# Compare every node's membership view side by side
./bin/simulator -cmd=metrics

# 1000-request stress test, reports CV (target K6: <15%)
./bin/simulator -cmd=stress -count=1000

# Time how long failover takes after a kill (target K1: <5s)
./bin/simulator -cmd=failover -node=node-c
```

## SWIM parameters

Defaults in `pkg/types.DefaultConfig()` — tunable via config file or env.

| Parameter | Default | Effect |
| --- | --- | --- |
| `protocol.interval` | 1s | Gossip loop period |
| `protocol.ping_timeout` | 500ms | Direct ping deadline |
| `protocol.indirect_timeout` | 1s | Indirect (PING_REQ) deadline |
| `protocol.suspect_timeout` | 3s | Suspect → Dead transition |
| `protocol.indirect_nodes` | 3 | K, fan-out for indirect ping |
| `protocol.lifeguard_enabled` | false | Master switch for SWIM extensions |
| `protocol.self_awareness_max` | 8 | Cap on noop score (drives timeout scaling) |
| `protocol.ping_timeout_min/max` | 500ms / 4s | Lifeguard ping bounds |
| `protocol.indirect_timeout_min/max` | 1s / 8s | Lifeguard indirect bounds |
| `protocol.suspect_timeout_min/max` | 3s / 24s | Lifeguard / Dogpile bounds |

When Lifeguard is on, Self-Awareness scales probe timeouts dynamically
between `*_min` and `*_max` based on the local node's `noop` score.
Dogpile shrinks the suspect window logarithmically as independent
nodes confirm a failure. See `pkg/gossip/lifeguard.go` for the formulas.

## Limitations

This project deliberately covers a narrow slice of what nginx-class
proxies do. Do **not** put it on the public internet without an edge
proxy in front of it.

| Missing | Why nginx ships it |
| --- | --- |
| Per-client rate limiting (only admin endpoints are limited) | Edge defense |
| Body size limits / slow-loris hardening | DoS defense |
| Request / response caching | Latency reduction |
| gzip / brotli compression | Bandwidth |
| Path / host-based routing, virtual hosts, URL rewriting | Multi-tenant edges |
| WAF / OWASP rules | Edge defense |
| HTTP/3 / QUIC | Modern transport |
| Configuration hot-reload (currently restart-only) | Zero-downtime ops |

For internal east-west traffic on a private network — the use case
this project actually targets — these are usually somebody else's
problem.

## Tests

```bash
go test ./...
go test -race ./...
```

~210 tests across six packages:

| Package | What's covered |
| --- | --- |
| `pkg/gossip` | SWIM state machine, Lifeguard formulas (Self-Awareness scaling, Dogpile log-decay), forged-sender rejection, end-to-end two-node convergence |
| `pkg/loadbalancer` | Selector distributions (RR, LeastConn, P2C, P2CEWMA), backend-pool reuse, TLS handshake (mTLS, MinVersion, hot-reload), WebSocket bidirectional copy, gRPC HTTP/2 ALPN, dropped-request metrics, admin rate limiter, alignment contract (`unsafe.Offsetof`) |
| `pkg/healthcheck` | Threshold transitions, callback-bound saturation, panic-safe slot release |
| `pkg/config` | JSON / TOML loading, env var overrides, CLI merge precedence, duration round-trip, validation |
| `pkg/metrics` | All collectors registered, Prometheus exposition format, isolated registries (no cross-test interference) |
| `pkg/types` | Always-on Lifeguard validation, ShutdownTimeout sanity |

`goleak.VerifyTestMain` runs after every test in `pkg/config`,
`pkg/metrics`, `pkg/healthcheck`, `pkg/gossip`, and `pkg/loadbalancer`.
Goroutine leaks fail the suite immediately rather than silently
accumulating.

## Project layout

```
.
├── cmd/
│   ├── node/           # Backend binary
│   ├── loadbalancer/   # LB binary
│   └── simulator/      # Failure injection / measurement tool
├── configs/
│   ├── loadbalancer.json|.toml
│   ├── loadbalancer-lifeguard.json
│   ├── loadbalancer-healthcheck.json
│   └── node.json
└── pkg/
    ├── types/          # Wire-level config types (ProtocolConfig, LBConfig, ...)
    ├── config/         # File-loadable configs, env, validation, merge
    ├── gossip/         # SWIM + Lifeguard, transport, membership
    ├── healthcheck/    # HTTP health checker with threshold state machine
    ├── loadbalancer/   # HTTP reverse proxy, selectors, TLS, WebSocket, admin
    └── metrics/        # Prometheus collectors
```

## Dependencies

Three direct, all from well-maintained sources:

```
github.com/prometheus/client_golang  # metrics
golang.org/x/time/rate               # admin rate limiter
go.uber.org/goleak                   # test-only — goroutine leak detection
```

Plus an optional TOML parser behind the `toml` build tag:

```
github.com/BurntSushi/toml           # only with -tags toml
```

## Algorithmic foundations

This project implements three published distributed-systems primitives:

- **SWIM** — Das, Gupta, Motivala. *Scalable Weakly-consistent
  Infection-style Process Group Membership Protocol.* Cornell, 2002.
- **Lifeguard** — Dadgar et al. (HashiCorp). *Lifeguard: Local Health
  Awareness for More Accurate Failure Detection.* arXiv:1707.00788.
- **Power of Two Choices** — Mitzenmacher. *The Power of Two Choices
  in Randomized Load Balancing.* PhD thesis, 1996.

The contribution of this project is the integration: putting these
primitives in one operable Go binary with the operational hooks
(metrics, hot-reload, graceful shutdown, configurable everything)
that make it deployable.

## License

Add your license of choice in `LICENSE` before publishing.
