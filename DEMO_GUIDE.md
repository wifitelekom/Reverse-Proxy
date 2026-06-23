# Demo & Test Guide — P2P Gossip Load Balancer

A step-by-step guide for demonstrating the project live. Everything here
runs on a laptop with **Go 1.25+** and no external services. Copy-paste
the commands as you go, or run the one-shot script (`scripts/demo.sh`)
described in §6.

> **TL;DR for the impatient:** `make test` proves correctness,
> `go run ./experiments/heterogeneous_bench` is the single most impressive
> number (P2C-EWMA ≈3× lower p99), and `./scripts/run_cluster.sh start`
> shows a real registry-free cluster you can curl through.

---

## 0. Prerequisites

```bash
go version          # expect go1.25.x
cd <project-root>   # the folder containing go.mod
```

Nothing else to install — no database, no Consul, no Docker. Every test
spins up its own in-process servers and tears them down.

---

## 1. The Test Suite — "it is correct"

The whole correctness story is the test suite running clean under the
**race detector**.

```bash
make test
```

**What to point at:**
- Every package line ends in `ok`.
- It ran with `-race` (the Makefile adds it) — so there are no data races
  across the gossip, health-check, and proxy goroutines.
- It ran with `-count=1` — results are not cached, every test really ran.

**Count the tests** (optional, if asked "how many tests?"):

```bash
go test -v ./... 2>/dev/null | grep -cE "^=== RUN"     # ~270 incl. sub-tests
go test -v ./... 2>/dev/null | grep -E "^=== RUN" | grep -v "/" | wc -l   # 255 top-level
```

**Both build flavours** (the optional TOML config loader lives behind a
build tag — CI tests both):

```bash
make test-all       # default build + `-tags toml`
```

Expected: every package `ok` in both passes.

---

## 2. Experiment 1 — Fair distribution under failure

Drives 1000 requests through 5 backends, **kills one mid-run**, injects
5 % transient failures on the survivors, and reports how evenly traffic
was distributed (coefficient of variation, CV).

```bash
go run ./experiments/stress
```

**What to point at** (last lines of output):
```
  node-a: 238 hits
  node-b: 223 hits
  node-c: 100 hits (killed at #500)
  node-d: 222 hits
  node-e: 217 hits
  CV across survivors (phase B): 0.0349  (target <0.15)
  STATUS: PASS
```
- node-c stops getting traffic the instant it is killed.
- The four survivors split near-evenly → **CV ≈ 3.5 %, well under the
  15 % target.**

---

## 3. Experiment 4 — The headline: P2C-EWMA on a mixed cluster ⭐

**This is the most impressive single result. Lead with it if short on
time.** Three backends at 1 ms / 5 ms / 20 ms, 2000 requests through each
of the four selectors.

```bash
go run ./experiments/heterogeneous_bench
```

**What to point at** (the summary table at the end):
```
selector      p50 (ms)  p90 (ms)  p99 (ms)  mean (ms)   distribution
roundrobin    5.24      20.28     20.63     8.98        fast=33% medium=33% slow=33%
leastconn     1.65      6.12      21.23     3.47        fast=75% medium=20% slow=5%
p2c           1.54      5.89      21.07     4.09        fast=65% medium=27% slow=8%
p2c-ewma      1.90      5.75      7.46      3.19        fast=64% medium=35% slow=0.8%
Best selector by p99: p2c-ewma
```
- Round-Robin spreads evenly → every 3rd request hits the slow backend →
  **p99 ≈ 20 ms.**
- P2C-EWMA learns the slow backend is slow and sends it **only 0.8 %** of
  traffic → **p99 collapses to ~7.5 ms — about 3× better.**
- Say: *"This is the empirical justification for the latency-aware
  selector — same cluster, one extra atomic read per request, 3× lower
  tail latency."*

---

## 4. Experiments 2, 3, 5, 6 — the rest of the validation

Run any of these if asked about TLS cost, WebSocket safety, portability,
or memory stability.

### 4a. mTLS handshake cost (Experiment 2)
```bash
go run ./experiments/mtls_bench -samples 5000      # 5k is plenty for a demo; 100000 for the paper
```
Point at: `tls13` p50 ≈ 5 ms, `mtls13` p50 ≈ 6.5 ms, **zero errors**.
mTLS adds ~1.4 ms — the cost of verifying the client certificate.

### 4b. WebSocket idle-timeout safety (Experiment 3) — ~3 min
```bash
go run ./experiments/ws_keepalive
```
Point at: 4/4 cases PASS. The **control** (no keepalive) is reaped at
exactly the 15 s idle window; the three keepalive cadences all survive.
This proves the H-4' fix: idle sessions are reaped, active ones are not.

### 4c. 32-bit ARM portability (Experiment 5)
```bash
GOOS=linux GOARCH=arm GOARM=5 go vet ./...      # alignment analysis, clean
GOOS=linux GOARCH=arm GOARM=5 go build ./...    # cross-compiles clean
```
Point at: both exit 0 with no output → no atomic-alignment hazard on
32-bit targets.

### 4d. Memory / goroutine stability (Experiment 6) — 5 min
```bash
go run ./experiments/soak -duration 1m     # 1m for a demo; 5m for the paper
```
Point at the final verdict: **`FLAT — no leak observable`**. Goroutines
and heap stay flat under sustained load + 8 long-lived WebSocket sessions.

---

## 5. The Live Cluster — "it really works end to end" ⭐

This is the most convincing part: a **real** 4-backend cluster + load
balancer, all discovering each other by gossip, with **no registry**.

### 5a. Build and start
```bash
./scripts/run_cluster.sh build
./scripts/run_cluster.sh start
```
This launches `node-a..d` (HTTP :8001–:8004, gossip UDP :9001–:9004) and
the load balancer (HTTP :8080, gossip :9000). Debug endpoints are enabled.

### 5b. Show the gossip membership — no registry involved
```bash
curl -s http://localhost:8001/members | python3 -m json.tool
```
Point at: node-a lists **all four** nodes — it learned them purely by
gossip, never from a config file or registry.

### 5c. Send traffic through the load balancer
```bash
for i in $(seq 1 8); do curl -s http://localhost:8080/ ; echo; done
```
Point at: responses rotate across `node-a..d` — the LB is proxying to
backends it discovered via the ring.

### 5d. Kill a backend and watch failover live
```bash
# In one terminal, hammer the LB:
while true; do curl -s http://localhost:8080/ ; echo; sleep 0.3; done

# In another terminal, kill a node:
go run ./cmd/simulator -cmd=kill -node=node-c
```
Point at: within a couple of gossip rounds the dead node **drops out of
rotation** and traffic continues uninterrupted across the survivors. No
manual reconfiguration, no registry update.

### 5e. Stop the cluster
```bash
./scripts/run_cluster.sh stop
```

> **Optional — Prometheus metrics:** the cluster script starts the LB
> without metrics. To show the `/_lb/metrics` endpoint, start an LB
> manually with `-metrics`:
> ```bash
> go run ./cmd/loadbalancer -listen=:8080 -gossip=:9000 \
>     -seeds=:9001,:9002,:9003,:9004 -strategy=p2c-ewma -metrics &
> curl -s http://localhost:8080/_lb/metrics | grep lb_backend_latency_ewma_seconds
> ```

---

## 6. One-shot demo script

For a hands-free walkthrough, run:

```bash
./scripts/demo.sh
```

It runs §1 (tests), §2 (stress), §3 (heterogeneous selector), and the
ARM cross-compile check in sequence, pausing between each so you can
narrate. Press **Enter** to advance, **Ctrl-C** to stop. It does **not**
start the live cluster (that needs its own terminals) — do §5 by hand
for the finale.

---

## 7. Quick reference — what each command proves

| Command | Proves | Headline number |
|---|---|---|
| `make test` | Correctness, race-clean | 255 tests, 0 races |
| `make test-all` | Both build flavours pass | default + `-tags toml` |
| `go run ./experiments/stress` | Even distribution under failure | CV 3.5 % vs 15 % target |
| `go run ./experiments/heterogeneous_bench` | **Latency-aware selection wins** | **P2C-EWMA ~3× lower p99** |
| `go run ./experiments/mtls_bench` | TLS termination is cheap | mTLS p50 ≈ 6.5 ms |
| `go run ./experiments/ws_keepalive` | WebSocket idle reaping correct | 4/4 cases pass |
| `GOARCH=arm GOARM=5 go vet ./...` | 32-bit safe | clean, no diagnostics |
| `go run ./experiments/soak` | No resource leak | FLAT goroutine/heap trend |
| `./scripts/run_cluster.sh start` | **Registry-free cluster works** | 4 nodes, gossip discovery |

---

## 8. Troubleshooting

| Symptom | Fix |
|---|---|
| `command not found: go` | Install Go 1.25+, ensure it's on `PATH` |
| `bind: address already in use` | A previous cluster is still up → `./scripts/run_cluster.sh stop`, or kill stray `loadbalancer`/`node` processes |
| `mtls_bench` shows many plain-TCP errors | Expected at high sample counts (TIME_WAIT port exhaustion) — it's a benchmark artefact, not an LB bug. Use `-samples 5000` for the demo |
| `ws_keepalive` / `soak` feel slow | They are time-based (45 s / up to 5 min). Use `-duration 1m` on soak for a demo |
| Cluster curl returns nothing | Wait ~2 s after `start` for gossip convergence, then retry |
| Want to reset everything | `./scripts/run_cluster.sh stop && make clean` |

---

*Prepared for the final project defense. All numbers reproduce on the
development host (Apple silicon, macOS, Go 1.25); your figures may vary
slightly but the relationships hold.*
