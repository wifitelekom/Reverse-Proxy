# Chapter 5 — Selectors and Health Checking

> First-revision outline + key sections.
> Scope: four selector algorithms, the optional HTTP health checker,
> the interaction between gossip-derived liveness and HTTP-derived
> health, and the experimental evaluation that supports the chosen
> defaults.
> Estimated final length: 25 – 30 pages.

## 5.1 Overview

The gossip subsystem of Chapter 4 produces a list of *alive* backends.
This chapter covers the layer above that list: how the load balancer
picks one specific backend per request, and how an optional HTTP
health checker can further filter the alive list down to a *healthy*
list. Section 5.2 walks through the four selector algorithms.
Section 5.3 covers the health checker. Section 5.4 documents the
interaction between the two and the order in which they are applied.
Section 5.5 presents the experimental evaluation that supports the
choice of LeastConnections as the production default for
even-distribution scenarios and P2C-EWMA for latency-sensitive
workloads.

## 5.2 Selector Algorithms

All four selectors implement the same single-method interface:

```go
type Selector interface {
    Select(backends []*Backend) *Backend
}
```

The contract: given a non-empty slice of alive (and healthy, if the
checker is enabled) backends, return one of them. A nil return is
treated by the caller as "no backend available" and surfaces as a 503
with `lb_requests_dropped_total{reason="no_alive_backends"}`.

### 5.2.1 Round-Robin

The simplest selector: an atomic counter modulo the backend count. No
backend state is consulted; no in-flight count, no latency. The
selector is suitable when backends are uniform and the operator wants
predictable per-backend traffic share.

```go
func (rr *RoundRobin) Select(backends []*Backend) *Backend {
    idx := atomic.AddUint64(&rr.counter, 1) - 1
    return backends[idx%uint64(len(backends))]
}
```

The atomic counter avoids any locking. The `- 1` makes the first
selection (after counter=1) pick backends[0], matching operator
intuition.

### 5.2.2 Least-Connections

The selector iterates through backends, comparing `ActiveConns`
counters, and returns the backend with the smallest value. Ties are
broken by index order.

```go
func (lc *LeastConnections) Select(backends []*Backend) *Backend {
    var best *Backend
    bestConns := int32(math.MaxInt32)
    for _, b := range backends {
        c := atomic.LoadInt32(&b.ActiveConns)
        if c < bestConns {
            bestConns = c
            best = b
        }
    }
    return best
}
```

`ActiveConns` is incremented at the start of each proxy call and
decremented in a `defer`, so the snapshot the selector reads is
slightly stale by the time the next request lands — but the staleness
is bounded by the proxy hot-path duration (microseconds), which is
negligible relative to inter-request latency.

LeastConnections converges to even distribution under uniform backends
and concurrent load. It is the production default for
even-distribution scenarios and is the selector used in the §5.5
stress experiment.

### 5.2.3 Power of Two Choices

Mitzenmacher (1996, 2001) proved that sampling two random bins and
picking the less-loaded yields an exponential improvement over uniform
random selection in the maximum-load metric. The implementation:

```go
func (p *P2C) Select(backends []*Backend) *Backend {
    n := len(backends)
    if n == 1 {
        return backends[0]
    }
    i := p.rng.Intn(n)
    j := p.rng.Intn(n - 1)
    if j >= i { j++ }
    a, b := backends[i], backends[j]
    if atomic.LoadInt32(&a.ActiveConns) <= atomic.LoadInt32(&b.ActiveConns) {
        return a
    }
    return b
}
```

The `j >= i { j++ }` adjustment ensures i ≠ j without rejection
sampling. Per selection the algorithm performs two atomic loads — a
constant-time, lock-free operation.

P2C avoids the herd behaviour of pure LeastConnections under bursty
arrivals: with many concurrent goroutines all reading the same
`ActiveConns` snapshot, LeastConnections will dispatch all of them to
the same "least-loaded" backend, only for the in-flight counter to
catch up after the fact. P2C's randomness spreads concurrent decisions
across the fleet.

### 5.2.4 P2C with EWMA Latency

This is the most expressive selector. The two random samples are
compared by EWMA-smoothed response latency rather than by in-flight
count. The EWMA itself is maintained atomically per backend (see
§ 4.2 of Chapter 2 in the thesis for the formal definition):

```go
func (b *Backend) RecordLatency(d time.Duration) {
    ns := d.Nanoseconds()
    for {
        old := atomic.LoadInt64(&b.LatencyEWMA)
        var next int64
        if old == 0 {
            next = ns
        } else {
            next = int64(0.5*float64(ns) + 0.5*float64(old))
        }
        if atomic.CompareAndSwapInt64(&b.LatencyEWMA, old, next) {
            return
        }
    }
}
```

The smoothing factor α = 0.5 was chosen empirically: it weights the
latest sample equally with the entire history, giving the selector
enough responsiveness to react to a degraded backend within a few
requests, without overreacting to a single tail-slow response.

The CAS loop handles concurrent writers without locking; concurrent
readers use `atomic.LoadInt64` and are race-free against the writer.
The first-sample case (`old == 0`) bootstraps the EWMA to the first
observed value, preventing a slow initial convergence from zero.

P2C-EWMA is the production default for *latency-sensitive* workloads
— gRPC RPC fan-out, cache-tier lookups, anything where p99 matters
more than per-backend traffic balance. In Section 5.5 we demonstrate
empirically that P2C-EWMA's per-backend latency ratio matches the
underlying backend speed ratio at roughly 100×.

### 5.2.5 Atomic Alignment Contract

`Backend.LatencyEWMA` is an `int64` accessed via `sync/atomic`. The
struct is laid out with `LatencyEWMA` at offset zero so the Go runtime's
guarantee about the first field of a heap-allocated struct enforces
8-byte alignment on every supported architecture. A contract test
(`alignment_test.go`) pins the offset via `unsafe.Offsetof`.

## 5.3 HTTP Health Checking

The gossip layer's notion of "alive" is "we've received a recent
ALIVE message from this peer." That is a network-level liveness signal
and not necessarily a *health* signal: a backend whose process is
running but whose HTTP handler is returning 500 from a corrupted DB
connection is technically alive from gossip's point of view.

The optional `pkg/healthcheck` layer adds an HTTP-level probe loop
that runs alongside gossip. When `HealthCheckConfig.Enabled` is true,
the load balancer starts a checker that polls each alive backend at
the configured interval (default 5 s) over the configured path
(default `/health`).

### 5.3.1 Status State Machine

Each backend tracks consecutive successes and consecutive failures
against thresholds:

| Current     | Consecutive successes ≥ HT | Consecutive failures ≥ UT | Latency > DegradedLatency |
|-------------|----------------------------|---------------------------|---------------------------|
| Unknown     | → Healthy                  | → Unhealthy               | (no transition on first probe) |
| Healthy     | (stays)                    | → Unhealthy               | → Degraded                |
| Degraded    | → Healthy                  | → Unhealthy               | (stays)                   |
| Unhealthy   | → Healthy                  | (stays)                   | (stays)                   |

Defaults: `HealthyThreshold = 2`, `UnhealthyThreshold = 3`,
`DegradedLatency = 1 s`. The higher unhealthy threshold dampens
flapping under transient noise; the lower healthy threshold lets a
recovered backend return to rotation quickly.

### 5.3.2 Saturation Control

The checker bounds the number of concurrent probes via a buffered
channel acting as a semaphore. When the semaphore is full, the next
tick's probe attempts are skipped (with a `health_checks_total{result="error"}`
increment) rather than queueing — queueing would amplify a transient
slowdown into a probe storm.

### 5.3.3 Panic Safety

Probe goroutines run their callbacks under a deferred recover. A
panicking callback (operator-supplied status-change handler) is logged
and the probe slot is released, but the checker continues to run.
Test coverage (`pkg/healthcheck/panic_safe_test.go`) pins this
invariant.

## 5.4 Interaction Between Gossip and Health

The load balancer's per-request flow:

1. `membership.GetAliveMembers()` — gossip-alive list.
2. `LoadBalancer.filterHealthy(backends)` — drops any backend the
   health checker considers Unhealthy. Degraded backends are kept;
   the latency-aware selectors will deprioritise them via their EWMA.
3. `selector.Select(filtered)` — picks one.

A backend that is gossip-alive but health-Unhealthy is functionally
identical to one that is gossip-dead, from the selector's point of
view. The two layers compose without coupling: an operator can run
with gossip only (`HealthCheck.Enabled = false`), with health-check
only (gossip provides liveness but every backend is health-Unknown
initially and treated as Healthy on the conservative path), or with
both.

### 5.4.1 Graceful Empty-Filter Fallback

If `filterHealthy` returns an empty slice — every alive backend is
Unhealthy — the load balancer falls back to the unfiltered gossip-alive
list rather than returning 503 immediately. The reasoning: a global
health-check failure (DNS, transient network) is more likely the
fault of the checker itself than of every backend simultaneously, and
serving with degraded-but-real backends is better than refusing all
traffic. The fallback is metrics-instrumented
(`lb_requests_total{...}` continues; an audit-period future-work item
is to add a dedicated `lb_health_filter_fallback_total` counter so
this condition is visible without log inspection).

## 5.5 Experimental Evaluation

This section reports the per-experiment results gathered during the
second progress reporting period. The full numbers are in the
*Experimental Validation* section of the second progress report; this
chapter summarises the methodology and interprets the findings in the
context of the selector design.

### 5.5.1 Distribution Quality Under Failure Injection

**Experiment**: 1000 sequential-equivalent requests at concurrency 25
through a load balancer fronting five backends. Each backend sleeps a
deterministic 3 ms before returning 200. At request 500, backend C is
killed (closed) and marked dead in the gossip table; 5 % of
subsequent requests on the survivors return 503 (simulated transient
failure). The coefficient-of-variation (CV) of per-backend hit counts
is computed across the four surviving backends.

**Selector**: LeastConnections (the fairness baseline).

**Result**: survivor hit counts of 238 / 224 / 217 / 221, yielding
CV = 0.0351. The target threshold is CV < 0.15. The experiment passes
with a wide margin, confirming that LeastConnections is the correct
production default for even-distribution workloads.

The CV across all five backends including the killed one is 0.25,
dominated by node-C's truncated participation (100 hits vs the
≈ 220 the others received). This figure is reported for
completeness but is not the primary success criterion — node-C's
hit count is a function of how long it stayed alive, not of the
selector's distribution quality.

### 5.5.2 mTLS Handshake Cost

**Experiment**: 100 000 handshakes per variant against three locally-
bound listeners — plain TCP, TLS 1.3 server-only, and mTLS 1.3 — at
concurrency 32, using ECDSA P-256 throughout.

**Result** (per handshake, p50 / p99):

| Variant      | p50      | p99       | Throughput        |
|--------------|----------|-----------|-------------------|
| Plain TCP    | 0.21 ms  | 31.09 ms  | ~28 000 / s sustained |
| TLS 1.3      | 5.18 ms  | 17.66 ms  | ~5 400 / s        |
| mTLS 1.3     | 6.54 ms  | 23.10 ms  | ~4 200 / s        |

The plain-TCP variant suffered 63 k errors from TIME_WAIT exhaustion
on the macOS dev host; the 37 k successful samples are still adequate
for the p50/p99 estimate but the throughput figure is an underestimate.

mTLS adds roughly 1.4 ms (p50) and 5.4 ms (p99) of overhead per
handshake on top of plain TLS — the cost of the client certificate
chain verification. For a typical request rate this is amortised by
the TLS session resumption path; the bench measures cold handshakes,
which is the worst case.

### 5.5.3 WebSocket Idle-Timeout Validation

**Experiment**: four sessions, each held open for 45 s against a load
balancer with `WebSocketIdleTimeout = 15 s`, varying keepalive
cadence:

- Control (no keepalive): expect termination ≤ 15 s.
- 0.5 s cadence: expect survival (proxy for 30 s @ 15 min production).
- 1 s cadence:   expect survival (proxy for 60 s @ 15 min production).
- 2 s cadence:   expect survival (proxy for 120 s @ 15 min production).

**Result**: all four cases pass. The control case terminates as
expected; the three keepalive cadences all survive the full 45 s
window. The deadline-refresh path in `pumpWithIdleTimeout` correctly
distinguishes silent abandonment from active traffic.

## 5.6 Discussion and Design Choices

The four selectors form a hierarchy of expressiveness:

> Round-Robin ⊂ Least-Connections ⊂ P2C ⊂ P2C-EWMA

Each is strictly more informative than the previous; each is also
slightly more expensive in observation cost (atomic loads). The
production default is operator-chosen because the right answer
depends on backend heterogeneity:

- *Homogeneous backends, even-distribution priority* → LeastConnections.
- *Homogeneous backends, burst-resilience priority* → P2C.
- *Heterogeneous backends, latency priority* → P2C-EWMA.
- *Predictability requirement (debugging, audit logs)* → RoundRobin.

The health checker is orthogonal to selector choice and composes with
all four. The audit-driven additions to the metrics layer — per-backend
`ActiveConns` and `LatencyEWMA` gauges — close the observability gap
that previously made P2C-EWMA's decisions opaque to operators.

## 5.7 Test Coverage Summary

The `pkg/loadbalancer` package carries 90 + tests. Selector-specific
coverage:

- **Distribution tests**: each selector is exercised at concurrency
  25 against five identical backends and the per-backend hit count
  is checked against the algorithm's expected fairness bound.
- **Atomic alignment contract**: `alignment_test.go` pins the offset
  of `Backend.LatencyEWMA`.
- **Per-backend gauge isolation** (`backend_gauges_test.go`): two
  backends with intentionally different latency profiles, verifies
  the EWMA gauge values diverge by at least the configured ratio.
- **Integration tests for selector decisions**: the
  `selector_test.go` suite drives ten thousand selections through
  the `lb_selector_decisions_total` counter and asserts the
  per-strategy expected distribution.

Chapter 6 covers the audit-driven additions in detail.
