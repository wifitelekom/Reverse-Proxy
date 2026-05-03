# LOAD BALANCER

## Purpose and API surface
- Implements HTTP reverse proxy based on gossip-derived alive backend membership.
- Main API surface:
- `NewLoadBalancer(cfg, membership, selector, logger) *LoadBalancer`
- `(*LoadBalancer).Start(ctx) error`
- Strategy abstraction: `Selector` with implementations `RoundRobin` and `LeastConnections`.
- Entrypoint wiring: `cmd/loadbalancer/main.go` selects strategy and starts LB.

## Request routing flow
1. `handleRequest` reads alive members via `membership.GetAliveMembers()`.
2. `getOrSyncBackends` maps members to persistent `*Backend` instances (`backendPool`).
3. A selector chooses one backend.
4. `ActiveConns` increments atomically before proxying and decrements with `defer`.
5. `httputil.ReverseProxy` forwards request; proxy errors return `502`.

## Selection algorithms (round robin, least connections) behavior
- Round Robin:
- Precondition: backend list is pre-sorted by `NodeID` in `getOrSyncBackends`.
- Uses atomic `counter` and modulo index; O(1), allocation-free on select path.
- Least Connections:
- Scans list and picks minimum `ActiveConns`; O(N) per request.
- Uses atomic reads; tie-breaker is first minimal backend in current order.

## Failure handling and edge cases
- No alive backend -> `503 Service Unavailable`.
- Backend URL parse failure during sync -> backend skipped, request continues with valid pool.
- Reverse proxy failure -> custom `ErrorHandler` emits `502 Bad Gateway`.
- Context cancellation triggers graceful LB HTTP shutdown with 5s timeout.

## Concurrency/thread-safety notes
- Membership reads are thread-safe through gossip package locks.
- LB backend pool uses `poolMu` (`RLock` fast path, `Lock` rebuild path).
- Active connections use atomics on shared persistent backend objects.
- Selector operations are lock-free; round-robin counter is atomic.

## Risks/Gaps
- `getOrSyncBackends` performs full membership comparison on each request (O(N) check); acceptable for small N but linear.
- Least-connections has no smoothing/slow-start; new/recovered nodes may receive abrupt shifts.
- No per-backend circuit breaker/retry policy; transient backend errors surface directly as `502`.
- `newBackend` only validates parseability, not backend reachability, at registration time.

## Recommendations
- Add optional active health probing and temporary backend quarantine policy.
- Add passive failure accounting (e.g., consecutive proxy errors) before routing retries.
- Expose selector/route metrics (selection counts, per-backend error rates, active conns).
- Consider eventual sticky/session-aware policies if client affinity is required.

## Evidence
- `pkg/loadbalancer/proxy.go`
- `pkg/loadbalancer/roundrobin.go`
- `pkg/loadbalancer/leastconn.go`
- `cmd/loadbalancer/main.go`
- `pkg/loadbalancer/proxy_test.go`
- `pkg/loadbalancer/selector_test.go`

