# ARCHITECTURE

## Overview
This repository implements a decentralized load-balancing system in Go with SWIM-style gossip membership and an HTTP reverse proxy front door.  
Architecture is organized as process-level binaries in `cmd/*` and reusable domain packages in `pkg/*`, with `pkg/types` as shared configuration contracts.

## Components / Layout
- `cmd/node/main.go`: Backend node process. Hosts HTTP endpoints and participates in gossip as an active member.
- `cmd/loadbalancer/main.go`: Load balancer process. Runs HTTP reverse proxy and joins gossip as a passive member (no `ServiceAddr`).
- `cmd/simulator/main.go`: Operational test/chaos utility for kill, partition, stress, failover, and metrics collection.
- `pkg/gossip/*`: Membership state, UDP transport, and SWIM protocol round execution.
- `pkg/loadbalancer/*`: Backend selection strategies and reverse-proxy orchestration.
- `pkg/types/config.go`: Shared protocol and load balancer config structs plus validation.

## Control flow
1. Process startup (`cmd/node` / `cmd/loadbalancer`) parses flags and validates config (`Validate()`).
2. A shared `MembershipList` is initialized and local node info is inserted as `ALIVE`.
3. UDP `Transport` starts receive loop; `Protocol` starts gossip loop and message handler goroutines.
4. Seed bootstrap sends `ALIVE` gossip to known seed addresses.
5. `node` starts HTTP API (`/`, `/health`, `/members`, optional debug endpoints).
6. `loadbalancer` starts HTTP server and, per request:
- Reads alive members from gossip state.
- Syncs/reuses backend pool.
- Selects backend via strategy (`roundrobin` or `leastconn`).
- Proxies request and tracks active connections atomically.
7. Shutdown path: OS signal or context cancellation triggers graceful HTTP shutdown and gossip loop termination.

## Coupling and boundaries
- `cmd/*` binaries depend on `pkg/*`; `pkg/*` does not import `cmd/*` (clean executable/library boundary).
- `pkg/loadbalancer` depends on `pkg/gossip` abstraction through `MembershipList` API (`GetAliveMembers`), coupling LB availability directly to gossip correctness.
- `pkg/gossip/protocol.go` couples protocol runtime to `Transport` and `MembershipList`, with `pendingPings` map used for async ACK coordination.
- `pkg/types` is a low-level shared dependency used by both `node` and `loadbalancer`.
- `cmd/simulator` is loosely coupled via HTTP endpoints, but hard-codes cluster addresses.

## Risks/Gaps
- `cmd/simulator` hard-coded cluster topology (`defaultCluster`, `lbAddr`) makes architecture less portable across environments.
- Gossip and LB are co-located in `cmd/loadbalancer`; failure modes in gossip may directly degrade request routing quality.
- `Transport.Send` drops outbound messages silently when partition mode is on; this is intentional for simulation but can hide diagnostics during test runs.
- No clear abstraction for persistent/shared state snapshots outside in-memory membership, so process restarts reset local view until gossip reconverges.

## Recommendations
- Externalize simulator topology to config/flags to reduce environment coupling.
- Add explicit observability interface (metrics/tracing) at package boundaries (`gossip`, `loadbalancer`) for production diagnosis.
- Consider a thin interface around membership reads in LB (`AliveBackendsProvider`) to reduce direct dependency on concrete gossip implementation.
- Add architecture decision records (ADR) for key choices: passive LB membership, in-memory-only membership state, and selector design.

