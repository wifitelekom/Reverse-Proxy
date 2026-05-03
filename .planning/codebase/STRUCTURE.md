# STRUCTURE

## Overview
Repository structure follows a conventional Go layout: entrypoint binaries under `cmd/`, reusable logic in `pkg/`, shell helpers in `scripts/`, and documentation artifacts at repo root.

## Components / Layout
- `go.mod`: Module definition (`p2p-loadbalancer`) targeting Go 1.21.
- `cmd/`
- `loadbalancer/main.go`: LB process lifecycle, gossip join, strategy selection, HTTP proxy server.
- `node/main.go`: Backend node lifecycle, gossip membership, HTTP service/debug endpoints.
- `simulator/main.go`: Runtime scenario tooling for operational checks.
- `pkg/`
- `types/config.go`: `ProtocolConfig`, `LBConfig`, defaults, and validation rules.
- `gossip/`
- `types.go`: Domain enums/types (`NodeState`, `GossipMessage`, `NodeInfo`).
- `membership.go`: Thread-safe membership store and SWIM state transitions.
- `transport.go`: UDP IO layer and partition simulation toggle.
- `protocol.go`: SWIM rounds, message handling, suspicion/dead gossip propagation.
- `loadbalancer/`
- `proxy.go`: Backend pool/cache lifecycle and reverse proxy request handling.
- `roundrobin.go`: O(1) atomic index selector.
- `leastconn.go`: O(N) scan selector based on `ActiveConns`.
- `scripts/run_cluster.sh`, `scripts/run_cluster.bat`: Multi-process local cluster lifecycle scripts.

## Control flow
1. Scripts (optional) build/start binaries.
2. Each binary bootstraps local config and runtime dependencies.
3. Gossip package maintains cluster membership and liveness transitions.
4. Load balancer package consumes alive membership snapshots to build routing candidates.
5. Simulator invokes HTTP/debug endpoints to induce and measure structural behavior under failures.

## Coupling and boundaries
- Strong internal cohesion in `pkg/gossip`: membership, protocol, and transport are separate files but runtime-coupled by shared types and callbacks.
- `pkg/loadbalancer` encapsulates selector strategy behind `Selector` interface, allowing policy swaps without changing proxy orchestration.
- `cmd/node` and `cmd/loadbalancer` duplicate some startup patterns (logger setup, signal wiring, seed join behavior), indicating potential shared bootstrap utility gap.
- `README.md` documentation aligns with actual folder structure and component intent.
- Inference: Diagram files (`*.mermaid`) appear documentation-only and are not wired into CI checks.

## Risks/Gaps
- Structural duplication across binaries can cause drift when lifecycle logic evolves.
- Tests are concentrated in `pkg/gossip` and `pkg/loadbalancer`; `cmd/*` integration boundaries are largely untested.
- No dedicated `internal/` boundary: all `pkg/*` packages are externally importable, which may make refactors harder if this repo becomes a library dependency.
- Simulator and scripts encode local defaults; multi-environment deployment structure is not present.

## Recommendations
- Introduce shared bootstrap helpers (logger, signal handling, seed join utility) to reduce duplicated process scaffolding.
- Add integration test harness covering `cmd/node` + `cmd/loadbalancer` interaction paths.
- If this project is intended as application-only, consider moving non-public packages to `internal/` to protect refactors.
- Add `docs/` directory and relocate architecture/operational documents for clearer separation from source code.

