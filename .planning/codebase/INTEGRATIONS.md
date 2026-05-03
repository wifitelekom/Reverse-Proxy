# INTEGRATIONS

## Overview
- Internal integration is between gossip membership (`pkg/gossip`) and request routing (`pkg/loadbalancer`).
- External protocol integration is minimal: HTTP endpoints and UDP gossip packets.
- No external SaaS, database, queue, or cloud SDK integration exists.

## Evidence (specific files)
- `pkg/loadbalancer/proxy.go`
- `pkg/gossip/protocol.go`
- `pkg/gossip/transport.go`
- `cmd/node/main.go`
- `cmd/loadbalancer/main.go`
- `cmd/simulator/main.go`
- `scripts/run_cluster.sh`
- `scripts/run_cluster.bat`

## Current behavior
- `cmd/node` and `cmd/loadbalancer` send startup ALIVE messages to configured seeds.
- `pkg/loadbalancer` reads alive backends via `MembershipList.GetAliveMembers()` and proxies traffic with `httputil.ReverseProxy`.
- `cmd/simulator` integrates over HTTP debug/status endpoints (`/debug/kill`, `/debug/partition`, `/members`, `/health`, `/`).
- Scripts orchestrate local multi-process environments and log to temporary OS folders.

## Risks/Gaps
- Seed join is best-effort on startup (warning log on failure, no handshake confirmation).
- Simulator integration is hard-coded to a fixed localhost topology.
- No auth/TLS integration on node or LB HTTP endpoints.

## Recommendations
- Add retry/backoff or handshake confirmation for initial seed join reliability.
- Externalize simulator topology and addresses into CLI/config.
- Add optional auth/TLS modes for non-local deployments.

## Inference
- The project is intentionally local-first and experiment-oriented, not production-hardened, based on debug-first scripts and fixed topology defaults.

