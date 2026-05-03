# Runtime Flows

## `cmd/node` Flow (Backend Node)

### Startup

1. Parse required flags (`-id`, `-gossip`, `-service`, optional `-seeds`), exit on missing required values.
2. Build protocol config via `types.DefaultConfig()`, override with CLI values, validate with `cfg.Validate()`.
3. Create local `NodeInfo` and add to a shared `MembershipList`.
4. Start UDP transport (`gossip.NewTransport(...).Start(ctx)`).
5. Start SWIM protocol (`gossip.NewProtocol(...).Start(ctx)`).
6. Send one ALIVE join message to each configured seed.
7. Start HTTP server with:
- `/` basic node response
- `/health` health check
- `/members` membership dump
- `/debug/kill` and `/debug/partition` only when `ENABLE_DEBUG=1`

### Steady State

- Gossip protocol exchanges membership updates over UDP.
- HTTP server continues serving service and observability endpoints.
- Optional debug endpoints can trigger self-termination (`/debug/kill`) or temporary gossip isolation (`/debug/partition`).

### Shutdown

1. Wait for OS signal (`SIGINT`/`SIGTERM`) or context cancellation.
2. Gracefully shut down HTTP server with 5s timeout.
3. Cancel context to stop protocol/transport goroutines.

## `cmd/loadbalancer` Flow (Reverse Proxy + Passive Gossip Member)

### Startup

1. Parse flags (`-listen`, `-strategy`, `-gossip`, optional `-seeds`).
2. Build and validate LB protocol config.
3. Create shared `MembershipList` and insert LB as alive member with empty `ServiceAddr` (non-backend marker).
4. Start UDP transport and gossip protocol.
5. Send one startup ALIVE join message to each seed.
6. Select backend strategy:
- `roundrobin` (default)
- `leastconn`
7. Construct `LoadBalancer` and start blocking HTTP service (`lb.Start(ctx)`).

### Steady State

- Membership data is kept current by gossip.
- For each HTTP request, LB selects from alive backend members (excluding LB/self) and forwards via reverse proxy.
- Signal handler cancels context for graceful shutdown path.

### Shutdown

1. Receive signal or context cancellation.
2. `lb.Start` returns after server shutdown path completes.
3. Process exits after structured log message.

## `cmd/simulator` Flow (Chaos/Test Driver)

### Command Modes

- `-cmd=kill -node=<id>`: calls target node `/debug/kill`.
- `-cmd=partition -node=<id> [-duration=15s]`: calls `/debug/partition`.
- `-cmd=metrics`: fetches `/members` from all default nodes.
- `-cmd=stress [-count=N]`: sends concurrent requests to LB, computes success/failure and distribution CV.
- `-cmd=failover -node=<id>`: kills a node and measures time until 5 consecutive responses avoid killed node.

### Operational Assumptions

- Assumes default cluster map (`node-a..node-d`) with fixed local service ports.
- Depends on debug endpoints being active for kill/partition commands.
- Uses short HTTP timeouts for active probing behavior.

## Cluster Script Interaction

- `scripts/run_cluster.sh` and `.bat` compile binaries, start 4 nodes + 1 LB, and enable debug endpoints by default (`ENABLE_DEBUG=1`).
- Linux/macOS script tracks PIDs in `/tmp/p2p-cluster`; Windows script kills processes by listening ports.
- Scripts provide status checks through `/health` and LB root endpoint probes.

## Inferred Runtime Characteristics

- Inference: convergence quality at startup depends on both seed reachability and subsequent gossip rounds, not startup join ACKs.
- Inference: runtime resilience is strongest for node liveness shifts already represented in membership; weakest around initial bootstrap under packet loss/port conflicts.
