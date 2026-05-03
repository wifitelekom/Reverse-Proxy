# P2P Gossip Load Balancer - Mac Developer Guide

This guide is for local development and testing on macOS (zsh/bash).
It focuses on running the cluster, simulating failures, and validating behavior.

## 1) Prerequisites

- macOS terminal (`zsh` is fine)
- Go 1.21+

Check:

```bash
go version
```

If needed, make scripts executable once:

```bash
chmod +x ./scripts/run_cluster.sh
```

## 2) Build and Start the Cluster

From repo root:

```bash
./scripts/run_cluster.sh build
./scripts/run_cluster.sh start
```

What starts:

- `node-a`: HTTP `:8001`, gossip UDP `:9001`
- `node-b`: HTTP `:8002`, gossip UDP `:9002`
- `node-c`: HTTP `:8003`, gossip UDP `:9003`
- `node-d`: HTTP `:8004`, gossip UDP `:9004`
- `loadbalancer`: HTTP `:8080`, gossip UDP `:9000`

Notes:

- `run_cluster.sh start` enables debug endpoints automatically by exporting `ENABLE_DEBUG=1`.
- Logs are written under `/tmp/p2p-cluster/logs`.

## 3) Basic Health Verification

```bash
./scripts/run_cluster.sh status
curl http://localhost:8080/
curl http://localhost:8001/health
curl http://localhost:8001/members
```

Expected:

- LB should return JSON from one of the nodes on `:8080`.
- `/health` should return healthy.
- `/members` should list nodes with `ALIVE/SUSPECT/DEAD` states.

## 4) Failure Simulation Commands (macOS)

Run all commands from repo root after cluster is up.

### A) Kill a Node

```bash
./bin/simulator -cmd=kill -node=node-c
```

What it does:

- Calls `POST /debug/kill` on `node-c`.
- Node process exits shortly after.

Verify:

```bash
./scripts/run_cluster.sh status
curl http://localhost:8001/members
```

### B) Network Partition

```bash
# Isolate node-b from gossip for 15 seconds (bidirectional UDP drop simulation)
./bin/simulator -cmd=partition -node=node-b -duration=15s

# Inspect convergence/membership views
./bin/simulator -cmd=metrics
```

What it does:

- Node stays HTTP-reachable but its gossip transport is paused for the duration.

### C) Stress Test

```bash
# Send 1000 requests to LB and print distribution stats
./bin/simulator -cmd=stress -count=1000
```

What it prints:

- success/failure counts
- per-node request distribution
- coefficient of variation (CV) and K6 target check (`<15%`)

### D) Failover Measurement

```bash
# Kill node-c and measure failover stabilization time
./bin/simulator -cmd=failover -node=node-c
```

What it measures:

- Time from kill request to stable routing away from killed node.
- K1 target is failover in `<=5s`.

## 5) Typical Developer Workflow on Mac

```bash
# 1) Build and start
./scripts/run_cluster.sh build
./scripts/run_cluster.sh start

# 2) Smoke check
curl http://localhost:8080/

# 3) Run one simulation
./bin/simulator -cmd=partition -node=node-b -duration=10s
./bin/simulator -cmd=metrics

# 4) Run tests
go test ./...

# 5) Stop cluster
./scripts/run_cluster.sh stop
```

## 6) Logs and Debugging

Tail logs:

```bash
tail -f /tmp/p2p-cluster/logs/node-a.log
tail -f /tmp/p2p-cluster/logs/node-b.log
tail -f /tmp/p2p-cluster/logs/node-c.log
tail -f /tmp/p2p-cluster/logs/node-d.log
tail -f /tmp/p2p-cluster/logs/lb.log
```

Useful quick checks:

```bash
curl -s http://localhost:8080/
curl -s http://localhost:8001/members | jq .
```

(`jq` is optional; if not installed, remove `| jq .`)

## 7) macOS Troubleshooting

### Port already in use

Find process:

```bash
lsof -nP -iTCP:8080 -sTCP:LISTEN
lsof -nP -iTCP:8001 -sTCP:LISTEN
```

Kill process by PID:

```bash
kill <PID>
```

### Cluster stopped unexpectedly

```bash
./scripts/run_cluster.sh status
./scripts/run_cluster.sh restart
```

### `go test` cache permission issues in restricted environments

If your environment blocks `~/Library/Caches/go-build`, use a writable temp cache:

```bash
GOCACHE=/tmp/go-build-cache go test ./...
```

## 8) Clean Shutdown

Always stop cluster after tests:

```bash
./scripts/run_cluster.sh stop
```

If any process remains, inspect with:

```bash
ps aux | grep -E "bin/node|bin/loadbalancer|bin/simulator" | grep -v grep
```
