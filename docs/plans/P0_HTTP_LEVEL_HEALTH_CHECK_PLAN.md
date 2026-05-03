# P0 Plan: HTTP-Level Health Check for Routing Safety

## 1. Objective

Add application-level health probing to the load balancer so routing decisions are based on both:

- gossip liveness (node-level network membership)
- HTTP service health (application-level readiness)

This reduces misrouting when a node is reachable by gossip but not healthy for serving requests.

## 2. Why This Is P0

- Gossip can report a node as alive while the app is degraded (handler timeouts, internal lockups, partial failures).
- End users currently hit these failures directly and receive avoidable errors.
- Active health checks provide a fast local safety layer before gossip convergence completes.

## 3. Scope

In scope:

- Add periodic HTTP probe loop in load balancer.
- Keep a local health state map per backend.
- Exclude unhealthy backends from routing candidate set.
- Integrate with existing retry + eviction behavior.
- Add tests for probe state transitions and routing behavior.

Out of scope:

- Cross-node health gossip propagation.
- Deep synthetic checks (DB, dependency chains, custom scripts).
- External monitoring integrations.

## 4. Target Behavior

Health source:

- Probe endpoint: `GET /health` on each backend `ServiceAddr`.

Probe policy (default):

- Interval: `2s`
- Timeout: `500ms`
- Failure threshold: `2` consecutive failures -> mark unhealthy
- Success threshold: `1` success -> recover healthy

Routing rules:

- Candidate backends must be:
  - alive by gossip
  - not locally quarantined
  - healthy by HTTP probe state
- If all are unhealthy:
  - do one fallback attempt on alive backends (for transient false negatives)
  - then return `502` if still no successful route

Fail handling:

- Transport-level proxy failures continue to trigger immediate local eviction/quarantine.
- Health state and quarantine state remain separate but both are required for selection.

## 5. Implementation Plan

1. Extend `LBConfig` with health check options and safe defaults.
2. Add health tracker to load balancer state:
   - per-node status
   - failure/success counters
   - last check timestamp
3. Start background probe goroutine from `LoadBalancer.Start(ctx)`.
4. Update backend candidate filtering to enforce healthy-only selection.
5. Add structured logs for probe changes:
   - healthy -> unhealthy
   - unhealthy -> healthy
6. Keep behavior deterministic when zero healthy nodes are available.

## 6. Testing Plan

Unit/behavior tests:

- Healthy backend is selectable.
- Backend becomes unhealthy after threshold failures and is excluded.
- Backend recovers after required successful probes.
- Mixed cluster (healthy + unhealthy) routes only to healthy backends.
- All unhealthy path returns `502` after bounded fallback attempts.

Integration checks:

- Start cluster and stop one node service while gossip still sees it.
- Verify LB rapidly stops routing to the broken node.
- Confirm traffic resumes to recovered node after health recovery.

## 7. Acceptance Criteria

- Routing errors caused by app-level backend failures are significantly reduced.
- Unhealthy backends are excluded within one probe window + threshold.
- Recovery is automatic without process restart.
- `go test ./...` remains green with new health-check tests.

## 8. Risks and Mitigations

Risk: false unhealthy due to aggressive timeout  
Mitigation: configurable timeout and thresholds, conservative defaults.

Risk: probe overhead on large backend sets  
Mitigation: lightweight endpoint + interval tuning.

Risk: conflict with gossip state transitions  
Mitigation: treat health filter as local routing guard, not membership replacement.
