# Concerns

## P0 - Debug Endpoints Can Trigger Node Kill/Partition If Exposed

- Evidence: `cmd/node/main.go` enables `/debug/kill` and `/debug/partition` handlers whenever `ENABLE_DEBUG=1`; `scripts/run_cluster.sh` and `scripts/run_cluster.bat` set `ENABLE_DEBUG=1` during start.
- Impact: operationally unsafe if this mode leaks to non-test environments; any caller reaching node HTTP port can kill or partition nodes.
- Why severity is P0: direct availability loss can be triggered remotely without authentication in debug mode.

## P1 - Gossip/LB Bootstrapping Uses Fire-and-Forget Seed Join Without Retry Backoff Policy

- Evidence: both `cmd/node/main.go` and `cmd/loadbalancer/main.go` send one ALIVE join message per seed at startup and continue; failures are logged as warnings.
- Impact: startup can succeed with partial discovery if transient UDP loss occurs at join time; convergence depends on later periodic protocol rounds rather than robust bootstrap confirmation.
- Why severity is P1: may not fail immediately, but can produce slow or inconsistent initial membership visibility.

## P1 - Runtime Orchestration Is Script-Centric and Relies on Fixed Ports

- Evidence: `scripts/run_cluster.sh` and `.bat` hardcode node ports (`8001-8004`, `9001-9004`, LB `8080/9000`) and process management assumptions.
- Impact: collisions with existing services lead to brittle startup; difficult to run concurrent clusters or deterministic CI parallel jobs.
- Why severity is P1: strongly affects reproducibility and operational reliability under non-ideal host conditions.

## P2 - Test Suite Lacks End-to-End Scenario Gate for SLO Claims

- Evidence: package-level unit tests exist, but no single automated suite validates full system SLOs (failover threshold, convergence timing, distribution fairness) across real process topology.
- Impact: README/project claims can regress silently when startup wiring or inter-process behavior changes.
- Why severity is P2: moderate release risk; functional regressions may pass unit tests but fail in cluster reality.

## P2 - Time-Based Protocol Tests May Be Flaky on Slow CI Nodes

- Evidence: `pkg/gossip/protocol_test.go` uses short timeout constants plus sleeps for state assertions.
- Impact: intermittent false negatives or positives under noisy scheduling conditions.
- Why severity is P2: quality friction and confidence degradation rather than direct production failure.

## Additional Notes

- Inference: no explicit authentication/TLS layer is visible in entrypoints for node or load balancer HTTP exposure.
- Inference: no persistent membership checkpointing means full state rebuild on process restart is expected behavior.

## Recommendations

- Make debug endpoints opt-in with stronger guardrails: secret token, bind-only localhost by default, and startup hard-fail if `ENABLE_DEBUG=1` in production profile.
- Add bootstrap retry/ack strategy (or initial sync handshake) for seed joins to reduce cold-start uncertainty.
- Parameterize cluster scripts for ports and node count; add collision checks before start.
- Add an automated end-to-end test target executing kill/partition/failover and validating SLO thresholds.
- Stabilize timing-sensitive tests with eventually/poll assertions and wider adaptive timeout windows.
