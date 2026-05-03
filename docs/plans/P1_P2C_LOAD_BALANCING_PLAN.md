# P1 Plan: Power of Two Choices (P2C) Load Balancing Strategy

## 1. Objective

Add a new `p2c` backend selection strategy to improve load distribution under variable traffic and reduce tail-latency compared to basic round-robin or naive least-connections scans.

Primary goals:

- reduce hotspot formation under bursty traffic
- keep selector path fast and simple
- provide a strong practical strategy for multi-instance LB usage

## 2. Why This Is P1

- P2C is a proven, low-cost algorithm with strong real-world balancing quality.
- It generally outperforms RR under uneven request durations.
- It is a high-impact feature with relatively low implementation complexity.

## 3. Scope

In scope:

- Implement `p2c` selector in `pkg/loadbalancer`.
- Register `-strategy=p2c` in load balancer startup.
- Keep compatibility with current backend pool model (`ActiveConns` atomics).
- Add tests and benchmark-style sanity checks.

Out of scope:

- Weighted P2C variants (capacity-aware).
- EWMA latency-aware balancing (future enhancement).
- Cross-LB coordinated global balancing.

## 4. Target Behavior

Selection algorithm:

1. If backend count is 0 -> return `nil`.
2. If backend count is 1 -> return that backend.
3. Randomly sample two distinct backends.
4. Choose the one with lower `ActiveConns`.
5. Tie-break deterministically (stable rule, e.g., lower `NodeID`).

Operational expectations:

- O(1) selection work per request.
- Lower overload probability than pure random or RR in variable workloads.

## 5. Implementation Plan

1. Add new selector file (e.g., `p2c.go`) implementing existing `Selector` interface.
2. Integrate strategy switch in `cmd/loadbalancer/main.go`:
   - `roundrobin | leastconn | p2c`
3. Ensure existing pre-sorted backend list remains compatible.
4. Keep no-allocation fast path where practical.
5. Update docs and examples with `-strategy=p2c`.

## 6. Testing Plan

Unit tests:

- Empty list returns `nil`.
- Single backend returns the only backend.
- Lower `ActiveConns` backend is preferred from sampled pair.
- Tie-break behavior is deterministic.

Behavior tests:

- Over many requests, distribution variance should be competitive vs RR.
- Under artificial slow-node conditions, P2C should reduce concentration on busy nodes.

Regression:

- Existing selector tests continue to pass.
- Retry/quarantine/health-check logic remains strategy-agnostic.

## 7. Acceptance Criteria

- `-strategy=p2c` works in load balancer runtime.
- P2C unit tests pass and demonstrate expected behavior.
- No regressions in existing routing/failover tests.
- Documentation includes when to prefer `p2c` over `roundrobin`/`leastconn`.

## 8. Risks and Mitigations

Risk: randomness can make test outcomes flaky  
Mitigation: deterministic RNG injection or seed control in tests.

Risk: tie behavior differences across runs  
Mitigation: explicit deterministic tie-break rule.

Risk: misunderstanding of P2C benefits without measurements  
Mitigation: include comparative stress test notes in docs.
