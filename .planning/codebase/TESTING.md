# Testing

## Coverage Map

- `pkg/gossip/membership_test.go` covers membership conflict resolution, suspect/dead transitions, self-defense (`Defend`), alive-member filtering (self and LB exclusion), and random peer selection with exclusions.
- `pkg/gossip/protocol_test.go` covers protocol-level behavior for successful ping/ack across two running transports and timeout path leading to `SUSPECT`/`DEAD` progression.
- `pkg/loadbalancer/proxy_test.go` covers HTTP proxy behavior (request routing, `503` on no backends), invalid backend handling (`ServiceAddr` parse failure skip), backend pool pointer reuse, and graceful shutdown under context cancel.
- `pkg/loadbalancer/selector_test.go` covers round-robin order/fairness, least-connections selection, empty/single backend edge cases, and atomic read behavior for active connections.
- `cmd/*` binaries and `scripts/*` cluster orchestration are not directly unit/integration tested.

## Test Quality

- Strength: tests are behavior-focused and validate failure/edge paths, not only happy paths.
- Strength: protocol tests use real UDP sockets with reduced timeouts, which gives realistic signal for gossip timing behavior.
- Strength: load balancer tests use `httptest` backends and verify contract-level HTTP outcomes (`200`/`503`) and state reuse required for least-connections correctness.
- Limitation: protocol tests rely on sleeps/timeouts (`time.Sleep`) and can be timing-sensitive under slow CI or heavy load.
- Limitation: no explicit race-detector gate (`go test -race`) is defined in scripts/CI metadata in repo.

## Missing Tests

- No tests for `cmd/node/main.go`, `cmd/loadbalancer/main.go`, and `cmd/simulator/main.go` CLI flag validation and startup wiring.
- No multi-node integration test asserting cluster convergence end-to-end (node processes + LB + failure injection) as one automated scenario.
- No tests for debug endpoint authorization/guard behavior beyond `ENABLE_DEBUG` branch presence.
- No tests for degraded networks with packet loss, delayed ACK, or repeated partition/rejoin cycles.
- No tests around log/error observability contracts (expected structured fields and failure telemetry quality).

## Recommendations

- Add an integration test suite that boots 3-4 nodes plus LB in-process or via ephemeral subprocesses and validates: join convergence, kill failover SLA, partition healing, and LB routing shift.
- Add `go test -race ./...` as a standard quality gate and include it in cluster build/test workflow.
- Refactor argument parsing in `cmd/*` into testable helpers and add table-driven tests for invalid/missing flags.
- Convert timing-dependent protocol assertions to eventually/poll helpers with bounded retries to reduce flakiness.
- Add simulator contract tests for `stress`, `failover`, and `partition` output semantics and failure handling.
