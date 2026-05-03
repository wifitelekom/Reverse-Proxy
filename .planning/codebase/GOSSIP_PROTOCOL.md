# Gossip/Membership Subsystem Analysis

## Purpose and data model

### Direct evidence
- `pkg/gossip/types.go` defines a SWIM-like state model with `StateAlive`, `StateSuspect`, and `StateDead`.
- `NodeInfo` carries identity (`ID`), gossip address (`Addr`), service address (`ServiceAddr`), state, incarnation, heartbeat time, and current load.
- `GossipMessage` supports `PING`, `ACK`, `PING_REQ`, `DEAD`, `ALIVE`, and `SUSPECT`, with piggybacked membership snapshots in `Members`.
- `cmd/node/main.go` initializes local node state as `Alive` and injects it into the shared membership list at startup.

### Inference
- The protocol prioritizes eventual convergence via piggybacked membership dissemination rather than strict consensus.
- `Load` inside `NodeInfo` suggests cross-module coupling with load balancer strategy decisions.

## Membership lifecycle

### Direct evidence
- `pkg/gossip/membership.go:AddOrUpdate` applies incarnation-first conflict resolution:
  - lower incarnation is ignored,
  - higher incarnation wins,
  - equal incarnation accepts only worse state progression (`Alive < Suspect < Dead`).
- `MarkSuspect` transitions a member to `Suspect` and starts a timer that promotes to `Dead` via `markDead`.
- `Defend` increments local incarnation and forces local state to `Alive` when self is suspected/dead by peers.
- `GetAliveMembers` excludes the local node and members without `ServiceAddr`.

### Inference
- State progression is intentionally monotonic at a fixed incarnation, reducing flapping and stale resurrection.
- Defense mechanism follows the SWIM-style "refute by higher incarnation" pattern.

## Gossip message flow

### Direct evidence
- `pkg/gossip/protocol.go:runProtocolRound` executes direct ping first, then indirect ping (`PING_REQ`) through `K` random intermediaries, then marks suspect on timeout.
- `handleMessages` processes every incoming message and first merges piggybacked `Members` into local membership.
- `handlePing` replies `ACK` and includes full membership piggyback.
- `handleAck` resolves pending probes through a `sync.Map` keyed by sequence number.
- `handlePingReq` performs delegated probe and forwards `ACK` to requester if target responds in time.
- `suspectNode` and `gossipDead` broadcast suspect/dead updates to random peers.

### Inference
- Piggyback-first processing improves anti-entropy speed by turning every packet into membership synchronization.
- Indirect probing reduces false positives under transient one-hop path issues.

## Transport behavior and reliability assumptions

### Direct evidence
- `pkg/gossip/transport.go` uses UDP (`net.ListenUDP`, `WriteToUDP`) and JSON payload encoding.
- Receive loop has read deadlines (100ms) to permit responsive context cancellation.
- Backpressure policy is drop-on-full (`recvCh` non-blocking send with warning).
- Partition simulation is built-in via `Transport.SetPaused`, dropping both inbound and outbound gossip.
- `normalizeAddr` rewrites `:port` to `127.0.0.1:port` to handle platform-specific UDP targeting issues.

### Inference
- Reliability model is best-effort delivery; protocol correctness depends on periodic retries and gossip redundancy.
- Under sustained receive-channel saturation, message loss can increase detection latency and temporary inconsistency.

## Failure scenarios and healing behavior

### Direct evidence
- If direct + indirect probes time out, node transitions to `Suspect`, then `Dead` after `SuspectTimeout`.
- On receiving `SUSPECT` or `DEAD` targeting self, node calls `Defend` and announces `ALIVE` with higher incarnation.
- `cmd/node/main.go` supports chaos endpoints (when `ENABLE_DEBUG=1`) to kill node or simulate partition by pausing transport.
- Tests (`membership_test.go`, `protocol_test.go`) validate stale update rejection, suspect->dead timeout path, and ping/ack success/failure behavior.

### Inference
- Self-defense plus ALIVE rebroadcast provides automatic healing from false positives once connectivity recovers.
- Bootstrap via one-shot seed `ALIVE` messages may delay convergence if initial seeds are unreachable.

## Risks/Gaps

### Direct evidence
- No cryptographic/authentication checks on gossip messages (`transport.go` + `protocol.go`), so any sender able to reach UDP port can inject membership updates.
- Protocol tests are limited to small in-process scenarios; there is no broader resilience suite for packet loss, duplication, reordering, or larger cluster scales.
- `runProtocolRound` piggybacks `GetAllMembers()` on ping traffic, which grows message size with cluster size.

### Inference
- Full membership piggyback on every probe can cause bandwidth amplification and larger UDP fragmentation risk as node count increases.
- Lack of anti-entropy compaction (e.g., digest/delta dissemination) may reduce scalability beyond small clusters.
- Node heartbeat timestamp exists in data model but is not used for failure decisions, leaving timeout logic entirely probe-driven.

## Recommendations

### Direct evidence based actions
- Add message authentication (shared key HMAC at minimum) for gossip packets.
- Add network-fault tests: loss, reordering, duplication, and prolonged partitions.
- Add size-aware piggyback strategy (bounded subset or digest + pull) instead of unconditional full membership list.
- Add observability counters: dropped recv messages, probe timeouts, suspect/dead transitions, defend events.

### Inference-driven improvements
- Introduce configurable suspicion multipliers/adaptive timers to reduce false positives under variable latency.
- Add periodic full-sync/repair path for clusters experiencing long-term divergence.
- Consider lightweight rate limiting or per-peer trust checks for unsolicited state transitions.
