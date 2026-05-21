# Chapter 4 — The Gossip Subsystem

> First-revision outline + key sections.
> Scope: SWIM realisation, Lifeguard extensions, anti-spoofing, transport layer.
> Estimated final length: 25 – 30 pages.

## 4.1 Overview

This chapter walks through the implementation of the membership and
failure-detection layer that allows every load balancer and every
backend in the system to maintain a consistent view of which peers are
alive, without any external coordination service. The implementation
follows the SWIM protocol introduced by Das, Gupta and Motivala (DSN
2002), augmented by HashiCorp's Lifeguard extensions (arXiv 1707.00788)
and a small set of project-specific hardening measures developed
during the two audit rounds described in Chapter 6.

Section 4.2 covers the core protocol — direct and indirect probing,
incarnation numbers, the suspect-to-dead transition, and the gossip
piggyback mechanism. Section 4.3 documents the three Lifeguard
extensions and the configuration knobs that enable them. Section 4.4
covers the UDP transport layer and the message format. Section 4.5
covers the trust-on-first-use (TOFU) anti-spoofing check that defends
the membership table against forged-sender attacks. Section 4.6 walks
through the operational metrics emitted by the subsystem. Section 4.7
summarises the test coverage.

## 4.2 Core SWIM Protocol

### 4.2.1 Protocol Periods and Random Probing

The protocol operates on a fixed-period tick (default 1 s, configurable
via `ProtocolConfig.ProtocolInterval`). On every period, each node
selects one peer uniformly at random from its current membership table
(excluding itself) and sends a PING message. If the peer ACKs within
the per-period ping timeout (default 500 ms), it is recorded as
ALIVE and the period completes.

### 4.2.2 Indirect Probing

If the direct PING times out, the protocol falls back to indirect
probing. The probing node selects `k` further peers (default `k = 3`,
configurable via `IndirectNodes`) and asks each one to ping the suspect
on its behalf via a PING-REQ message. Each indirect peer attempts a
direct PING to the suspect; on success it sends an ACK back to the
original probing node. If *any* of the k indirect probes returns an
ACK within the indirect timeout (default 1 s), the suspect is
considered ALIVE.

The indirect step is what distinguishes SWIM from naive heartbeat
schemes: a transient network glitch between two specific peers is
absorbed by the k-fold redundancy, so the failure detector reports a
peer as failed only when multiple independent paths concur.

### 4.2.3 Suspect → Dead Transition

If neither direct nor indirect probing succeeds, the suspect enters
the SUSPECT state. A timer of `SuspectTimeout` (default 3 s) is
armed. If the timer fires without any ALIVE refutation arriving from
gossip, the node transitions to DEAD and is removed from the
GetAliveMembers set.

The SUSPECT state is broadcast via piggyback so that other nodes can
contribute confirmations or refutations. Confirmations are gathered
under the Dogpile mechanism (§ 4.3.2); refutations come from the
suspect node itself, defending with a higher incarnation number
(§ 4.2.5).

### 4.2.4 Piggyback Dissemination

Every outgoing protocol message — PING, ACK, PING-REQ, ALIVE, SUSPECT,
DEAD — carries a piggyback payload containing the sender's view of
recent membership state changes. There is no separate gossip
dissemination protocol; state changes spread through the ring at the
same rate as the probe traffic itself. The piggyback payload is
size-capped to keep UDP datagrams under the configured MTU; older
state changes age out and are not re-broadcast.

### 4.2.5 Incarnation Numbers

Each node has an `Incarnation` counter, monotonically increasing.
Every gossip message that mentions a node carries that node's current
incarnation. The membership table's update rule:

> *Accept the incoming state if either the incarnation is strictly
> higher, or the incarnation is equal and the new state is "worse"
> (Dead > Suspect > Alive). Otherwise ignore.*

This rule lets a healthy node defend itself: on receiving a SUSPECT or
DEAD message about itself, the node bumps its own incarnation and
re-broadcasts an ALIVE message with the new value. The bumped
incarnation supersedes the SUSPECT entry in everyone else's table.

## 4.3 Lifeguard Extensions

Lifeguard, introduced by HashiCorp in 2017 to address production
issues observed in Memberlist deployments, adds three orthogonal
mechanisms on top of vanilla SWIM. All three are gated behind
`ProtocolConfig.LifeguardEnabled`; with the flag off, the protocol
behaves exactly like the 2002 paper.

### 4.3.1 Self-Awareness

Real systems do not have constant timing. A node experiencing GC
pauses, high CPU contention, or scheduler delays will see its outgoing
probes time out more frequently than usual, and if its probe timeouts
are fixed it will rapidly mistake healthy peers for dead ones. The
Self-Awareness mechanism scales the probe timeout in proportion to a
NACK-driven score:

> `effectiveTimeout = baseTimeout × (1 + score × multiplier)`

The score increases when other nodes suspect this node (NACK messages)
and decreases when probes succeed. The result: an overloaded node
probes its peers more patiently, *and* tolerates being suspected for
longer before transitioning, both of which reduce the false-positive
rate during transient overload.

### 4.3.2 Dogpile

When a node enters SUSPECT, multiple peers may simultaneously decide
to confirm. The Dogpile mechanism shrinks the SUSPECT-to-DEAD timeout
in proportion to the number of independent confirmers, using a
logarithmic decay:

> `effectiveTimeout = baseTimeout × max(minScale, 1 - log(1 + confirmers) / log(1 + maxConfirmers))`

A single confirmer keeps the full timeout (single noisy peer cannot
force premature death); a quorum of confirmers collapses the timeout
quickly (genuine failures are removed without delay).

The per-confirmer dedup is critical for the security posture: each
unique confirmer ID counts as one vote. Combined with the anti-spoof
TOFU check (§ 4.5), an attacker would need to compromise multiple
distinct peers to inflate the confirmer count.

### 4.3.3 Buddy System

The Buddy System sends a SUSPECT message directly to the suspect peer,
in addition to gossiping it. This gives the peer the earliest possible
opportunity to defend itself (§ 4.2.5) — short-circuiting the
SUSPECT-to-DEAD transition entirely when the peer is in fact alive.

## 4.4 Transport Layer

The transport is UDP. SWIM was designed around UDP and the
implementation preserves that choice: messages are small, retries are
the protocol's responsibility (not the transport's), and the lack of
connection state keeps the per-peer memory footprint bounded.

### 4.4.1 Message Format

Each message carries:

- `Type` — one of `MsgPing`, `MsgAck`, `MsgPingReq`, `MsgAlive`,
  `MsgSuspect`, `MsgDead`
- `Seq` — monotonically increasing sequence number for matching ACKs
  to PINGs (atomic counter; see § 4.4.2 for the alignment requirement)
- `SenderID` — string identifier of the originating node
- `Target` — payload field used by PING-REQ to identify the suspect
- `Members` — piggyback payload (compact representation of zero or
  more NodeInfo entries)

The wire encoding is JSON for the current iteration. A binary encoding
(MessagePack or a fixed-layout binary format) is a candidate
optimisation listed in the future-work section of Chapter 8.

### 4.4.2 Atomic Counter Alignment

The sequence counter `seqCounter` is an `atomic.Uint64` (formerly
`uint64` accessed via the `sync/atomic` package). On 32-bit ARM and
x86, the Go runtime requires 64-bit atomic operations to be performed
on 8-byte-aligned addresses; misalignment causes a runtime panic.
The Go runtime guarantees 8-byte alignment for the first field of a
heap-allocated struct, so the `Protocol` struct is laid out with
`seqCounter` as field zero. An `unsafe.Offsetof`-based contract test
in `pkg/gossip/alignment_test.go` pins the invariant against future
refactoring.

### 4.4.3 Membership Table Concurrency

The membership table is a `map[string]*nodeEntry` guarded by a
`sync.RWMutex`. All public methods that read or write the table take
the appropriate lock; the metrics hook is called *outside* the lock to
prevent a future hook implementation from deadlocking against the
membership list. Read-mostly operations (`GetAliveMembers`,
`GetRandomMembers`) use `RLock`; mutations (`AddOrUpdate`,
`MarkSuspect`, `ConfirmSuspect`) use `Lock`.

## 4.5 Anti-Spoofing: TOFU Source Pinning

### 4.5.1 Threat Model

The gossip protocol is unauthenticated. An attacker who can reach the
UDP listen port can send messages with arbitrary `SenderID` values.
The most damaging attack is *confirmer inflation*: by crafting
SUSPECT messages from many synthetic sender IDs, an attacker can fool
the Dogpile mechanism into believing many independent peers have
suspected a victim, collapsing the SUSPECT-to-DEAD timeout and
forcing premature removal.

### 4.5.2 The TOFU Check

The defence is *trust on first use*: the first message claiming a
given `SenderID` pins the UDP source address. Subsequent messages
claiming the same `SenderID` are required to arrive from the pinned
source. Mismatches are rejected before any piggyback processing.

The check accepts a TOFU window between the first message creating
the membership entry and the post-piggyback pin step. During that
window, a single forged sender ID can pin one source binding;
attackers gain nothing by pinning a fake ID to a real address (the
real peer is unaffected) and cannot un-pin a real ID. Per-confirmer
deduplication then bounds the leverage of any single bound pair.

### 4.5.3 Source-Address Canonicalisation

The naive comparator — raw string equality on `net.UDPAddr.String()`
— fails on two real-world cases. Link-local IPv6 addresses pick up a
`%zone` suffix from the receiving interface (`[fe80::1%eth0]` vs
`[fe80::1%lo0]`), and dual-stack listeners can report the same peer
as plain IPv4 or as IPv4-mapped IPv6. The implementation routes every
source string through `canonicalSourceAddr`, which uses
`netip.ParseAddrPort` plus `Addr.Unmap()` plus `Addr.WithZone("")` to
fold both shapes into a single canonical form. Without this
normalisation, multi-homed hosts experience false-positive
impersonation rejections — a finding raised as audit item H-1' and
fixed during the reporting period.

## 4.6 Operational Metrics

Five Prometheus collectors instrument the gossip subsystem:

| Metric                              | Type      | Labels         | Use                                   |
|-------------------------------------|-----------|----------------|---------------------------------------|
| `gossip_messages_sent_total`        | Counter   | `type`         | Traffic volume and pattern            |
| `gossip_messages_received_total`    | Counter   | `type`         | Per-type inbound rate                 |
| `gossip_messages_rejected_total`    | Counter   | `reason`       | Spoof attempts (`source_mismatch`)    |
| `gossip_state_transitions_total`    | Counter   | `from`, `to`   | Membership churn observability        |
| `gossip_lifeguard_self_awareness`   | Gauge     | —              | Current Self-Awareness score          |

All labels have small, bounded value spaces — five message types, a
handful of rejection reasons, three states. No high-cardinality field
(no remote address, no message body) is allowed as a label, which
preserves the Prometheus registry against cardinality DoS.

## 4.7 Test Coverage Summary

The gossip package carries 60 + tests, all running under the Go race
detector and `goleak.VerifyTestMain`. Major test categories:

- **State machine unit tests**: every transition (alive → suspect,
  suspect → alive, suspect → dead, defended-via-incarnation-bump)
  has a dedicated test.
- **Lifeguard formula tests**: each of Self-Awareness, Dogpile and
  Buddy System has a property-style test pinning the
  configured-vs-observed scaling.
- **Anti-spoof tests** (`sender_validation_test.go`,
  `source_normalization_test.go`): TOFU pinning, impersonation
  rejection, zone-drift acceptance.
- **End-to-end convergence tests**: two real `Transport` instances
  spinning a real gossip ring, measuring time-to-convergence under
  fault injection.
- **Alignment contract test** (`alignment_test.go`): pins the
  `Protocol.seqCounter` offset at zero via `unsafe.Offsetof`.

Chapter 6 reports the audit-driven additions: zone-ID
canonicalisation, alignment contract, and a fan-out of fuzz inputs
against the validator.
