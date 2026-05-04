package gossip

import (
	"log/slog"
	"math/rand"
	"net/netip"
	"sync"
	"time"
)

// canonicalSourceAddr normalises an "ip:port" string so that two logically
// equivalent representations of the same UDP peer compare equal in the
// pinned-source check.
//
// Two real-world drift sources motivated this:
//
//  1. IPv6 zone IDs. A link-local address picks up a "%zone" suffix from
//     the receiving interface (`[fe80::1%eth0]:9001` vs `[fe80::1%lo0]:9001`).
//     The same peer reaching us via two interfaces — or after a NIC swap —
//     would otherwise look like an impersonator.
//
//  2. IPv4-mapped IPv6. A dual-stack listener may report the same peer as
//     `192.0.2.1:9001` on one socket and `[::ffff:192.0.2.1]:9001` on
//     another. They are the same address.
//
// netip.ParseAddrPort + Addr.Unmap + Addr.WithZone("") is the documented
// way to fold both shapes into a canonical form. If parsing fails (e.g.
// hostname:port or malformed input), we return the original string so
// the original byte-for-byte comparison still works as a fallback.
func canonicalSourceAddr(s string) string {
	ap, err := netip.ParseAddrPort(s)
	if err != nil {
		return s
	}
	addr := ap.Addr().Unmap().WithZone("")
	return netip.AddrPortFrom(addr, ap.Port()).String()
}

// nodeEntry is the private membership-table record. Pointers to it never
// leave this file; callers receive copies of NodeInfo so they cannot
// mutate the table by accident.
//
// suspectConfirmers is the Lifeguard "Dogpile" vote set: each unique
// confirmer ID counts as one independent suspicion. Cleared automatically
// when the node is observed alive again or transitions to dead.
//
// srcAddr is the UDP source address (IP:port form) we have on file for
// this node. It is the audit's C-2 anti-spoof anchor: any incoming
// gossip message claiming to be from this nodeID must arrive from
// srcAddr, otherwise the message is forged. srcAddr is empty for
// entries created indirectly (e.g. via piggyback Members from a third
// party), and gets pinned the first time we receive a direct message
// from this node — see VerifyOrPinSource for the trust-on-first-use
// semantics and edge cases.
type nodeEntry struct {
	info              NodeInfo
	suspectTimer      *time.Timer
	suspectCancel     func()
	suspectConfirmers map[string]bool
	srcAddr           string
}

// MembershipList is the thread-safe SWIM membership table.
// All reads and writes are guarded by an RWMutex. Callers only ever
// receive snapshots (copies); they cannot corrupt internal state.
type MembershipList struct {
	mu      sync.RWMutex
	members map[string]*nodeEntry
	localID string
	logger  *slog.Logger

	// metrics is non-nil; defaults to a no-op so emitObserved can fire
	// without a branch in the hot path. State-change observations are
	// always made AFTER ml.mu is released to avoid deadlocking against
	// the metrics hook (which is itself lock-free, but defensive).
	metrics MetricsHook
}

// NewMembershipList creates a new MembershipList.
func NewMembershipList(localID string, logger *slog.Logger) *MembershipList {
	return &MembershipList{
		members: make(map[string]*nodeEntry),
		localID: localID,
		logger:  logger,
		metrics: noopMetricsHook{},
	}
}

// SetMetricsHook installs a metrics sink. Pass nil to restore the no-op.
// State transitions detected after this call are observed via the hook.
func (ml *MembershipList) SetMetricsHook(h MetricsHook) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if h == nil {
		ml.metrics = noopMetricsHook{}
		return
	}
	ml.metrics = h
}

// SourceVerification is the outcome of VerifyOrPinSource.
type SourceVerification int

const (
	// SourceUnknownTOFU: no recorded source for this nodeID. Caller
	// should let the message through; the entry will be created via
	// piggyback processing, and a future call will pin srcAddr.
	SourceUnknownTOFU SourceVerification = iota

	// SourcePinnedNow: an entry existed but had no recorded source.
	// We pinned srcAddr and accept this message. Future messages
	// must match.
	SourcePinnedNow

	// SourceMatch: srcAddr matches the recorded source. Accept.
	SourceMatch

	// SourceMismatch: srcAddr differs from the recorded source.
	// Drop the message — it's almost certainly forged.
	SourceMismatch
)

// VerifyOrPinSource is the C-2 anti-spoof check.
//
// An attacker on the gossip network can craft messages with arbitrary
// SenderID values to inflate Dogpile's independent-vote count and
// force premature suspect→dead transitions on victim nodes. The
// defense is to verify that messages claiming to be from a nodeID
// actually arrive from the UDP source we have recorded for that node.
//
// Semantics:
//   - First message from an unknown nodeID: SourceUnknownTOFU. Caller
//     accepts — piggyback processing creates the entry; the next
//     message will pin srcAddr.
//   - Existing entry with empty srcAddr (e.g. piggyback-created):
//     SourcePinnedNow. We record srcAddr; future calls must match.
//   - Existing entry with matching srcAddr: SourceMatch. Accept.
//   - Existing entry with different srcAddr: SourceMismatch. Reject.
//
// Trade-off: TOFU lets an attacker register N fake IDs, one per
// fake-ID-to-srcAddr binding. ConfirmSuspect's per-confirmerID dedup
// caps repeated abuse from a single bound pair. The long-term defense
// is gossip encryption/authentication via shared key; this method is
// the cheap, deps-free line of defense.
func (ml *MembershipList) VerifyOrPinSource(nodeID, srcAddr string) SourceVerification {
	if nodeID == "" || srcAddr == "" {
		return SourceUnknownTOFU // caller decides; missing data isn't itself spoofing
	}
	// Canonicalise on every entry into the comparator. Stored values are
	// already canonical (we only ever assign through canonicalSourceAddr
	// below), so normalising the input is enough to make the equality
	// check robust to zone-ID drift and IPv4-mapped IPv6 dual-stack noise.
	srcAddr = canonicalSourceAddr(srcAddr)
	ml.mu.Lock()
	defer ml.mu.Unlock()
	entry, ok := ml.members[nodeID]
	if !ok {
		return SourceUnknownTOFU
	}
	if entry.srcAddr == "" {
		entry.srcAddr = srcAddr
		return SourcePinnedNow
	}
	if entry.srcAddr == srcAddr {
		return SourceMatch
	}
	return SourceMismatch
}

// PinSourceIfAbsent records srcAddr for nodeID if no source was
// recorded yet. Used by the post-piggyback pinning step in
// processMessage so that the FIRST message from a previously-unknown
// node still ends up with a pinned srcAddr after the entry is created.
//
// Returns true if a pin happened (entry now has srcAddr); false if
// the entry already had a source or the nodeID is unknown.
func (ml *MembershipList) PinSourceIfAbsent(nodeID, srcAddr string) bool {
	if nodeID == "" || srcAddr == "" {
		return false
	}
	// Canonicalise before storing so future VerifyOrPinSource lookups
	// with a differently-shaped (but logically identical) address still
	// match. Mirrors the normalisation step in VerifyOrPinSource.
	srcAddr = canonicalSourceAddr(srcAddr)
	ml.mu.Lock()
	defer ml.mu.Unlock()
	entry, ok := ml.members[nodeID]
	if !ok || entry.srcAddr != "" {
		return false
	}
	entry.srcAddr = srcAddr
	return true
}

// AddOrUpdate adds or updates a node entry. SWIM incarnation rules:
//   - new incarnation > existing → always accept
//   - same incarnation → worse state wins (Dead > Suspect > Alive)
//   - new incarnation < existing → stale, ignore
//
// Returns true if the entry was accepted, false if ignored.
func (ml *MembershipList) AddOrUpdate(info NodeInfo) bool {
	// We do all critical-section work under the lock, capture what the
	// metrics hook needs, then notify outside the lock. Avoids any chance
	// of deadlock if a future hook implementation calls back into the
	// membership list.
	var (
		accepted  bool
		emitTrans bool
		fromState string // "" if this is a join, else previous state name
		toState   string
	)

	ml.mu.Lock()
	hook := ml.metrics
	existing, exists := ml.members[info.ID]
	switch {
	case !exists:
		ml.members[info.ID] = &nodeEntry{info: info}
		ml.logger.Info("node joined", "id", info.ID, "addr", info.Addr)
		accepted = true
		emitTrans = true
		fromState = "" // empty signals "new arrival" downstream
		toState = info.State.String()

	case info.Incarnation < existing.info.Incarnation:
		// Stale — ignore.

	case info.Incarnation == existing.info.Incarnation && info.State <= existing.info.State:
		// Same incarnation, state equal or better — ignore.

	default:
		// Alive transition: cancel any pending suspect timer and clear
		// Dogpile votes so a future suspicion starts from scratch.
		if info.State == StateAlive && existing.suspectCancel != nil {
			existing.suspectCancel()
			existing.suspectTimer = nil
			existing.suspectCancel = nil
			// Lifeguard: revival cancels prior suspect votes — a fresh
			// suspicion in the future must accumulate confirmations from
			// scratch.
			existing.suspectConfirmers = nil
		}
		prev := existing.info.State
		existing.info = info
		ml.logger.Info("node updated",
			"id", info.ID,
			"state", info.State.String(),
			"incarnation", info.Incarnation,
		)
		accepted = true
		if prev != info.State {
			emitTrans = true
			fromState = prev.String()
			toState = info.State.String()
		}
	}
	ml.mu.Unlock()

	if emitTrans {
		hook.OnStateChange(info.ID, fromState, toState)
	}
	return accepted
}

// MarkSuspect flips a node to Suspect and arms a timer that will
// transition it to Dead after suspectTimeout elapses.
// deadCallback fires once the timer expires (typically used to
// disseminate the death via gossip). It runs OUTSIDE the lock to
// avoid deadlocks.
func (ml *MembershipList) MarkSuspect(
	nodeID string,
	suspectTimeout time.Duration,
	deadCallback func(nodeID string),
) {
	ml.mu.Lock()
	hook := ml.metrics
	entry, exists := ml.members[nodeID]
	if !exists || entry.info.State >= StateSuspect {
		ml.mu.Unlock()
		return
	}

	prev := entry.info.State
	entry.info.State = StateSuspect
	ml.logger.Warn("node suspect", "id", nodeID)

	// time.AfterFunc starts a timer without opening a goroutine — no memory leak risk.
	timer := time.AfterFunc(suspectTimeout, func() {
		ml.markDead(nodeID, deadCallback)
	})
	entry.suspectTimer = timer
	entry.suspectCancel = func() { timer.Stop() }
	ml.mu.Unlock()

	hook.OnStateChange(nodeID, prev.String(), StateSuspect.String())
}

// markDead is invoked by the suspect timer callback. It re-acquires
// the lock and runs the user-supplied callback OUTSIDE the lock to
// avoid deadlocks.
func (ml *MembershipList) markDead(nodeID string, callback func(string)) {
	ml.mu.Lock()
	hook := ml.metrics
	entry, exists := ml.members[nodeID]
	if !exists || entry.info.State == StateDead {
		ml.mu.Unlock()
		return
	}
	prev := entry.info.State
	entry.info.State = StateDead
	entry.suspectTimer = nil
	entry.suspectCancel = nil
	entry.suspectConfirmers = nil // Dogpile votes irrelevant once dead
	ml.logger.Error("node dead", "id", nodeID)
	ml.mu.Unlock()

	// Both notifications fire outside the lock to keep slow callbacks
	// from blocking the membership table.
	hook.OnStateChange(nodeID, prev.String(), StateDead.String())
	if callback != nil {
		callback(nodeID)
	}
}

// ConfirmSuspect records a Lifeguard "Dogpile" vote and reschedules the
// suspect timer using the supplied computeTimeout function.
//
//   - If the node is unknown or already Dead, this is a no-op (returns 0,false).
//   - If the node is currently Alive, it transitions to Suspect.
//   - confirmerID dedupes votes — a confirmer ID can vote at most once.
//   - computeTimeout receives the current vote count and returns the timeout
//     to apply; this lets the caller plug in the formula without leaking
//     timing constants into the membership package.
//
// Returns the post-vote count and true on success.
func (ml *MembershipList) ConfirmSuspect(
	nodeID, confirmerID string,
	computeTimeout func(votes int) time.Duration,
	deadCallback func(string),
) (int, bool) {
	ml.mu.Lock()
	hook := ml.metrics

	entry, exists := ml.members[nodeID]
	if !exists || entry.info.State == StateDead {
		ml.mu.Unlock()
		return 0, false
	}

	if entry.suspectConfirmers == nil {
		entry.suspectConfirmers = make(map[string]bool)
	}
	entry.suspectConfirmers[confirmerID] = true
	votes := len(entry.suspectConfirmers)

	// Transition to Suspect on first vote.
	transitioned := false
	prevState := entry.info.State
	if entry.info.State == StateAlive {
		entry.info.State = StateSuspect
		transitioned = true
		ml.logger.Warn("node suspect (lifeguard)", "id", nodeID, "votes", votes)
	}

	timeout := computeTimeout(votes)

	// Reschedule timer: cancel any existing one first.
	if entry.suspectCancel != nil {
		entry.suspectCancel()
	}
	timer := time.AfterFunc(timeout, func() {
		ml.markDead(nodeID, deadCallback)
	})
	entry.suspectTimer = timer
	entry.suspectCancel = func() { timer.Stop() }

	ml.mu.Unlock()

	if transitioned {
		hook.OnStateChange(nodeID, prevState.String(), StateSuspect.String())
	}
	return votes, true
}

// GetAliveMembers returns ALIVE nodes for the load balancer to route to.
// Filters: StateAlive + ServiceAddr != "" (the LB itself, which has an
// empty ServiceAddr, is excluded). Returns a fresh copy on every call
// so the caller cannot mutate internal state.
func (ml *MembershipList) GetAliveMembers() []NodeInfo {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	result := make([]NodeInfo, 0, len(ml.members))
	for _, entry := range ml.members {
		if entry.info.State == StateAlive &&
			entry.info.ID != ml.localID &&
			entry.info.ServiceAddr != "" {
			result = append(result, entry.info)
		}
	}
	return result
}

// GetAllMembers returns every member for piggyback dissemination.
// Dead nodes are included — they are needed for convergence (so other
// nodes still hear "X is dead" via piggyback long enough to apply it).
func (ml *MembershipList) GetAllMembers() []NodeInfo {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	result := make([]NodeInfo, 0, len(ml.members))
	for _, entry := range ml.members {
		result = append(result, entry.info)
	}
	return result
}

// GetRandomMembers returns up to count random members, excluding
// the local node, anything in excludeIDs, and any Dead members.
// Uses Fisher-Yates shuffle for a uniform distribution.
func (ml *MembershipList) GetRandomMembers(count int, excludeIDs ...string) []NodeInfo {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	exclude := make(map[string]bool, len(excludeIDs)+1)
	exclude[ml.localID] = true
	for _, id := range excludeIDs {
		exclude[id] = true
	}

	candidates := make([]NodeInfo, 0, len(ml.members))
	for _, entry := range ml.members {
		if !exclude[entry.info.ID] && entry.info.State != StateDead {
			candidates = append(candidates, entry.info)
		}
	}

	// Fisher-Yates shuffle
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	if count > len(candidates) {
		count = len(candidates)
	}
	return candidates[:count]
}

// Defend bumps the local node's incarnation and pulls its state back to
// Alive. Called whenever another node has declared us suspect or dead;
// the bumped incarnation overrides the bad gossip on subsequent rounds.
func (ml *MembershipList) Defend(localInfo *NodeInfo) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	entry, exists := ml.members[localInfo.ID]
	if !exists {
		return
	}
	entry.info.Incarnation++
	entry.info.State = StateAlive
	localInfo.Incarnation = entry.info.Incarnation
	ml.logger.Info("node defended self",
		"id", localInfo.ID,
		"incarnation", entry.info.Incarnation,
	)
}

// GetMember returns the NodeInfo for a specific nodeID.
// Returns false if no such node exists.
func (ml *MembershipList) GetMember(nodeID string) (NodeInfo, bool) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	entry, exists := ml.members[nodeID]
	if !exists {
		return NodeInfo{}, false
	}
	return entry.info, true
}
