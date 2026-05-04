package gossip

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"p2p-loadbalancer/pkg/types"
)

// Protocol drives the SWIM protocol loop. Two goroutines coordinate via
// pendingPings (sync.Map): gossipLoop runs ping/ack rounds and
// handleMessages dispatches incoming messages.
//
// When config.LifeguardEnabled is true, Self-Awareness adjusts probe
// timeouts dynamically and Dogpile shortens the suspect window via
// ConfirmSuspect on MembershipList. The selfAwareness field stays nil if
// the feature is off, so the hot path has zero overhead.
//
// FIELD ORDER IS LOAD-BEARING. seqCounter is read/written via
// atomic.AddUint64. On 32-bit ARM/x86 the standard library only
// guarantees 8-byte alignment for the FIRST field of a heap-allocated
// struct; a uint64 anywhere else can land at a 4-aligned offset and
// panic on the atomic op. Keeping seqCounter at offset 0 makes the
// alignment requirement satisfied on every supported GOARCH.
//
// Pinned by TestProtocol_SeqCounterAtOffsetZero (alignment test).
// Do NOT reorder these fields without also updating that test.
type Protocol struct {
	seqCounter   uint64 // atomic monotonic sequence number (must remain at offset 0)
	config       types.ProtocolConfig
	membership   *MembershipList
	transport    *Transport
	localInfo    NodeInfo
	pendingPings sync.Map // map[uint64]chan bool — in-flight PINGs
	logger       *slog.Logger

	// selfAwareness is non-nil only when config.LifeguardEnabled.
	selfAwareness *SelfAwareness

	// metrics is never nil — defaults to a no-op hook so the hot path
	// can call through without branching. Set via SetMetricsHook.
	metrics MetricsHook
}

// NewProtocol creates a new SWIM protocol instance.
func NewProtocol(
	cfg types.ProtocolConfig,
	ml *MembershipList,
	transport *Transport,
	localInfo NodeInfo,
	logger *slog.Logger,
) *Protocol {
	p := &Protocol{
		config:     cfg,
		membership: ml,
		transport:  transport,
		localInfo:  localInfo,
		logger:     logger,
		metrics:    noopMetricsHook{},
	}
	if cfg.LifeguardEnabled {
		p.selfAwareness = NewSelfAwareness(cfg.SelfAwarenessMax)
	}
	return p
}

// SetMetricsHook installs the metrics sink. Call once at startup before
// Start() — the hook is read concurrently from the gossip goroutines.
// Passing nil silently restores the no-op hook.
func (p *Protocol) SetMetricsHook(h MetricsHook) {
	if h == nil {
		p.metrics = noopMetricsHook{}
		return
	}
	p.metrics = h
}

// pingTimeout returns the direct ping timeout, scaled by Self-Awareness if
// Lifeguard is enabled. Falls back to the static PingTimeout otherwise so
// non-Lifeguard deployments behave exactly as before.
func (p *Protocol) pingTimeout() time.Duration {
	if p.selfAwareness != nil {
		return p.selfAwareness.ScaleTimeout(p.config.PingTimeoutMin, p.config.PingTimeoutMax)
	}
	return p.config.PingTimeout
}

// indirectTimeout returns the indirect ping timeout, scaled if enabled.
func (p *Protocol) indirectTimeout() time.Duration {
	if p.selfAwareness != nil {
		return p.selfAwareness.ScaleTimeout(p.config.IndirectTimeoutMin, p.config.IndirectTimeoutMax)
	}
	return p.config.IndirectTimeout
}

// computeSuspectTimeout returns the Dogpile-formula timeout for the given
// number of independent confirmations. Used as the closure argument to
// MembershipList.ConfirmSuspect.
func (p *Protocol) computeSuspectTimeout(votes int) time.Duration {
	return dogpileTimeout(
		p.config.SuspectTimeoutMin,
		p.config.SuspectTimeoutMax,
		votes,
		p.config.IndirectNodes,
	)
}

// Start launches the two protocol goroutines — non-blocking.
// Both goroutines exit cleanly when ctx is canceled.
func (p *Protocol) Start(ctx context.Context) {
	go p.gossipLoop(ctx)
	go p.handleMessages(ctx)
}

// gossipLoop is the main loop, ticking once per ProtocolInterval.
// Each tick runs one gossip round.
func (p *Protocol) gossipLoop(ctx context.Context) {
	ticker := time.NewTicker(p.config.ProtocolInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Refresh the Lifeguard score gauge once per round. Cheap —
			// a single mutex Lock/Unlock plus a metric Set.
			if p.selfAwareness != nil {
				p.metrics.OnLifeguardScore(p.selfAwareness.Score())
			}
			p.runProtocolRound(ctx)
		}
	}
}

// runProtocolRound executes one SWIM round.
// Direct ping → timeout → indirect ping → timeout → suspect.
func (p *Protocol) runProtocolRound(ctx context.Context) {
	targets := p.membership.GetRandomMembers(1)
	if len(targets) == 0 {
		return // no other members to probe
	}
	target := targets[0]
	seqNum := p.nextSeq()

	// ACK channel: buffer=1; we expect at most one ACK.
	replyCh := make(chan bool, 1)
	p.pendingPings.Store(seqNum, replyCh)

	pingMsg := GossipMessage{
		Type:     MsgPing,
		SenderID: p.localInfo.ID,
		TargetID: target.ID,
		SeqNum:   seqNum,
		Members:  p.membership.GetAllMembers(), // piggyback
	}

	if err := p.transport.Send(target.Addr, pingMsg); err != nil {
		p.logger.Error("ping send failed", "target", target.ID, "err", err)
		p.pendingPings.Delete(seqNum)
		return
	}
	p.metrics.OnMessageSent(string(MsgPing))

	// Step 1: wait for direct ACK with the ping timeout.
	// Self-Awareness scaling: under stress this can grow up to PingTimeoutMax.
	directTimer := time.NewTimer(p.pingTimeout())
	defer directTimer.Stop()

	select {
	case <-ctx.Done():
		p.pendingPings.Delete(seqNum)
		return
	case <-replyCh:
		p.pendingPings.Delete(seqNum)
		// Lifeguard: successful direct probe → we're healthy, lower noop.
		if p.selfAwareness != nil {
			p.selfAwareness.RecordSuccess()
		}
		p.logger.Debug("direct ACK received", "target", target.ID)
		return
	case <-directTimer.C:
		// Direct ping timeout — fall through to indirect.
		// Lifeguard: missed direct ACK is the signal that drives noop up.
		if p.selfAwareness != nil {
			p.selfAwareness.RecordFailure()
		}
	}
	p.pendingPings.Delete(seqNum)

	// Step 2: send PING_REQ to K intermediaries.
	intermediaries := p.membership.GetRandomMembers(p.config.IndirectNodes, target.ID)
	if len(intermediaries) == 0 {
		p.suspectNode(target.ID)
		return
	}

	indirectSeq := p.nextSeq()
	// buffer = K: each intermediary may reply once.
	indirectCh := make(chan bool, len(intermediaries))
	p.pendingPings.Store(indirectSeq, indirectCh)

	pingReqMsg := GossipMessage{
		Type:     MsgPingReq,
		SenderID: p.localInfo.ID,
		TargetID: target.ID,
		SeqNum:   indirectSeq,
	}
	for _, inter := range intermediaries {
		if err := p.transport.Send(inter.Addr, pingReqMsg); err != nil {
			p.logger.Error("ping-req send failed", "intermediary", inter.ID, "err", err)
			continue
		}
		p.metrics.OnMessageSent(string(MsgPingReq))
	}

	// Step 3: wait for indirect ACK with the indirect timeout.
	indirectTimer := time.NewTimer(p.indirectTimeout())
	defer indirectTimer.Stop()

	select {
	case <-ctx.Done():
		p.pendingPings.Delete(indirectSeq)
		return
	case <-indirectCh:
		p.pendingPings.Delete(indirectSeq)
		p.logger.Debug("indirect ACK received", "target", target.ID)
		return
	case <-indirectTimer.C:
		// Indirect ping also timed out — mark suspect.
	}
	p.pendingPings.Delete(indirectSeq)

	p.suspectNode(target.ID)
}

// suspectNode declares a node suspect and disseminates the news.
//
// Lifeguard path (LifeguardEnabled=true):
//   - Records the local node as the first independent confirmer via
//     ConfirmSuspect. Initial timeout follows the Dogpile formula.
//   - Buddy System: sends a SUSPECT message directly to the suspected node
//     so that, if alive, it can defend itself without waiting for gossip
//     to propagate the news.
//
// Vanilla path: identical to the historical SWIM behaviour.
func (p *Protocol) suspectNode(nodeID string) {
	p.logger.Warn("marking node suspect", "id", nodeID)

	if p.config.LifeguardEnabled {
		p.membership.ConfirmSuspect(nodeID, p.localInfo.ID, p.computeSuspectTimeout, p.gossipDead)
	} else {
		p.membership.MarkSuspect(
			nodeID,
			p.config.SuspectTimeout,
			p.gossipDead, // invoked when the timer expires
		)
	}

	suspectMsg := GossipMessage{
		Type:     MsgSuspect,
		SenderID: p.localInfo.ID,
		TargetID: nodeID,
		SeqNum:   p.nextSeq(),
		Members:  p.membership.GetAllMembers(),
	}

	// Buddy System: direct notification to the suspected node.
	// If the node is in fact alive, this lets it defend before gossip arrives.
	// On a real outage this packet is simply dropped — no harm done.
	if p.config.LifeguardEnabled {
		if target, ok := p.membership.GetMember(nodeID); ok && target.Addr != "" {
			if err := p.transport.Send(target.Addr, suspectMsg); err != nil {
				p.logger.Debug("buddy notification send failed", "target", nodeID, "err", err)
			}
		}
	}

	p.broadcastToK(suspectMsg)
}

// gossipDead disseminates a death notice to K random neighbours.
// Invoked from MembershipList's suspect-timer callback.
func (p *Protocol) gossipDead(nodeID string) {
	deadMsg := GossipMessage{
		Type:     MsgDead,
		SenderID: p.localInfo.ID,
		TargetID: nodeID,
		SeqNum:   p.nextSeq(),
		Members:  p.membership.GetAllMembers(),
	}
	p.broadcastToK(deadMsg)
}

// broadcastToK sends msg to K random members (gossip fan-out).
func (p *Protocol) broadcastToK(msg GossipMessage) {
	targets := p.membership.GetRandomMembers(p.config.IndirectNodes)
	for _, t := range targets {
		if err := p.transport.Send(t.Addr, msg); err != nil {
			p.logger.Error("broadcast failed", "target", t.ID, "err", err)
			continue
		}
		p.metrics.OnMessageSent(string(msg.Type))
	}
}

// handleMessages consumes incoming messages from the transport — runs
// in its own goroutine.
func (p *Protocol) handleMessages(ctx context.Context) {
	recvCh := p.transport.Receive()

	for {
		select {
		case <-ctx.Done():
			return
		case received, ok := <-recvCh:
			if !ok {
				return // channel closed
			}
			p.processMessage(ctx, received)
		}
	}
}

// processMessage dispatches by message type.
// Piggyback membership data is processed first on every message.
//
// ctx is propagated to handlers that spawn long-lived goroutines (notably
// handlePingReq). Without it, those goroutines would survive a protocol
// shutdown for up to IndirectTimeout — the audit's H-2 finding.
//
// SenderID is validated against the UDP source address (audit's C-2):
// if we already have a pinned source for the SenderID and it differs,
// the message is forged and dropped. The first message from a new node
// goes through under TOFU; the post-piggyback pin step ensures the
// next message from that same node is bound to its actual src address.
func (p *Protocol) processMessage(ctx context.Context, received ReceivedMessage) {
	msg := received.Msg
	p.metrics.OnMessageReceived(string(msg.Type))

	// C-2 anti-spoof: verify SenderID source BEFORE processing piggyback.
	// Otherwise a forged message could poison the membership table via
	// piggyback Members even if we'd later drop the message.
	//
	// We skip validation when:
	//   - SenderID is empty or matches our own ID (defensive — neither
	//     condition normally arises, but both are safe to ignore).
	//   - The membership has no entry yet for SenderID (TOFU bootstrap).
	//     The PinSourceIfAbsent call after piggyback locks the source
	//     so subsequent messages will be verified.
	srcAddr := ""
	if received.From != nil {
		srcAddr = received.From.String()
	}
	if msg.SenderID != "" && msg.SenderID != p.localInfo.ID && srcAddr != "" {
		switch p.membership.VerifyOrPinSource(msg.SenderID, srcAddr) {
		case SourceMismatch:
			p.logger.Warn("rejected gossip message: source address mismatch",
				"claimed_sender", msg.SenderID,
				"actual_source", srcAddr,
				"type", string(msg.Type))
			p.metrics.OnMessageRejected("source_mismatch")
			return
		case SourceUnknownTOFU, SourcePinnedNow, SourceMatch:
			// Allow through. SourceUnknownTOFU will be pinned after
			// piggyback creates the entry, below.
		}
	}

	// Process piggyback membership entries first so the rest of the
	// dispatch sees the freshest view.
	for _, memberInfo := range msg.Members {
		if memberInfo.ID == p.localInfo.ID {
			// Bad gossip about ourselves → bump incarnation to defend.
			if memberInfo.State > StateAlive {
				p.membership.Defend(&p.localInfo)
				p.announceAlive()
			}
			continue
		}
		p.membership.AddOrUpdate(memberInfo)
	}

	// C-2 step 2: pin SenderID's source if piggyback just created the
	// entry. This closes the TOFU window after the first message —
	// subsequent messages from the same nodeID must come from this
	// same UDP source.
	if msg.SenderID != "" && msg.SenderID != p.localInfo.ID && srcAddr != "" {
		p.membership.PinSourceIfAbsent(msg.SenderID, srcAddr)
	}

	switch msg.Type {
	case MsgPing:
		p.handlePing(received.From, msg)
	case MsgAck:
		p.handleAck(msg)
	case MsgPingReq:
		p.handlePingReq(ctx, msg)
	case MsgDead:
		p.handleDeadMsg(msg)
	case MsgSuspect:
		p.handleSuspectMsg(msg)
	case MsgAlive:
		// piggyback already processed
	}
}

// handlePing replies to a PING with an ACK, piggybacking our membership view.
func (p *Protocol) handlePing(from *net.UDPAddr, msg GossipMessage) {
	ack := GossipMessage{
		Type:     MsgAck,
		SenderID: p.localInfo.ID,
		TargetID: msg.SenderID,
		SeqNum:   msg.SeqNum,
		Members:  p.membership.GetAllMembers(),
	}
	if err := p.transport.Send(from.String(), ack); err != nil {
		p.logger.Error("ACK send failed", "err", err)
		return
	}
	p.metrics.OnMessageSent(string(MsgAck))
}

// handleAck signals the matching pending PING via its reply channel.
func (p *Protocol) handleAck(msg GossipMessage) {
	if ch, loaded := p.pendingPings.LoadAndDelete(msg.SeqNum); loaded {
		replyCh := ch.(chan bool)
		// Non-blocking send — the request may have already timed out.
		select {
		case replyCh <- true:
		default:
		}
	}
}

// handlePingReq runs as an intermediary: pings the target on behalf of the
// requester and forwards the result. Spawns its own goroutine because the
// blocking ping must not stall the message-receive loop.
//
// The spawned goroutine waits up to IndirectTimeout (or its scaled
// Lifeguard equivalent — up to 8s by default) for the target's ACK.
// Without ctx-awareness, a protocol shutdown during that window would
// leave the goroutine running until its timer fires, holding a
// pendingPings entry, a UDP socket reference, and a goroutine slot.
// On a busy cluster being shut down hundreds of these can be in flight.
//
// We honor ctx by adding ctx.Done() to the select. Cancellation drains
// the goroutine within microseconds of the ctx flip, regardless of
// IndirectTimeout configuration.
func (p *Protocol) handlePingReq(ctx context.Context, msg GossipMessage) {
	go func() {
		// Look up the target's address.
		target, found := p.membership.GetMember(msg.TargetID)
		if !found {
			return
		}

		// Look up the requester's address.
		requester, found := p.membership.GetMember(msg.SenderID)
		if !found {
			return
		}

		// Send a probe to the target on behalf of the requester.
		indirectSeq := p.nextSeq()
		replyCh := make(chan bool, 1)
		p.pendingPings.Store(indirectSeq, replyCh)
		defer p.pendingPings.Delete(indirectSeq)

		pingMsg := GossipMessage{
			Type:     MsgPing,
			SenderID: p.localInfo.ID,
			TargetID: msg.TargetID,
			SeqNum:   indirectSeq,
		}
		if err := p.transport.Send(target.Addr, pingMsg); err != nil {
			return
		}

		// Wait for IndirectTimeout, or bail out early on ctx cancel.
		timer := time.NewTimer(p.config.IndirectTimeout)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			// Protocol shutdown — bail out instantly. Skipping the ACK
			// forward is correct: the requester is being torn down too.
			return
		case <-replyCh:
			// Target replied — forward an ACK to the requester.
			ackMsg := GossipMessage{
				Type:     MsgAck,
				SenderID: p.localInfo.ID,
				TargetID: msg.SenderID,
				SeqNum:   msg.SeqNum, // original seq num
			}
			p.transport.Send(requester.Addr, ackMsg)
		case <-timer.C:
			// Target didn't reply — stay silent; the requester runs
			// its own timeout.
		}
	}()
}

// handleDeadMsg applies an incoming DEAD message to the membership table.
func (p *Protocol) handleDeadMsg(msg GossipMessage) {
	if msg.TargetID == p.localInfo.ID {
		// They declared us dead — defend ourselves.
		p.membership.Defend(&p.localInfo)
		p.announceAlive()
		return
	}
	// Apply the DEAD state for TargetID. Piggyback handling above already
	// ran, but the explicit DEAD message gets its own application path
	// to ensure the transition lands even when the piggyback was stale.
	deadInfo := NodeInfo{
		ID:    msg.TargetID,
		State: StateDead,
	}
	if existing, found := p.membership.GetMember(msg.TargetID); found {
		// Existing record — preserve its incarnation.
		deadInfo.Addr = existing.Addr
		deadInfo.ServiceAddr = existing.ServiceAddr
		deadInfo.Incarnation = existing.Incarnation
	} else {
		// Unknown node — pull addr/incarnation from the piggyback Members.
		for _, m := range msg.Members {
			if m.ID == msg.TargetID {
				deadInfo.Addr = m.Addr
				deadInfo.ServiceAddr = m.ServiceAddr
				deadInfo.Incarnation = m.Incarnation
				break
			}
		}
		if deadInfo.Addr == "" {
			// No address info anywhere — log a warning and bail.
			p.logger.Warn("dead msg for unknown node without addr info", "target", msg.TargetID)
			return
		}
	}
	p.membership.AddOrUpdate(deadInfo)
}

// handleSuspectMsg applies an incoming SUSPECT message to the membership table.
//
// Lifeguard: each unique sender counts as an independent Dogpile vote, so
// the suspect timer shrinks logarithmically as more nodes corroborate.
// Vanilla: behaves like the original — fixed SuspectTimeout regardless of
// how many SUSPECT messages we receive.
func (p *Protocol) handleSuspectMsg(msg GossipMessage) {
	if msg.TargetID == p.localInfo.ID {
		// They declared us suspect — defend ourselves.
		p.membership.Defend(&p.localInfo)
		p.announceAlive()
		return
	}
	if _, found := p.membership.GetMember(msg.TargetID); !found {
		return
	}
	if p.config.LifeguardEnabled {
		votes, ok := p.membership.ConfirmSuspect(msg.TargetID, msg.SenderID, p.computeSuspectTimeout, p.gossipDead)
		if ok {
			p.logger.Debug("dogpile vote recorded",
				"target", msg.TargetID,
				"confirmer", msg.SenderID,
				"votes", votes,
			)
		}
		return
	}
	p.membership.MarkSuspect(msg.TargetID, p.config.SuspectTimeout, p.gossipDead)
}

// announceAlive broadcasts our own ALIVE status to the gossip ring.
func (p *Protocol) announceAlive() {
	aliveMsg := GossipMessage{
		Type:     MsgAlive,
		SenderID: p.localInfo.ID,
		SeqNum:   p.nextSeq(),
		Members:  []NodeInfo{p.localInfo},
	}
	p.broadcastToK(aliveMsg)
}

// nextSeq generates a thread-safe monotonic sequence number.
func (p *Protocol) nextSeq() uint64 {
	return atomic.AddUint64(&p.seqCounter, 1)
}
