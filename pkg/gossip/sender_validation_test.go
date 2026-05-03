package gossip

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// =====================================================================
// MembershipList.VerifyOrPinSource — unit-level state machine tests
// =====================================================================

func TestVerifyOrPinSource_UnknownNodeIsTOFU(t *testing.T) {
	// First contact with a previously-unknown nodeID returns
	// SourceUnknownTOFU. The caller is expected to let the message
	// through (piggyback will create the entry, then PinSourceIfAbsent
	// closes the TOFU window).
	ml := NewMembershipList("self", silentLogger())
	got := ml.VerifyOrPinSource("new-node", "10.0.0.1:9001")
	if got != SourceUnknownTOFU {
		t.Errorf("expected SourceUnknownTOFU for unknown node, got %v", got)
	}
}

func TestVerifyOrPinSource_PinsOnFirstSet(t *testing.T) {
	// Existing entry with empty srcAddr → SourcePinnedNow. The src
	// address is recorded so subsequent calls are verifiable.
	ml := NewMembershipList("self", silentLogger())
	ml.AddOrUpdate(NodeInfo{ID: "n", Addr: "x", State: StateAlive, Incarnation: 1})

	got := ml.VerifyOrPinSource("n", "192.0.2.1:9001")
	if got != SourcePinnedNow {
		t.Errorf("expected SourcePinnedNow on first src, got %v", got)
	}

	// A second call with the same source must now match.
	got = ml.VerifyOrPinSource("n", "192.0.2.1:9001")
	if got != SourceMatch {
		t.Errorf("expected SourceMatch on repeat, got %v", got)
	}
}

func TestVerifyOrPinSource_RejectsImpersonation(t *testing.T) {
	// Pin one source, then try a different one with the same nodeID.
	// This is the exact attack we're defending against: an attacker
	// crafts MsgSuspect{SenderID:"victim"} from their own address
	// after we already know victim's real source.
	ml := NewMembershipList("self", silentLogger())
	ml.AddOrUpdate(NodeInfo{ID: "victim", Addr: "x", State: StateAlive, Incarnation: 1})
	ml.VerifyOrPinSource("victim", "10.0.0.1:9001") // legitimate first contact

	got := ml.VerifyOrPinSource("victim", "10.0.0.99:54321") // attacker
	if got != SourceMismatch {
		t.Errorf("expected SourceMismatch from impersonator, got %v", got)
	}
}

func TestVerifyOrPinSource_EmptyArgsAreTOFU(t *testing.T) {
	// Empty nodeID or empty srcAddr returns TOFU — these are degenerate
	// cases that shouldn't happen in production, but we don't want them
	// to be treated as spoofing failures either.
	ml := NewMembershipList("self", silentLogger())
	if got := ml.VerifyOrPinSource("", "1.2.3.4:9001"); got != SourceUnknownTOFU {
		t.Errorf("empty nodeID: got %v", got)
	}
	if got := ml.VerifyOrPinSource("n", ""); got != SourceUnknownTOFU {
		t.Errorf("empty srcAddr: got %v", got)
	}
}

func TestPinSourceIfAbsent_OnlyPinsOnce(t *testing.T) {
	// Idempotent for the same nodeID: first call pins, subsequent
	// calls (with any srcAddr) return false without overwriting.
	ml := NewMembershipList("self", silentLogger())
	ml.AddOrUpdate(NodeInfo{ID: "n", Addr: "x", State: StateAlive, Incarnation: 1})

	if !ml.PinSourceIfAbsent("n", "10.0.0.1:9001") {
		t.Error("first PinSourceIfAbsent should have returned true")
	}
	if ml.PinSourceIfAbsent("n", "10.0.0.99:9001") {
		t.Error("second call must NOT overwrite — returned true")
	}
	// Verify the original pin survived by checking match.
	if ml.VerifyOrPinSource("n", "10.0.0.1:9001") != SourceMatch {
		t.Error("original pin was overwritten")
	}
}

func TestPinSourceIfAbsent_NoEntryReturnsFalse(t *testing.T) {
	ml := NewMembershipList("self", silentLogger())
	if ml.PinSourceIfAbsent("ghost", "10.0.0.1:9001") {
		t.Error("expected false for unknown nodeID")
	}
}

// =====================================================================
// Integration: forged messages are dropped at processMessage
// =====================================================================

// recordingMetricsHook captures rejection events so the test can verify
// the OnMessageRejected counter is invoked with the right reason.
type recordingMetricsHook struct {
	noopMetricsHook
	rejected atomic.Int32
	reason   atomic.Value // string
}

func (r *recordingMetricsHook) OnMessageRejected(reason string) {
	r.rejected.Add(1)
	r.reason.Store(reason)
}

// TestProcessMessage_RejectsForgedSenderID is the full attack scenario.
//
// Setup:
//   - Local protocol "lb" knows about "real-node" with pinned source addr X.
//   - Attacker sends a message claiming SenderID="real-node" from addr Y.
//
// Expectation:
//   - processMessage drops the message before piggyback or dispatch.
//   - OnMessageRejected("source_mismatch") fires.
//   - Membership table is unchanged — no Dogpile vote was inflated.
func TestProcessMessage_RejectsForgedSenderID(t *testing.T) {
	logger := silentLogger()
	ml := NewMembershipList("lb", logger)
	hook := &recordingMetricsHook{}
	ml.SetMetricsHook(hook)

	// Pin "real-node"'s legitimate source.
	realAddr := "10.0.0.5:9001"
	ml.AddOrUpdate(NodeInfo{ID: "real-node", Addr: "10.0.0.5:9001", State: StateAlive, Incarnation: 1})
	ml.VerifyOrPinSource("real-node", realAddr)

	// Build the protocol with our recording hook.
	tr, err := NewTransport(freeUDPAddr(t), 100, logger)
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	cfg := fastCfg("lb", "lb")
	proto := NewProtocol(cfg, ml, tr, NodeInfo{ID: "lb"}, logger)
	proto.SetMetricsHook(hook)

	ctx := context.Background()

	// Forge a MsgSuspect claiming to be from "real-node" but coming
	// from a different src address.
	attackerAddr, _ := net.ResolveUDPAddr("udp", "10.0.0.99:54321")
	forged := ReceivedMessage{
		From: attackerAddr,
		Msg: GossipMessage{
			Type:     MsgSuspect,
			SenderID: "real-node",
			TargetID: "victim",
			SeqNum:   1,
		},
	}

	proto.processMessage(ctx, forged)

	// The metric must have fired exactly once with the right reason.
	if got := hook.rejected.Load(); got != 1 {
		t.Errorf("expected 1 rejection, got %d", got)
	}
	if got := hook.reason.Load(); got != "source_mismatch" {
		t.Errorf("rejection reason: got %q want %q", got, "source_mismatch")
	}

	// Membership state for "real-node" must be unchanged (Alive).
	info, _ := ml.GetMember("real-node")
	if info.State != StateAlive {
		t.Errorf("real-node state changed: %v — forged message should not have effect", info.State)
	}
}

// TestProcessMessage_AllowsLegitimateMessage is the inverse: a real
// message from the genuine source must pass cleanly.
func TestProcessMessage_AllowsLegitimateMessage(t *testing.T) {
	logger := silentLogger()
	ml := NewMembershipList("lb", logger)
	hook := &recordingMetricsHook{}
	ml.SetMetricsHook(hook)

	realAddr := "10.0.0.5:9001"
	ml.AddOrUpdate(NodeInfo{ID: "real-node", Addr: realAddr, State: StateAlive, Incarnation: 1})
	ml.VerifyOrPinSource("real-node", realAddr) // pre-pin

	tr, err := NewTransport(freeUDPAddr(t), 100, logger)
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	cfg := fastCfg("lb", "lb")
	proto := NewProtocol(cfg, ml, tr, NodeInfo{ID: "lb"}, logger)
	proto.SetMetricsHook(hook)

	from, _ := net.ResolveUDPAddr("udp", realAddr)
	msg := ReceivedMessage{
		From: from,
		Msg: GossipMessage{
			Type:     MsgAlive,
			SenderID: "real-node",
			SeqNum:   1,
		},
	}

	proto.processMessage(context.Background(), msg)

	if got := hook.rejected.Load(); got != 0 {
		t.Errorf("legitimate message was rejected: count=%d", got)
	}
}

// TestProcessMessage_TOFUFirstMessagePinsSource verifies the
// trust-on-first-use behavior: the first message from a previously-
// unknown sender is allowed, and pinning happens AFTER piggyback.
// A subsequent message from a different source for the same SenderID
// must then be rejected.
func TestProcessMessage_TOFUFirstMessagePinsSource(t *testing.T) {
	logger := silentLogger()
	ml := NewMembershipList("lb", logger)
	hook := &recordingMetricsHook{}
	ml.SetMetricsHook(hook)

	tr, err := NewTransport(freeUDPAddr(t), 100, logger)
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	cfg := fastCfg("lb", "lb")
	proto := NewProtocol(cfg, ml, tr, NodeInfo{ID: "lb"}, logger)
	proto.SetMetricsHook(hook)

	// First message: an MsgAlive arrives from a fresh node, including
	// itself in the piggyback Members list. This is the seed-join flow.
	srcAddr1 := "10.0.0.5:9001"
	from1, _ := net.ResolveUDPAddr("udp", srcAddr1)
	join := ReceivedMessage{
		From: from1,
		Msg: GossipMessage{
			Type:     MsgAlive,
			SenderID: "newcomer",
			SeqNum:   1,
			Members: []NodeInfo{
				{ID: "newcomer", Addr: srcAddr1, State: StateAlive, Incarnation: 1},
			},
		},
	}
	proto.processMessage(context.Background(), join)

	// First message must NOT have been rejected (TOFU).
	if got := hook.rejected.Load(); got != 0 {
		t.Errorf("TOFU first message was rejected: %d", got)
	}

	// The post-piggyback pin should have recorded the source. Verify
	// by sending a follow-up from a DIFFERENT source: it must be
	// rejected.
	srcAddr2 := "10.0.0.99:54321" // attacker
	from2, _ := net.ResolveUDPAddr("udp", srcAddr2)
	followup := ReceivedMessage{
		From: from2,
		Msg: GossipMessage{
			Type:     MsgSuspect,
			SenderID: "newcomer",
			TargetID: "any",
			SeqNum:   2,
		},
	}
	proto.processMessage(context.Background(), followup)

	if got := hook.rejected.Load(); got != 1 {
		t.Errorf("post-TOFU mismatched-source message was NOT rejected; rejected=%d", got)
	}
}

// TestProcessMessage_OwnIDIsExempt verifies that messages claiming
// SenderID == localInfo.ID skip validation. This is the Defend-self
// flow: when our own incarnation gets bumped via a piggyback we
// receive, we don't want our own logic to drop the message just
// because we don't have a pinned srcAddr for ourselves.
func TestProcessMessage_OwnIDIsExempt(t *testing.T) {
	logger := silentLogger()
	ml := NewMembershipList("self", logger)
	hook := &recordingMetricsHook{}
	ml.SetMetricsHook(hook)

	tr, err := NewTransport(freeUDPAddr(t), 100, logger)
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	proto := NewProtocol(fastCfg("self", "self"), ml, tr, NodeInfo{ID: "self"}, logger)
	proto.SetMetricsHook(hook)

	// A message claiming to be from us, from any source — should be
	// passed through (not rejected) because SenderID == localInfo.ID.
	from, _ := net.ResolveUDPAddr("udp", "10.0.0.99:54321")
	msg := ReceivedMessage{
		From: from,
		Msg: GossipMessage{
			Type:     MsgAlive,
			SenderID: "self",
			SeqNum:   1,
		},
	}
	proto.processMessage(context.Background(), msg)

	if got := hook.rejected.Load(); got != 0 {
		t.Errorf("own-id message rejected: %d (validation should be skipped)", got)
	}
}

// TestProcessMessage_PiggybackForgedSenderDoesNotPoisonState is the
// audit's exact threat model: an attacker sends a forged message with
// piggyback Members claiming bogus state. With the C-2 fix, the
// message is dropped BEFORE piggyback processing, so the bogus state
// never reaches the membership table.
func TestProcessMessage_PiggybackForgedSenderDoesNotPoisonState(t *testing.T) {
	logger := silentLogger()
	ml := NewMembershipList("lb", logger)
	hook := &recordingMetricsHook{}
	ml.SetMetricsHook(hook)

	// Pre-pin "real-node" so subsequent messages from a different
	// src will be flagged as forged.
	realAddr := "10.0.0.5:9001"
	ml.AddOrUpdate(NodeInfo{ID: "real-node", Addr: realAddr, State: StateAlive, Incarnation: 1})
	ml.AddOrUpdate(NodeInfo{ID: "victim", Addr: "10.0.0.6:9001", State: StateAlive, Incarnation: 1})
	ml.VerifyOrPinSource("real-node", realAddr)

	tr, err := NewTransport(freeUDPAddr(t), 100, logger)
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	proto := NewProtocol(fastCfg("lb", "lb"), ml, tr, NodeInfo{ID: "lb"}, logger)
	proto.SetMetricsHook(hook)

	// Attacker sends from THEIR address but claims to be "real-node",
	// AND embeds a piggyback that would mark "victim" as Dead with a
	// forged high incarnation.
	attackerAddr, _ := net.ResolveUDPAddr("udp", "10.0.0.99:54321")
	forged := ReceivedMessage{
		From: attackerAddr,
		Msg: GossipMessage{
			Type:     MsgAlive,
			SenderID: "real-node",
			SeqNum:   1,
			Members: []NodeInfo{
				{ID: "victim", Addr: "10.0.0.6:9001", State: StateDead, Incarnation: 999},
			},
		},
	}
	proto.processMessage(context.Background(), forged)

	// Victim must still be Alive — the forged piggyback was dropped
	// along with the message.
	got, _ := ml.GetMember("victim")
	if got.State != StateAlive {
		t.Errorf("victim state poisoned by forged piggyback: %v", got.State)
	}
	if got.Incarnation == 999 {
		t.Errorf("victim incarnation poisoned: %d (forged value)", got.Incarnation)
	}
}

// TestSpoofRejectionMetricLabelStability defends against accidentally
// changing the label value (which would break dashboards in deployed
// systems). "source_mismatch" is intentionally a small, fixed label.
func TestSpoofRejectionMetricLabelStability(t *testing.T) {
	logger := silentLogger()
	ml := NewMembershipList("lb", logger)
	hook := &recordingMetricsHook{}
	ml.SetMetricsHook(hook)

	ml.AddOrUpdate(NodeInfo{ID: "n", State: StateAlive, Incarnation: 1})
	ml.VerifyOrPinSource("n", "10.0.0.1:9001")

	tr, _ := NewTransport(freeUDPAddr(t), 100, logger)
	proto := NewProtocol(fastCfg("lb", "lb"), ml, tr, NodeInfo{ID: "lb"}, logger)
	proto.SetMetricsHook(hook)

	from, _ := net.ResolveUDPAddr("udp", "10.0.0.99:9001") // mismatched
	proto.processMessage(context.Background(), ReceivedMessage{
		From: from,
		Msg:  GossipMessage{Type: MsgPing, SenderID: "n"},
	})

	got, _ := hook.reason.Load().(string)
	if !strings.HasPrefix(got, "source_") {
		t.Errorf("rejection reason changed format: %q (dashboards rely on stable labels)", got)
	}
	if got != "source_mismatch" {
		t.Errorf("rejection reason: got %q want %q", got, "source_mismatch")
	}
}

// =====================================================================
// Live two-node test: legitimate gossip continues working
// =====================================================================

// TestEndToEnd_LegitimateGossipFlowsAfterValidation is the regression
// guard: a real two-node setup must still complete its protocol round
// after the validation logic was added. If validation is too strict,
// this test will fail because legitimate messages get dropped.
func TestEndToEnd_LegitimateGossipFlowsAfterValidation(t *testing.T) {
	logger := silentLogger()
	addrA := freeUDPAddr(t)
	addrB := freeUDPAddr(t)

	mlA := NewMembershipList("node-a", logger)
	mlB := NewMembershipList("node-b", logger)

	nodeA := NodeInfo{ID: "node-a", Addr: addrA, ServiceAddr: addrA, State: StateAlive, Incarnation: 1}
	nodeB := NodeInfo{ID: "node-b", Addr: addrB, ServiceAddr: addrB, State: StateAlive, Incarnation: 1}

	mlA.AddOrUpdate(nodeA)
	mlA.AddOrUpdate(nodeB)
	mlB.AddOrUpdate(nodeA)
	mlB.AddOrUpdate(nodeB)

	tA, err := NewTransport(addrA, 100, logger)
	if err != nil {
		t.Fatalf("transport A: %v", err)
	}
	tB, err := NewTransport(addrB, 100, logger)
	if err != nil {
		t.Fatalf("transport B: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tA.Start(ctx)
	tB.Start(ctx)

	protoA := NewProtocol(fastCfg(addrA, "node-a"), mlA, tA, nodeA, logger)
	protoB := NewProtocol(fastCfg(addrB, "node-b"), mlB, tB, nodeB, logger)
	protoA.Start(ctx)
	protoB.Start(ctx)

	// Allow several protocol rounds — pings should ack normally,
	// neither node should mark the other suspect.
	time.Sleep(300 * time.Millisecond)

	infoA, _ := mlB.GetMember("node-a")
	infoB, _ := mlA.GetMember("node-b")
	if infoA.State != StateAlive {
		t.Errorf("B sees A as %v after validation added — gossip broken", infoA.State)
	}
	if infoB.State != StateAlive {
		t.Errorf("A sees B as %v after validation added — gossip broken", infoB.State)
	}
}
