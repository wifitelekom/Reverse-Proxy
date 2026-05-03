package gossip

import (
	"context"
	"net"
	"runtime"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// TestHandlePingReq_DrainsOnCtxCancel pins the H-2 fix.
//
// Setup: feed the protocol an MsgPingReq targeting a node whose address
// belongs to a UDP listener that NEVER replies. Without ctx-awareness in
// handlePingReq, the spawned goroutine would block on either replyCh or
// the IndirectTimeout (default 1s) regardless of protocol shutdown.
//
// With the fix, ctx cancel must drain the goroutine within milliseconds.
// goleak verifies no goroutine survives the test.
func TestHandlePingReq_DrainsOnCtxCancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	logger := silentLogger()

	// Local protocol bound to an ephemeral port.
	addrLocal := freeUDPAddr(t)
	addrTarget := freeUDPAddr(t) // chosen but never bound to a real listener

	ml := NewMembershipList("local", logger)
	local := NodeInfo{ID: "local", Addr: addrLocal, ServiceAddr: addrLocal, State: StateAlive, Incarnation: 1}
	target := NodeInfo{ID: "target", Addr: addrTarget, ServiceAddr: addrTarget, State: StateAlive, Incarnation: 1}
	requester := NodeInfo{ID: "requester", Addr: freeUDPAddr(t), ServiceAddr: "", State: StateAlive, Incarnation: 1}
	ml.AddOrUpdate(local)
	ml.AddOrUpdate(target)
	ml.AddOrUpdate(requester)

	tr, err := NewTransport(addrLocal, 100, logger)
	if err != nil {
		t.Fatalf("transport: %v", err)
	}

	// Use a long IndirectTimeout so the test would CLEARLY hang without
	// the ctx fix. If the timer wins instead of ctx, the goleak check
	// will fail with this goroutine still running.
	cfg := fastCfg(addrLocal, "local")
	cfg.IndirectTimeout = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	tr.Start(ctx)

	proto := NewProtocol(cfg, ml, tr, local, logger)

	// Don't start the full protocol loop — we want to drive a single
	// handlePingReq invocation deterministically.
	pingReq := GossipMessage{
		Type:     MsgPingReq,
		SenderID: "requester",
		TargetID: "target",
		SeqNum:   1,
	}
	proto.handlePingReq(ctx, pingReq)

	// Goroutine is now blocked in the select. Confirm it's actually
	// running before cancellation by polling runtime.NumGoroutine.
	deadline := time.Now().Add(200 * time.Millisecond)
	baseline := runtime.NumGoroutine()
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() > baseline-1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Cancel — this is the moment the fix must demonstrate.
	cancel()

	// Without the fix the goroutine would persist for the full 5s
	// IndirectTimeout. With the fix it returns within tens of microseconds.
	// We give it 250ms of slack for goroutine scheduling on busy CI hosts.
	drained := waitFor(func() bool {
		return runtime.NumGoroutine() <= baseline
	}, 250*time.Millisecond)
	if !drained {
		t.Fatalf("indirect-ping goroutine did not drain within 250ms; "+
			"NumGoroutine baseline=%d, current=%d — H-2 fix regressed?",
			baseline, runtime.NumGoroutine())
	}

	// goleak.VerifyNone in the deferred call performs the final check:
	// no goroutine traceable to our package may remain at function exit.
	// Allow time for transport's receiveLoop to fully stop.
	time.Sleep(50 * time.Millisecond)
}

// TestHandlePingReq_HappyPathStillForwardsAck guards against the fix
// breaking the normal acknowledged-by-target flow. Without ctx in the
// select, the original happy path is preserved; with ctx as the FIRST
// select case, we want to make sure replyCh still wins when it should.
func TestHandlePingReq_HappyPathStillForwardsAck(t *testing.T) {
	logger := silentLogger()
	addrLocal := freeUDPAddr(t)

	// Set up a sink that listens on a real UDP socket — represents the
	// "requester" that should receive the forwarded ACK.
	requesterAddr := freeUDPAddr(t)
	udpAddr, err := net.ResolveUDPAddr("udp", requesterAddr)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	requesterConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer requesterConn.Close()

	ml := NewMembershipList("local", logger)
	ml.AddOrUpdate(NodeInfo{ID: "local", Addr: addrLocal, State: StateAlive, Incarnation: 1})
	// Target's "address" is the local port — handlePingReq will fire a
	// ping there and the local protocol's own replyCh will be set when
	// we manually trigger it below.
	ml.AddOrUpdate(NodeInfo{ID: "target", Addr: addrLocal, State: StateAlive, Incarnation: 1})
	ml.AddOrUpdate(NodeInfo{ID: "requester", Addr: requesterAddr, State: StateAlive, Incarnation: 1})

	tr, err := NewTransport(addrLocal, 100, logger)
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	cfg := fastCfg(addrLocal, "local")
	cfg.IndirectTimeout = 500 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	proto := NewProtocol(cfg, ml, tr, NodeInfo{ID: "local", Addr: addrLocal}, logger)

	// Drive the handler. It will send a ping to "target" (== addrLocal,
	// our own port) and wait on replyCh.
	pingReq := GossipMessage{
		Type:     MsgPingReq,
		SenderID: "requester",
		TargetID: "target",
		SeqNum:   42,
	}
	proto.handlePingReq(ctx, pingReq)

	// Inject a synthetic ACK by walking the pendingPings map and pushing
	// to whatever channel was registered by the goroutine. The seq num
	// is allocated via nextSeq() inside the goroutine, so we don't know
	// it ahead of time — find any pending entry.
	deadline := time.Now().Add(200 * time.Millisecond)
	var pendingFound bool
	var registeredSeq uint64
	for time.Now().Before(deadline) {
		proto.pendingPings.Range(func(k, v any) bool {
			registeredSeq = k.(uint64)
			ch := v.(chan bool)
			select {
			case ch <- true:
				pendingFound = true
			default:
			}
			return false // stop after first
		})
		if pendingFound {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !pendingFound {
		t.Fatal("handler never registered a pending entry")
	}
	_ = registeredSeq

	// Now read on the requester's UDP socket — the forwarded ACK should
	// arrive within the IndirectTimeout window.
	requesterConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 4096)
	n, _, err := requesterConn.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("requester didn't receive forwarded ACK: %v", err)
	}
	if n == 0 {
		t.Errorf("empty forwarded packet")
	}
	// Don't assert on contents — we just want to know SOMETHING was
	// forwarded. Decoding the message would couple the test to the
	// JSON schema unnecessarily.
}
