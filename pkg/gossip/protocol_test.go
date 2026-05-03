package gossip

import (
	"context"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"p2p-loadbalancer/pkg/types"
)

// freeUDPAddr finds an ephemeral UDP address by asking the OS for one.
func freeUDPAddr(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("could not allocate ephemeral port: %v", err)
	}
	addr := conn.LocalAddr().String()
	conn.Close()
	// Brief pause so the OS can release the port for re-binding.
	time.Sleep(5 * time.Millisecond)
	return addr
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// fastCfg returns SWIM timing parameters tightened for fast tests.
func fastCfg(gossipAddr, nodeID string) types.ProtocolConfig {
	cfg := types.DefaultConfig()
	cfg.GossipAddr = gossipAddr
	cfg.NodeID = nodeID
	cfg.PingTimeout = 50 * time.Millisecond
	cfg.IndirectTimeout = 100 * time.Millisecond
	cfg.SuspectTimeout = 200 * time.Millisecond
	cfg.IndirectNodes = 1
	return cfg
}

// TestProtocol_PingAck: A pings B, B ACKs. After a few protocol rounds
// each node must see the other as Alive.
func TestProtocol_PingAck(t *testing.T) {
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

	// Allow a few protocol rounds to elapse.
	time.Sleep(300 * time.Millisecond)

	// A and B should each see the other as Alive.
	infoB, found := mlA.GetMember("node-b")
	if !found {
		t.Fatal("A doesn't see B in membership")
	}
	if infoB.State != StateAlive {
		t.Fatalf("expected B Alive, got: %v", infoB.State)
	}

	infoA, found := mlB.GetMember("node-a")
	if !found {
		t.Fatal("B doesn't see A in membership")
	}
	if infoA.State != StateAlive {
		t.Fatalf("expected A Alive, got: %v", infoA.State)
	}
}

// TestProtocol_SuspectOnTimeout: A pings B at a port nobody listens on.
// After T_ping + T_indirect, B must be Suspect or Dead.
func TestProtocol_SuspectOnTimeout(t *testing.T) {
	logger := silentLogger()

	addrA := freeUDPAddr(t)
	addrB := freeUDPAddr(t) // intentionally NO listener at this address

	mlA := NewMembershipList("node-a", logger)
	nodeA := NodeInfo{ID: "node-a", Addr: addrA, ServiceAddr: addrA, State: StateAlive, Incarnation: 1}
	// node-b's address is set but nobody listens — pings will time out.
	nodeB := NodeInfo{ID: "node-b", Addr: addrB, ServiceAddr: addrB, State: StateAlive, Incarnation: 1}
	mlA.AddOrUpdate(nodeA)
	mlA.AddOrUpdate(nodeB)

	tA, err := NewTransport(addrA, 100, logger)
	if err != nil {
		t.Fatalf("transport A: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tA.Start(ctx)

	cfg := fastCfg(addrA, "node-a")
	protoA := NewProtocol(cfg, mlA, tA, nodeA, logger)
	protoA.Start(ctx)

	// Run one protocol round — A pings B, B never replies.
	protoA.runProtocolRound(ctx)

	// Wait T_ping + T_indirect + a small buffer.
	time.Sleep(cfg.PingTimeout + cfg.IndirectTimeout + 100*time.Millisecond)

	info, found := mlA.GetMember("node-b")
	if !found {
		t.Fatal("node-b should not be removed from membership — only Suspect or Dead expected")
	}
	if info.State == StateAlive {
		t.Fatalf("non-responsive node still Alive — expected Suspect or Dead, got: %v", info.State)
	}
	t.Logf("node-b state: %v (expected Suspect or Dead) ✓", info.State)
}
