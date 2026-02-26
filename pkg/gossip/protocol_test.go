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

// freeUDPAddr ephemeral UDP adresi bulur (OS'un atamasıyla).
func freeUDPAddr(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("ephemeral port bulunamadı: %v", err)
	}
	addr := conn.LocalAddr().String()
	conn.Close()
	// Kısa süre bekle — OS port'u serbest bırakmak için
	time.Sleep(5 * time.Millisecond)
	return addr
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// fastCfg test için hızlandırılmış SWIM zamanlamaları.
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

// TestProtocol_PingAck: A protocol'ü B'ye ping atar, B ACK döner.
// A'nın membership'inde B Alive olarak kalmalı.
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
		t.Fatalf("transport A hatası: %v", err)
	}
	tB, err := NewTransport(addrB, 100, logger)
	if err != nil {
		t.Fatalf("transport B hatası: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	tA.Start(ctx)
	tB.Start(ctx)

	protoA := NewProtocol(fastCfg(addrA, "node-a"), mlA, tA, nodeA, logger)
	protoB := NewProtocol(fastCfg(addrB, "node-b"), mlB, tB, nodeB, logger)
	protoA.Start(ctx)
	protoB.Start(ctx)

	// Birkaç protocol round geçmesine izin ver
	time.Sleep(300 * time.Millisecond)

	// A ve B birbirini Alive olarak görmeli
	infoB, found := mlA.GetMember("node-b")
	if !found {
		t.Fatal("A, B'yi membership'inde göremedi")
	}
	if infoB.State != StateAlive {
		t.Fatalf("B'nin state'i Alive olmalı, alınan: %v", infoB.State)
	}

	infoA, found := mlB.GetMember("node-a")
	if !found {
		t.Fatal("B, A'yı membership'inde göremedi")
	}
	if infoA.State != StateAlive {
		t.Fatalf("A'nın state'i Alive olmalı, alınan: %v", infoA.State)
	}
}

// TestProtocol_SuspectOnTimeout: A, yanıt vermeyen B'yi ping eder.
// T_ping + T_indirect süresinden sonra B Suspect veya Dead olmalı.
func TestProtocol_SuspectOnTimeout(t *testing.T) {
	logger := silentLogger()

	addrA := freeUDPAddr(t)
	addrB := freeUDPAddr(t) // Bu adreste UDP dinleyen YOK — kasıtlı

	mlA := NewMembershipList("node-a", logger)
	nodeA := NodeInfo{ID: "node-a", Addr: addrA, ServiceAddr: addrA, State: StateAlive, Incarnation: 1}
	// node-b adresi var ama dinlemiyor — ping timeout olacak
	nodeB := NodeInfo{ID: "node-b", Addr: addrB, ServiceAddr: addrB, State: StateAlive, Incarnation: 1}
	mlA.AddOrUpdate(nodeA)
	mlA.AddOrUpdate(nodeB)

	tA, err := NewTransport(addrA, 100, logger)
	if err != nil {
		t.Fatalf("transport A hatası: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tA.Start(ctx)

	cfg := fastCfg(addrA, "node-a")
	protoA := NewProtocol(cfg, mlA, tA, nodeA, logger)
	protoA.Start(ctx)

	// Bir protocol round çalıştır — B'ye ping atar, timeout olur
	protoA.runProtocolRound(ctx)

	// T_ping + T_indirect + buffer kadar bekle
	time.Sleep(cfg.PingTimeout + cfg.IndirectTimeout + 100*time.Millisecond)

	info, found := mlA.GetMember("node-b")
	if !found {
		t.Fatal("node-b membership'ten silinmemeli — sadece suspect/dead olmalı")
	}
	if info.State == StateAlive {
		t.Fatalf("yanıtsız node hâlâ Alive — Suspect veya Dead bekleniyor, alınan: %v", info.State)
	}
	t.Logf("node-b state: %v (beklenen: Suspect veya Dead) ✓", info.State)
}
