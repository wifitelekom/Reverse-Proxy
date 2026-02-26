package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/loadbalancer"
	"p2p-loadbalancer/pkg/types"
)

// main Load Balancer node'u başlatır.
// LB gossip ağına pasif üye olarak katılır (membership takibi için)
// ve HTTP reverse proxy olarak çalışır.
//
// Başlatma sırası:
//  1. Config parse
//  2. MembershipList oluştur
//  3. Gossip transport + protocol başlat (LB pasif üye)
//  4. Seed node'lara join gönder
//  5. Selector seç (roundrobin | leastconn)
//  6. HTTP proxy başlat (blocking)
func main() {
	listenAddr := flag.String("listen", ":8080", "LB HTTP dinleme adresi")
	strategy := flag.String("strategy", "roundrobin", "Strateji: roundrobin|leastconn")
	gossipAddr := flag.String("gossip", ":9000", "LB UDP gossip adresi")
	seeds := flag.String("seeds", "", "Seed node'lar virgülle ayrılmış")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	logger = logger.With("component", "loadbalancer")

	cfg := types.LBConfig{
		ListenAddr: *listenAddr,
		Strategy:   *strategy,
		GossipAddr: *gossipAddr,
		NodeID:     "loadbalancer",
		Protocol:   types.DefaultConfig(),
	}
	cfg.Protocol.GossipAddr = *gossipAddr
	cfg.Protocol.NodeID = cfg.NodeID

	if *seeds != "" {
		cfg.SeedNodes = strings.Split(*seeds, ",")
		cfg.Protocol.SeedNodes = cfg.SeedNodes
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Shared membership list
	ml := gossip.NewMembershipList(cfg.NodeID, logger)

	// LB kendini gossip ağına ekler — ServiceAddr boş (backend değil)
	lbNode := gossip.NodeInfo{
		ID:            cfg.NodeID,
		Addr:          cfg.GossipAddr,
		ServiceAddr:   "", // LB backend olarak listelenmez
		State:         gossip.StateAlive,
		Incarnation:   1,
		LastHeartbeat: time.Now(),
	}
	ml.AddOrUpdate(lbNode)

	// UDP Transport
	transport, err := gossip.NewTransport(cfg.GossipAddr, 100, logger)
	if err != nil {
		logger.Error("transport başlatılamadı", "err", err)
		os.Exit(1)
	}
	transport.Start(ctx)

	// SWIM Protocol — LB pasif üye olarak katılır
	protocol := gossip.NewProtocol(cfg.Protocol, ml, transport, lbNode, logger)
	protocol.Start(ctx)

	// Seed node'lara join
	for _, seed := range cfg.SeedNodes {
		seed = strings.TrimSpace(seed)
		if seed == "" {
			continue
		}
		joinMsg := gossip.GossipMessage{
			Type:     gossip.MsgAlive,
			SenderID: cfg.NodeID,
			SeqNum:   1,
			Members:  []gossip.NodeInfo{lbNode},
		}
		if err := transport.Send(seed, joinMsg); err != nil {
			logger.Warn("seed'e join gönderilemedi", "seed", seed, "err", err)
		} else {
			logger.Info("seed'e join gönderildi", "seed", seed)
		}
	}

	// Selector seç
	var sel loadbalancer.Selector
	switch cfg.Strategy {
	case "leastconn":
		sel = loadbalancer.NewLeastConnections()
		logger.Info("strateji: least connections")
	default:
		sel = loadbalancer.NewRoundRobin()
		logger.Info("strateji: round robin")
	}

	lb := loadbalancer.NewLoadBalancer(cfg, ml, sel, logger)

	// Signal handling — context cancel ile graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		select {
		case sig := <-sigCh:
			fmt.Fprintf(os.Stderr, "sinyal alındı: %v\n", sig)
		case <-ctx.Done():
		}
		cancel()
	}()

	// Blocking — ctx iptalinde lb.Start içinde Shutdown çağrılır
	if err := lb.Start(ctx); err != nil {
		logger.Error("load balancer hatası", "err", err)
		os.Exit(1)
	}

	logger.Info("load balancer kapatıldı")
}
