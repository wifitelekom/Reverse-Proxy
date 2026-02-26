package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/types"
)

// main bir backend node'u başlatır.
// Hem HTTP servis hem UDP gossip çalıştırır.
//
// Başlatma sırası (kritik):
//  1. Config parse
//  2. MembershipList oluştur (shared state)
//  3. Transport başlat (UDP socket)
//  4. Protocol başlat (gossip goroutine'ları)
//  5. Seed node'lara ALIVE join mesajı gönder
//  6. HTTP server başlat
//  7. Signal bekle → graceful shutdown
func main() {
	nodeID := flag.String("id", "", "Node ID (örn: node-a)")
	gossipAddr := flag.String("gossip", "", "UDP gossip adresi (örn: :9001)")
	serviceAddr := flag.String("service", "", "HTTP servis adresi (örn: :8001)")
	seeds := flag.String("seeds", "", "Seed node'lar virgülle ayrılmış (örn: :9002,:9003)")
	flag.Parse()

	if *nodeID == "" || *gossipAddr == "" || *serviceAddr == "" {
		fmt.Fprintf(os.Stderr, "Kullanım: node -id=<id> -gossip=<addr> -service=<addr> [-seeds=<addr,...>]\n")
		flag.Usage()
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	logger = logger.With("node_id", *nodeID)

	cfg := types.DefaultConfig()
	cfg.GossipAddr = *gossipAddr
	cfg.ServiceAddr = *serviceAddr
	cfg.NodeID = *nodeID
	if *seeds != "" {
		cfg.SeedNodes = strings.Split(*seeds, ",")
	}

	localInfo := gossip.NodeInfo{
		ID:            *nodeID,
		Addr:          *gossipAddr,
		ServiceAddr:   *serviceAddr,
		State:         gossip.StateAlive,
		Incarnation:   1,
		LastHeartbeat: time.Now(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. MembershipList — shared state
	ml := gossip.NewMembershipList(*nodeID, logger)
	ml.AddOrUpdate(localInfo)

	// 2. UDP Transport
	transport, err := gossip.NewTransport(*gossipAddr, 100, logger)
	if err != nil {
		logger.Error("transport başlatılamadı", "err", err)
		os.Exit(1)
	}
	transport.Start(ctx)

	// 3. SWIM Protocol
	protocol := gossip.NewProtocol(cfg, ml, transport, localInfo, logger)
	protocol.Start(ctx)

	// 4. Seed node'lara join — ALIVE mesajı gönder
	for _, seed := range cfg.SeedNodes {
		seed = strings.TrimSpace(seed)
		if seed == "" {
			continue
		}
		joinMsg := gossip.GossipMessage{
			Type:     gossip.MsgAlive,
			SenderID: localInfo.ID,
			SeqNum:   1,
			Members:  []gossip.NodeInfo{localInfo},
		}
		if err := transport.Send(seed, joinMsg); err != nil {
			logger.Warn("seed'e join gönderilemedi", "seed", seed, "err", err)
		} else {
			logger.Info("seed'e join gönderildi", "seed", seed)
		}
	}

	// 5. HTTP Server
	mux := http.NewServeMux()

	// Ana servis endpoint'i
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"node":"%s","status":"ok","time":"%s"}`,
			*nodeID, time.Now().Format(time.RFC3339))
	})

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"healthy","node":"%s"}`, *nodeID)
	})

	// Gossip state debug — tüm üyeleri göster
	mux.HandleFunc("/members", func(w http.ResponseWriter, r *http.Request) {
		members := ml.GetAllMembers()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(members)
	})

	// Chaos engineering endpoint'leri — sadece ENABLE_DEBUG=1 ile aktif
	// Güvenlik: production ortamında bu endpoint'ler kapalı olmalı
	if os.Getenv("ENABLE_DEBUG") == "1" {
		logger.Warn("debug endpoints aktif (ENABLE_DEBUG=1) — production'da kapatın!")

		// Node'u öldür
		mux.HandleFunc("/debug/kill", func(w http.ResponseWriter, r *http.Request) {
			logger.Warn("kill endpoint çağrıldı — node kapatılıyor")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status":"killing","node":"%s"}`, *nodeID)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
		})

		// Node'u gossip'ten izole et (network partition simülasyonu)
		// duration query param: örn. /debug/partition?duration=15s
		mux.HandleFunc("/debug/partition", func(w http.ResponseWriter, r *http.Request) {
			dur := 15 * time.Second
			if d := r.URL.Query().Get("duration"); d != "" {
				if parsed, err := time.ParseDuration(d); err == nil {
					dur = parsed
				}
			}
			transport.SetPaused(true)
			time.AfterFunc(dur, func() {
				transport.SetPaused(false)
			})
			logger.Warn("partition aktif", "duration", dur)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"status":"partitioned","node":"%s","duration":"%s"}`, *nodeID, dur)
		})
	}

	httpServer := &http.Server{
		Addr:         *serviceAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("HTTP servis başlıyor", "addr", *serviceAddr)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("HTTP servis hatası", "err", err)
			cancel()
		}
	}()

	// 6. Signal bekle
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigCh:
		logger.Info("sinyal alındı, kapatılıyor", "signal", sig)
	case <-ctx.Done():
		logger.Info("context iptal edildi, kapatılıyor")
	}

	// Graceful shutdown sırası:
	// 1. HTTP server → yeni request alma
	// 2. Context cancel → gossip goroutine'ları durdur
	// 3. Transport kapanır (ctx'e bağlı)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP shutdown hatası", "err", err)
	}

	cancel()
	logger.Info("node kapatıldı", "id", *nodeID)
}
