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
	"sync"
	"syscall"
	"time"

	"p2p-loadbalancer/pkg/config"
	"p2p-loadbalancer/pkg/gossip"
)

// main starts a backend node. It runs both an HTTP service and the UDP
// gossip protocol.
//
// Configuration precedence (highest wins):
//  1. CLI flags
//  2. Environment variables (P2PLB_*)
//  3. Config file (-config <path>)
//  4. Built-in defaults (id/gossip/service still required — no defaults)
//
// Startup sequence (order is critical):
//  1. Resolve config
//  2. Build MembershipList (shared state)
//  3. Start UDP transport
//  4. Start SWIM protocol goroutines
//  5. Send ALIVE join to seeds
//  6. Start HTTP service
//  7. Wait for signal → graceful shutdown
func main() {
	configFile := flag.String("config", "", "Config file path (JSON or TOML, optional)")
	nodeID := flag.String("id", "", "Node ID (e.g. node-a)")
	gossipAddr := flag.String("gossip", "", "UDP gossip address (e.g. :9001)")
	serviceAddr := flag.String("service", "", "HTTP service address (e.g. :8001)")
	seeds := flag.String("seeds", "", "Comma-separated seed nodes (e.g. :9002,:9003)")
	logLevel := flag.String("log-level", "", "Log level: debug|info|warn|error (default info)")
	logFormat := flag.String("log-format", "", "Log format: text|json (default json)")
	flag.Parse()

	// 1. Resolve config — file (or defaults) → env → flags
	cfg := config.DefaultNodeConfig()
	if *configFile != "" {
		loaded, err := config.LoadNode(*configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "config load error: %v\n", err)
			os.Exit(1)
		}
		cfg = loaded
	}
	cfg = config.ApplyEnvNode(cfg)

	flags := config.NodeFlags{
		ID:          *nodeID,
		GossipAddr:  *gossipAddr,
		ServiceAddr: *serviceAddr,
		SeedNodes:   splitSeeds(*seeds),
		LogLevel:    *logLevel,
		LogFormat:   *logFormat,
	}
	cfg = config.MergeNode(cfg, flags)

	if err := config.ValidateNode(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		flag.Usage()
		os.Exit(1)
	}

	logger := buildLogger(cfg.Logging).With("node_id", cfg.ID)

	internal := cfg.ToInternal()
	if err := internal.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid protocol config: %v\n", err)
		os.Exit(1)
	}

	localInfo := gossip.NodeInfo{
		ID:            cfg.ID,
		Addr:          cfg.GossipAddr,
		ServiceAddr:   cfg.ServiceAddr,
		State:         gossip.StateAlive,
		Incarnation:   1,
		LastHeartbeat: time.Now(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. MembershipList — shared state
	ml := gossip.NewMembershipList(cfg.ID, logger)
	ml.AddOrUpdate(localInfo)

	// 3. UDP transport
	transport, err := gossip.NewTransport(cfg.GossipAddr, 100, logger)
	if err != nil {
		logger.Error("transport start failed", "err", err)
		os.Exit(1)
	}
	transport.Start(ctx)

	// 4. SWIM protocol
	protocol := gossip.NewProtocol(internal, ml, transport, localInfo, logger)
	protocol.Start(ctx)

	// 5. Seed join — send ALIVE
	for _, seed := range internal.SeedNodes {
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
			logger.Warn("seed join send failed", "seed", seed, "err", err)
		} else {
			logger.Info("seed join sent", "seed", seed)
		}
	}

	// 6. HTTP server
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"node":"%s","status":"ok","time":"%s"}`,
			cfg.ID, time.Now().Format(time.RFC3339))
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"healthy","node":"%s"}`, cfg.ID)
	})

	mux.HandleFunc("/members", func(w http.ResponseWriter, r *http.Request) {
		members := ml.GetAllMembers()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(members)
	})

	// Chaos endpoints — only enabled with ENABLE_DEBUG=1.
	// Security: must stay off in production.
	if os.Getenv("ENABLE_DEBUG") == "1" {
		logger.Warn("debug endpoints enabled (ENABLE_DEBUG=1) — disable in production!")

		mux.HandleFunc("/debug/kill", func(w http.ResponseWriter, r *http.Request) {
			logger.Warn("kill endpoint invoked — node shutting down")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status":"killing","node":"%s"}`, cfg.ID)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
		})

		// Network partition simulation — cancels any prior timer to avoid
		// races on overlapping calls.
		var (
			partitionMu    sync.Mutex
			partitionTimer *time.Timer
		)
		mux.HandleFunc("/debug/partition", func(w http.ResponseWriter, r *http.Request) {
			dur := 15 * time.Second
			if d := r.URL.Query().Get("duration"); d != "" {
				if parsed, err := time.ParseDuration(d); err == nil {
					dur = parsed
				}
			}
			transport.SetPaused(true)

			partitionMu.Lock()
			if partitionTimer != nil {
				partitionTimer.Stop()
			}
			partitionTimer = time.AfterFunc(dur, func() {
				transport.SetPaused(false)
			})
			partitionMu.Unlock()

			logger.Warn("partition active", "duration", dur)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"status":"partitioned","node":"%s","duration":"%s"}`, cfg.ID, dur)
		})
	}

	httpServer := &http.Server{
		Addr:         cfg.ServiceAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("HTTP service starting", "addr", cfg.ServiceAddr)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("HTTP service error", "err", err)
			cancel()
		}
	}()

	// 7. Signal wait
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigCh:
		logger.Info("signal received, shutting down", "signal", sig)
	case <-ctx.Done():
		logger.Info("context canceled, shutting down")
	}

	// Graceful shutdown:
	// 1. HTTP server stops accepting requests
	// 2. Context cancel stops gossip goroutines
	// 3. Transport closes via ctx
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP shutdown error", "err", err)
	}

	cancel()
	logger.Info("node stopped", "id", cfg.ID)
}

// splitSeeds parses a comma-separated seed list. Returns nil if empty so the
// merge layer can distinguish "no flag" from "explicit empty".
func splitSeeds(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// buildLogger creates a slog logger respecting the resolved logging config.
func buildLogger(cfg config.LoggingCfg) *slog.Logger {
	level := slog.LevelInfo
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	if cfg.Format == "text" {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}
