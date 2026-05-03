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

	"p2p-loadbalancer/pkg/config"
	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/healthcheck"
	"p2p-loadbalancer/pkg/loadbalancer"
	"p2p-loadbalancer/pkg/metrics"
)

// main starts the load balancer node.
// LB joins the gossip ring as a passive member (for membership tracking)
// and runs as an HTTP reverse proxy.
//
// Configuration precedence (highest wins):
//  1. CLI flags
//  2. Environment variables (P2PLB_*)
//  3. Config file (-config <path>, JSON or TOML)
//  4. Built-in defaults
//
// Startup sequence:
//  1. Resolve config (file + env + flags)
//  2. Build MembershipList
//  3. Start gossip transport + protocol (LB joins passively)
//  4. Send join message to seed nodes
//  5. Pick selector (roundrobin | leastconn)
//  6. Start HTTP proxy (blocking)
func main() {
	// CLI flags. Defaults are "" so the merge layer can tell apart "user set
	// to default value" from "user didn't pass the flag at all".
	configFile := flag.String("config", "", "Config file path (JSON or TOML, optional)")
	listenAddr := flag.String("listen", "", "HTTP listen address (default :8080)")
	strategy := flag.String("strategy", "", "Strategy: roundrobin|leastconn|p2c|p2c-ewma (default roundrobin)")
	gossipAddr := flag.String("gossip", "", "UDP gossip address (default :9000)")
	nodeID := flag.String("node-id", "", "Gossip node ID (default loadbalancer)")
	seeds := flag.String("seeds", "", "Comma-separated seed nodes")
	logLevel := flag.String("log-level", "", "Log level: debug|info|warn|error (default info)")
	logFormat := flag.String("log-format", "", "Log format: text|json (default json)")
	shutdownTimeout := flag.Duration("shutdown-timeout", 0,
		"Graceful shutdown budget for in-flight requests (default 30s)")

	// TLS flags. Defaults are zero so the merge layer can detect "not set".
	// Booleans use *string trickery via empty == not-set; for simplicity we
	// just expose Bool flags directly and require the operator to use the
	// config file for nuanced setups.
	tlsEnabled := flag.Bool("tls", false, "Enable HTTPS listener for client traffic")
	tlsCert := flag.String("tls-cert", "", "TLS certificate file (PEM)")
	tlsKey := flag.String("tls-key", "", "TLS private key file (PEM)")
	tlsMinVer := flag.String("tls-min-version", "", "Minimum TLS version: 1.2 or 1.3 (default 1.2)")
	tlsClientCA := flag.String("tls-client-ca", "", "Client CA bundle (PEM) — enables mTLS")
	tlsAutoReload := flag.Bool("tls-auto-reload", false, "Periodically reload cert/key from disk")
	backendTLS := flag.Bool("backend-tls", false, "Speak HTTPS to backends (LB→backend)")
	backendTLSSkipVerify := flag.Bool("backend-tls-skip-verify", false, "Skip backend cert validation (DEV ONLY)")
	backendTLSCA := flag.String("backend-tls-ca", "", "CA bundle (PEM) for backend cert validation")

	// Observability flags. Metrics are opt-in: when off, the LB allocates
	// nothing for Prometheus and the /_lb/metrics endpoint returns 503.
	metricsEnabled := flag.Bool("metrics", false, "Expose Prometheus metrics at /_lb/metrics")

	flag.Parse()

	// 1. Resolve config — file (or defaults) → env → flags
	cfg := config.DefaultLBConfig()
	if *configFile != "" {
		loaded, err := config.LoadLB(*configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "config load error: %v\n", err)
			os.Exit(1)
		}
		cfg = loaded
	}
	cfg = config.ApplyEnvLB(cfg)

	flags := config.LBFlags{
		Listen:          *listenAddr,
		Strategy:        *strategy,
		GossipAddr:      *gossipAddr,
		NodeID:          *nodeID,
		SeedNodes:       splitSeeds(*seeds),
		ShutdownTimeout: *shutdownTimeout,
		LogLevel:        *logLevel,
		LogFormat:       *logFormat,
	}
	cfg = config.MergeLB(cfg, flags)

	// TLS flags applied directly — the merge package's pattern doesn't
	// extend cleanly to bools, and TLS is a small, contained surface.
	// Booleans always overwrite (no "not set" distinction with bool flags).
	if *tlsEnabled {
		cfg.TLS.Enabled = true
	}
	if *tlsCert != "" {
		cfg.TLS.CertFile = *tlsCert
	}
	if *tlsKey != "" {
		cfg.TLS.KeyFile = *tlsKey
	}
	if *tlsMinVer != "" {
		cfg.TLS.MinVersion = *tlsMinVer
	}
	if *tlsClientCA != "" {
		cfg.TLS.ClientCAFile = *tlsClientCA
	}
	if *tlsAutoReload {
		cfg.TLS.AutoReload = true
	}
	if *backendTLS {
		cfg.BackendTLS.Enabled = true
	}
	if *backendTLSSkipVerify {
		cfg.BackendTLS.SkipVerify = true
	}
	if *backendTLSCA != "" {
		cfg.BackendTLS.CAFile = *backendTLSCA
	}

	if err := config.ValidateLB(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	// Build slog logger from resolved logging config.
	logger := buildLogger(cfg.Logging).With("component", "loadbalancer")

	// Convert to internal types used by gossip + loadbalancer packages.
	internal := cfg.ToInternal()
	if err := internal.Protocol.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid protocol config: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Shared membership list
	ml := gossip.NewMembershipList(internal.NodeID, logger)

	// LB advertises itself with empty ServiceAddr — it isn't a backend.
	lbNode := gossip.NodeInfo{
		ID:            internal.NodeID,
		Addr:          internal.GossipAddr,
		ServiceAddr:   "",
		State:         gossip.StateAlive,
		Incarnation:   1,
		LastHeartbeat: time.Now(),
	}
	ml.AddOrUpdate(lbNode)

	// 3. UDP transport + SWIM protocol (passive)
	transport, err := gossip.NewTransport(internal.GossipAddr, 100, logger)
	if err != nil {
		logger.Error("transport start failed", "err", err)
		os.Exit(1)
	}
	transport.Start(ctx)

	// Build the metrics registry early so we can attach hooks before the
	// protocol starts emitting events. Nil means metrics disabled — the
	// hot-path nil checks turn into a no-op.
	var m *metrics.Metrics
	if *metricsEnabled {
		m = metrics.New()
		// Bridge gossip events into Prometheus counters via a small
		// adapter that satisfies gossip.MetricsHook implicitly.
		hook := metricsAdapter{m: m}
		ml.SetMetricsHook(hook)
		// protocol gets its hook below right after construction.
		logger.Info("Prometheus metrics enabled", "endpoint", "/_lb/metrics")
	}

	protocol := gossip.NewProtocol(internal.Protocol, ml, transport, lbNode, logger)
	if m != nil {
		protocol.SetMetricsHook(metricsAdapter{m: m})
	}
	protocol.Start(ctx)

	// 4. Send ALIVE join to each seed
	for _, seed := range internal.SeedNodes {
		seed = strings.TrimSpace(seed)
		if seed == "" {
			continue
		}
		joinMsg := gossip.GossipMessage{
			Type:     gossip.MsgAlive,
			SenderID: internal.NodeID,
			SeqNum:   1,
			Members:  []gossip.NodeInfo{lbNode},
		}
		if err := transport.Send(seed, joinMsg); err != nil {
			logger.Warn("seed join send failed", "seed", seed, "err", err)
		} else {
			logger.Info("seed join sent", "seed", seed)
		}
	}

	// 5. Selector
	var sel loadbalancer.Selector
	switch internal.Strategy {
	case "leastconn":
		sel = loadbalancer.NewLeastConnections()
		logger.Info("strategy: least connections")
	case "p2c":
		sel = loadbalancer.NewP2C()
		logger.Info("strategy: power of two choices")
	case "p2c-ewma":
		sel = loadbalancer.NewP2CEWMA()
		logger.Info("strategy: power of two choices (EWMA latency)")
	default:
		sel = loadbalancer.NewRoundRobin()
		logger.Info("strategy: round robin")
	}

	// Optional HTTP health checker. nil → disabled (the historical default,
	// preserves prior behaviour exactly).
	var hc *healthcheck.Checker
	if internal.HealthCheck.Enabled {
		hcCfg := healthcheck.Config{
			Enabled:            internal.HealthCheck.Enabled,
			Interval:           internal.HealthCheck.Interval,
			Timeout:            internal.HealthCheck.Timeout,
			Path:               internal.HealthCheck.Path,
			HealthyThreshold:   internal.HealthCheck.HealthyThreshold,
			UnhealthyThreshold: internal.HealthCheck.UnhealthyThreshold,
			DegradedLatency:    internal.HealthCheck.DegradedLatency,
		}
		if err := hcCfg.Validate(); err != nil {
			fmt.Fprintf(os.Stderr, "invalid health-check config: %v\n", err)
			os.Exit(1)
		}
		hc = healthcheck.NewChecker(hcCfg, logger.With("component", "healthcheck"))
		hc.SetOnStatusChange(func(nodeID string, old, new healthcheck.Status) {
			logger.Info("backend health change",
				"node", nodeID, "from", old.String(), "to", new.String())
			if m != nil {
				m.HealthStatusTransitions.WithLabelValues(old.String(), new.String()).Inc()
			}
		})
		if m != nil {
			hc.SetMetricsHook(healthMetricsAdapter{m: m})
		}
		hc.Start(ctx)
		logger.Info("HTTP health check enabled",
			"interval", internal.HealthCheck.Interval,
			"path", internal.HealthCheck.Path)
	}

	lb, err := loadbalancer.NewLoadBalancer(internal, ml, sel, logger, hc, m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load balancer init failed: %v\n", err)
		os.Exit(1)
	}

	// Signal handling — context cancel triggers graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		select {
		case sig := <-sigCh:
			fmt.Fprintf(os.Stderr, "signal received: %v\n", sig)
		case <-ctx.Done():
		}
		cancel()
	}()

	// 6. Blocking HTTP proxy. lb.Start invokes Shutdown when ctx is canceled.
	if err := lb.Start(ctx); err != nil {
		logger.Error("load balancer error", "err", err)
		os.Exit(1)
	}

	logger.Info("load balancer stopped")
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

// metricsAdapter bridges gossip.MetricsHook (a narrow per-package interface)
// to the wider metrics.Metrics struct. Defined here in main rather than in
// the metrics package so the metrics package stays free of any gossip
// imports — preserves the layering rule that metrics is a leaf dependency.
type metricsAdapter struct{ m *metrics.Metrics }

func (a metricsAdapter) OnMessageSent(t string)     { a.m.GossipMessagesSent.WithLabelValues(t).Inc() }
func (a metricsAdapter) OnMessageReceived(t string) { a.m.GossipMessagesReceived.WithLabelValues(t).Inc() }
func (a metricsAdapter) OnStateChange(_ string, from, to string) {
	a.m.GossipStateTransitions.WithLabelValues(from, to).Inc()
}
func (a metricsAdapter) OnLifeguardScore(score int) { a.m.GossipLifeguardNoop.Set(float64(score)) }

// healthMetricsAdapter bridges healthcheck.MetricsHook to the metrics struct.
// Same layering rationale as metricsAdapter — kept in main so the
// healthcheck and metrics packages stay decoupled.
type healthMetricsAdapter struct{ m *metrics.Metrics }

func (a healthMetricsAdapter) OnCheck(result string, elapsed time.Duration) {
	a.m.HealthChecksTotal.WithLabelValues(result).Inc()
	a.m.HealthCheckDuration.WithLabelValues(result).Observe(elapsed.Seconds())
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
