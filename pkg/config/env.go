package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// envPrefix is the prefix for all environment variables this package reads.
// Variables: P2PLB_LISTEN, P2PLB_STRATEGY, P2PLB_GOSSIP_ADDR, P2PLB_NODE_ID,
// P2PLB_SEED_NODES (comma-separated), P2PLB_SERVICE_ADDR (node only),
// P2PLB_LOG_LEVEL, P2PLB_LOG_FORMAT,
// P2PLB_PROTOCOL_INTERVAL, P2PLB_PROTOCOL_PING_TIMEOUT, etc.
const envPrefix = "P2PLB_"

// ApplyEnvLB overlays environment variables on top of an LBConfig.
// Env values override file values but lose to CLI flags (which apply later).
func ApplyEnvLB(cfg LBConfig) LBConfig {
	if v := os.Getenv(envPrefix + "LISTEN"); v != "" {
		cfg.Listen = v
	}
	if v := os.Getenv(envPrefix + "STRATEGY"); v != "" {
		cfg.Strategy = v
	}
	if v := os.Getenv(envPrefix + "GOSSIP_ADDR"); v != "" {
		cfg.GossipAddr = v
	}
	if v := os.Getenv(envPrefix + "NODE_ID"); v != "" {
		cfg.NodeID = v
	}
	if v := os.Getenv(envPrefix + "SEED_NODES"); v != "" {
		cfg.SeedNodes = splitAndTrim(v)
	}
	cfg.Logging = applyEnvLogging(cfg.Logging)
	cfg.Protocol = applyEnvProtocol(cfg.Protocol)
	return cfg
}

// ApplyEnvNode overlays environment variables on top of a NodeConfig.
func ApplyEnvNode(cfg NodeConfig) NodeConfig {
	if v := os.Getenv(envPrefix + "NODE_ID"); v != "" {
		cfg.ID = v
	}
	if v := os.Getenv(envPrefix + "GOSSIP_ADDR"); v != "" {
		cfg.GossipAddr = v
	}
	if v := os.Getenv(envPrefix + "SERVICE_ADDR"); v != "" {
		cfg.ServiceAddr = v
	}
	if v := os.Getenv(envPrefix + "SEED_NODES"); v != "" {
		cfg.SeedNodes = splitAndTrim(v)
	}
	cfg.Logging = applyEnvLogging(cfg.Logging)
	cfg.Protocol = applyEnvProtocol(cfg.Protocol)
	return cfg
}

func applyEnvLogging(l LoggingCfg) LoggingCfg {
	if v := os.Getenv(envPrefix + "LOG_LEVEL"); v != "" {
		l.Level = v
	}
	if v := os.Getenv(envPrefix + "LOG_FORMAT"); v != "" {
		l.Format = v
	}
	return l
}

func applyEnvProtocol(p ProtocolCfg) ProtocolCfg {
	if v := os.Getenv(envPrefix + "PROTOCOL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			p.Interval = Duration{d}
		}
	}
	if v := os.Getenv(envPrefix + "PROTOCOL_PING_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			p.PingTimeout = Duration{d}
		}
	}
	if v := os.Getenv(envPrefix + "PROTOCOL_INDIRECT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			p.IndirectTimeout = Duration{d}
		}
	}
	if v := os.Getenv(envPrefix + "PROTOCOL_SUSPECT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			p.SuspectTimeout = Duration{d}
		}
	}
	if v := os.Getenv(envPrefix + "PROTOCOL_INDIRECT_NODES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			p.IndirectNodes = n
		}
	}
	return p
}

// splitAndTrim splits a comma-separated string and trims whitespace.
// Empty entries are dropped.
func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
