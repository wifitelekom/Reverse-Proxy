package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LoadNode reads and parses a node config file. Format is determined by file
// extension (.json or .toml). TOML support requires the `toml` build tag.
//
// The returned config has defaults applied for any zero-valued fields in the
// Protocol and Logging sections.
func LoadNode(path string) (NodeConfig, error) {
	cfg := DefaultNodeConfig()
	if err := loadInto(path, &cfg); err != nil {
		return NodeConfig{}, err
	}
	cfg.Protocol = applyProtocolDefaults(cfg.Protocol)
	cfg.Logging = applyLoggingDefaults(cfg.Logging)
	return cfg, nil
}

// LoadLB reads and parses a load balancer config file.
func LoadLB(path string) (LBConfig, error) {
	cfg := DefaultLBConfig()
	if err := loadInto(path, &cfg); err != nil {
		return LBConfig{}, err
	}
	// File values overwrite defaults during Unmarshal; ensure zero-valued
	// nested fields still get sensible defaults.
	cfg.Protocol = applyProtocolDefaults(cfg.Protocol)
	cfg.Logging = applyLoggingDefaults(cfg.Logging)
	cfg.HealthCheck = applyHealthCheckDefaults(cfg.HealthCheck)
	cfg.TLS = applyTLSDefaults(cfg.TLS)
	if cfg.Listen == "" {
		cfg.Listen = ":8080"
	}
	if cfg.Strategy == "" {
		cfg.Strategy = "roundrobin"
	}
	if cfg.GossipAddr == "" {
		cfg.GossipAddr = ":9000"
	}
	if cfg.NodeID == "" {
		cfg.NodeID = "loadbalancer"
	}
	return cfg, nil
}

// applyTLSDefaults fills MinVersion and ReloadInterval if unset. Enabled
// is preserved — operators must opt in by setting it true.
func applyTLSDefaults(t TLSCfg) TLSCfg {
	d := DefaultTLSCfg()
	if t.MinVersion == "" {
		t.MinVersion = d.MinVersion
	}
	if t.ReloadInterval.Duration == 0 {
		t.ReloadInterval = d.ReloadInterval
	}
	return t
}

// applyHealthCheckDefaults fills zero-valued duration/int fields. Enabled is
// preserved as-is — the user must opt in explicitly. The defaults are
// applied unconditionally so flipping Enabled to true gives a working
// checker without any other changes.
func applyHealthCheckDefaults(h HealthCheckCfg) HealthCheckCfg {
	d := DefaultHealthCheckCfg()
	if h.Interval.Duration == 0 {
		h.Interval = d.Interval
	}
	if h.Timeout.Duration == 0 {
		h.Timeout = d.Timeout
	}
	if h.Path == "" {
		h.Path = d.Path
	}
	if h.HealthyThreshold == 0 {
		h.HealthyThreshold = d.HealthyThreshold
	}
	if h.UnhealthyThreshold == 0 {
		h.UnhealthyThreshold = d.UnhealthyThreshold
	}
	if h.DegradedLatency.Duration == 0 {
		h.DegradedLatency = d.DegradedLatency
	}
	return h
}

// loadInto reads `path`, dispatches to the right parser by extension, and
// unmarshals into v.
func loadInto(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("config read failed: %w", err)
	}
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".json":
		if err := json.Unmarshal(data, v); err != nil {
			return fmt.Errorf("json parse failed for %s: %w", path, err)
		}
		return nil
	case ".toml":
		if err := parseTOML(data, v); err != nil {
			return fmt.Errorf("toml parse failed for %s: %w", path, err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported config format %q (want .json or .toml)", ext)
	}
}

// applyProtocolDefaults fills in any zero-valued duration/int fields with
// production defaults. This lets a config file specify only the fields it
// cares about without breaking the gossip layer's invariants.
//
// Lifeguard bounds are also defaulted here. They are still ignored at
// runtime when LifeguardEnabled is false, but providing sensible numbers
// means a user can flip the feature on with a single boolean change.
func applyProtocolDefaults(p ProtocolCfg) ProtocolCfg {
	d := DefaultProtocolCfg()
	if p.Interval.Duration == 0 {
		p.Interval = d.Interval
	}
	if p.PingTimeout.Duration == 0 {
		p.PingTimeout = d.PingTimeout
	}
	if p.IndirectTimeout.Duration == 0 {
		p.IndirectTimeout = d.IndirectTimeout
	}
	if p.SuspectTimeout.Duration == 0 {
		p.SuspectTimeout = d.SuspectTimeout
	}
	if p.IndirectNodes == 0 {
		p.IndirectNodes = d.IndirectNodes
	}
	if p.SelfAwarenessMax == 0 {
		p.SelfAwarenessMax = d.SelfAwarenessMax
	}
	if p.PingTimeoutMin.Duration == 0 {
		p.PingTimeoutMin = d.PingTimeoutMin
	}
	if p.PingTimeoutMax.Duration == 0 {
		p.PingTimeoutMax = d.PingTimeoutMax
	}
	if p.IndirectTimeoutMin.Duration == 0 {
		p.IndirectTimeoutMin = d.IndirectTimeoutMin
	}
	if p.IndirectTimeoutMax.Duration == 0 {
		p.IndirectTimeoutMax = d.IndirectTimeoutMax
	}
	if p.SuspectTimeoutMin.Duration == 0 {
		p.SuspectTimeoutMin = d.SuspectTimeoutMin
	}
	if p.SuspectTimeoutMax.Duration == 0 {
		p.SuspectTimeoutMax = d.SuspectTimeoutMax
	}
	return p
}

func applyLoggingDefaults(l LoggingCfg) LoggingCfg {
	if l.Level == "" {
		l.Level = "info"
	}
	if l.Format == "" {
		l.Format = "json"
	}
	return l
}
