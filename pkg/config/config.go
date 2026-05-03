// Package config provides file-based configuration loading for nodes and the
// load balancer. It supports JSON natively (zero dependencies) and optionally
// TOML when built with the `toml` build tag.
//
// Precedence (highest to lowest):
//  1. CLI flags
//  2. Environment variables (P2PLB_* prefix)
//  3. Config file
//  4. Built-in defaults
//
// The loader produces neutral structs that can be converted to the existing
// types.ProtocolConfig and types.LBConfig via ToInternal helpers, so the rest
// of the codebase stays unchanged.
package config

import (
	"encoding/json"
	"fmt"
	"time"

	"p2p-loadbalancer/pkg/types"
)

// Duration wraps time.Duration with JSON/TOML support for human-readable
// values like "5s", "500ms". Without this wrapper time.Duration would
// serialize as nanoseconds (an int), which is unreadable in config files.
type Duration struct {
	time.Duration
}

// MarshalJSON serializes as a duration string ("500ms", "1s", ...).
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

// UnmarshalJSON accepts either a duration string ("500ms") or an integer
// number of nanoseconds, matching time.Duration's text behaviour.
func (d *Duration) UnmarshalJSON(b []byte) error {
	// Try string first
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		dur, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		d.Duration = dur
		return nil
	}
	// Fallback: raw integer nanoseconds
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("duration must be a string or integer ns")
	}
	d.Duration = time.Duration(n)
	return nil
}

// MarshalText is used by TOML and other text-based encoders.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

// UnmarshalText is used by TOML and other text-based encoders.
func (d *Duration) UnmarshalText(b []byte) error {
	dur, err := time.ParseDuration(string(b))
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", string(b), err)
	}
	d.Duration = dur
	return nil
}

// ProtocolCfg mirrors types.ProtocolConfig but uses our Duration wrapper for
// human-readable serialization.
//
// Lifeguard fields are optional. When LifeguardEnabled is false (the
// default), the *Min/*Max bounds are ignored and the protocol behaves
// like vanilla SWIM — operators upgrading to a new build see no change
// unless they opt in.
type ProtocolCfg struct {
	Interval        Duration `json:"interval"          toml:"interval"`
	PingTimeout     Duration `json:"ping_timeout"      toml:"ping_timeout"`
	IndirectTimeout Duration `json:"indirect_timeout"  toml:"indirect_timeout"`
	SuspectTimeout  Duration `json:"suspect_timeout"   toml:"suspect_timeout"`
	IndirectNodes   int      `json:"indirect_nodes"    toml:"indirect_nodes"`

	// Lifeguard (HashiCorp's production-hardening of SWIM). Opt-in.
	LifeguardEnabled   bool     `json:"lifeguard_enabled"     toml:"lifeguard_enabled"`
	SelfAwarenessMax   int      `json:"self_awareness_max"    toml:"self_awareness_max"`
	PingTimeoutMin     Duration `json:"ping_timeout_min"      toml:"ping_timeout_min"`
	PingTimeoutMax     Duration `json:"ping_timeout_max"      toml:"ping_timeout_max"`
	IndirectTimeoutMin Duration `json:"indirect_timeout_min"  toml:"indirect_timeout_min"`
	IndirectTimeoutMax Duration `json:"indirect_timeout_max"  toml:"indirect_timeout_max"`
	SuspectTimeoutMin  Duration `json:"suspect_timeout_min"   toml:"suspect_timeout_min"`
	SuspectTimeoutMax  Duration `json:"suspect_timeout_max"   toml:"suspect_timeout_max"`
}

// LoggingCfg controls slog output.
type LoggingCfg struct {
	Level  string `json:"level"  toml:"level"`  // debug, info, warn, error
	Format string `json:"format" toml:"format"` // text, json
}

// TLSCfg is the file-loadable counterpart of types.TLSConfig.
type TLSCfg struct {
	Enabled        bool     `json:"enabled"         toml:"enabled"`
	CertFile       string   `json:"cert_file"       toml:"cert_file"`
	KeyFile        string   `json:"key_file"        toml:"key_file"`
	MinVersion     string   `json:"min_version"     toml:"min_version"`
	ClientCAFile   string   `json:"client_ca_file"  toml:"client_ca_file"`
	AutoReload     bool     `json:"auto_reload"     toml:"auto_reload"`
	ReloadInterval Duration `json:"reload_interval" toml:"reload_interval"`
}

// BackendTLSCfg is the file-loadable counterpart of types.BackendTLSConfig.
type BackendTLSCfg struct {
	Enabled    bool   `json:"enabled"     toml:"enabled"`
	SkipVerify bool   `json:"skip_verify" toml:"skip_verify"`
	CAFile     string `json:"ca_file"     toml:"ca_file"`
}

// HealthCheckCfg is the file-loadable counterpart of types.HealthCheckConfig,
// using the human-readable Duration wrapper.
type HealthCheckCfg struct {
	Enabled            bool     `json:"enabled"             toml:"enabled"`
	Interval           Duration `json:"interval"            toml:"interval"`
	Timeout            Duration `json:"timeout"             toml:"timeout"`
	Path               string   `json:"path"                toml:"path"`
	HealthyThreshold   int      `json:"healthy_threshold"   toml:"healthy_threshold"`
	UnhealthyThreshold int      `json:"unhealthy_threshold" toml:"unhealthy_threshold"`
	DegradedLatency    Duration `json:"degraded_latency"    toml:"degraded_latency"`
}

// NodeConfig is the file-loadable config for a backend node.
type NodeConfig struct {
	ID          string      `json:"id"           toml:"id"`
	GossipAddr  string      `json:"gossip_addr"  toml:"gossip_addr"`
	ServiceAddr string      `json:"service_addr" toml:"service_addr"`
	SeedNodes   []string    `json:"seed_nodes"   toml:"seed_nodes"`
	Protocol    ProtocolCfg `json:"protocol"     toml:"protocol"`
	Logging     LoggingCfg  `json:"logging"      toml:"logging"`
}

// LBConfig is the file-loadable config for the load balancer.
type LBConfig struct {
	Listen      string         `json:"listen"       toml:"listen"`
	Strategy    string         `json:"strategy"     toml:"strategy"`
	GossipAddr  string         `json:"gossip_addr"  toml:"gossip_addr"`
	NodeID      string         `json:"node_id"      toml:"node_id"`
	SeedNodes   []string       `json:"seed_nodes"   toml:"seed_nodes"`
	Protocol    ProtocolCfg    `json:"protocol"     toml:"protocol"`
	HealthCheck HealthCheckCfg `json:"health_check" toml:"health_check"`
	TLS         TLSCfg         `json:"tls"          toml:"tls"`
	BackendTLS  BackendTLSCfg  `json:"backend_tls"  toml:"backend_tls"`
	Logging     LoggingCfg     `json:"logging"      toml:"logging"`
}

// DefaultProtocolCfg returns production-safe SWIM defaults.
// These match types.DefaultConfig() — kept in sync intentionally so a config
// file with no `protocol` section behaves identically to the old CLI defaults.
func DefaultProtocolCfg() ProtocolCfg {
	return ProtocolCfg{
		Interval:        Duration{1 * time.Second},
		PingTimeout:     Duration{500 * time.Millisecond},
		IndirectTimeout: Duration{1 * time.Second},
		SuspectTimeout:  Duration{3 * time.Second},
		IndirectNodes:   3,

		// Lifeguard defaults — applied even when disabled, so opting in is
		// a single boolean flip without having to re-specify every bound.
		LifeguardEnabled:   false,
		SelfAwarenessMax:   8,
		PingTimeoutMin:     Duration{500 * time.Millisecond},
		PingTimeoutMax:     Duration{4 * time.Second},
		IndirectTimeoutMin: Duration{1 * time.Second},
		IndirectTimeoutMax: Duration{8 * time.Second},
		SuspectTimeoutMin:  Duration{3 * time.Second},
		SuspectTimeoutMax:  Duration{24 * time.Second},
	}
}

// DefaultLoggingCfg returns the default logging configuration.
func DefaultLoggingCfg() LoggingCfg {
	return LoggingCfg{Level: "info", Format: "json"}
}

// DefaultTLSCfg returns the disabled-by-default frontend TLS config.
func DefaultTLSCfg() TLSCfg {
	return TLSCfg{
		Enabled:        false,
		MinVersion:     "1.2",
		ReloadInterval: Duration{1 * time.Hour},
	}
}

// DefaultBackendTLSCfg returns the disabled-by-default backend TLS config.
func DefaultBackendTLSCfg() BackendTLSCfg {
	return BackendTLSCfg{Enabled: false}
}

// DefaultHealthCheckCfg returns the disabled-by-default configuration. The
// values mirror healthcheck.DefaultConfig() so a single boolean flip is
// enough to enable the feature with sane numbers.
func DefaultHealthCheckCfg() HealthCheckCfg {
	return HealthCheckCfg{
		Enabled:            false,
		Interval:           Duration{5 * time.Second},
		Timeout:            Duration{2 * time.Second},
		Path:               "/health",
		HealthyThreshold:   2,
		UnhealthyThreshold: 3,
		DegradedLatency:    Duration{1 * time.Second},
	}
}

// DefaultNodeConfig returns a fresh NodeConfig with default protocol/logging.
// ID, GossipAddr and ServiceAddr have no defaults — they must be supplied.
func DefaultNodeConfig() NodeConfig {
	return NodeConfig{
		Protocol: DefaultProtocolCfg(),
		Logging:  DefaultLoggingCfg(),
	}
}

// DefaultLBConfig returns a fresh LBConfig with default values matching the
// previous CLI defaults (":8080", "roundrobin", ":9000", "loadbalancer").
func DefaultLBConfig() LBConfig {
	return LBConfig{
		Listen:      ":8080",
		Strategy:    "roundrobin",
		GossipAddr:  ":9000",
		NodeID:      "loadbalancer",
		Protocol:    DefaultProtocolCfg(),
		HealthCheck: DefaultHealthCheckCfg(),
		TLS:         DefaultTLSCfg(),
		BackendTLS:  DefaultBackendTLSCfg(),
		Logging:     DefaultLoggingCfg(),
	}
}

// ToInternalProtocol converts a ProtocolCfg + identity fields into the
// existing types.ProtocolConfig used by the gossip layer.
func (p ProtocolCfg) ToInternalProtocol(nodeID, gossipAddr, serviceAddr string, seeds []string) types.ProtocolConfig {
	return types.ProtocolConfig{
		ProtocolInterval: p.Interval.Duration,
		PingTimeout:      p.PingTimeout.Duration,
		IndirectTimeout:  p.IndirectTimeout.Duration,
		SuspectTimeout:   p.SuspectTimeout.Duration,
		IndirectNodes:    p.IndirectNodes,
		GossipAddr:       gossipAddr,
		ServiceAddr:      serviceAddr,
		NodeID:           nodeID,
		SeedNodes:        seeds,

		LifeguardEnabled:   p.LifeguardEnabled,
		SelfAwarenessMax:   p.SelfAwarenessMax,
		PingTimeoutMin:     p.PingTimeoutMin.Duration,
		PingTimeoutMax:     p.PingTimeoutMax.Duration,
		IndirectTimeoutMin: p.IndirectTimeoutMin.Duration,
		IndirectTimeoutMax: p.IndirectTimeoutMax.Duration,
		SuspectTimeoutMin:  p.SuspectTimeoutMin.Duration,
		SuspectTimeoutMax:  p.SuspectTimeoutMax.Duration,
	}
}

// ToInternal converts an LBConfig to the existing types.LBConfig.
func (c LBConfig) ToInternal() types.LBConfig {
	return types.LBConfig{
		ListenAddr:  c.Listen,
		Strategy:    c.Strategy,
		GossipAddr:  c.GossipAddr,
		NodeID:      c.NodeID,
		SeedNodes:   c.SeedNodes,
		Protocol:    c.Protocol.ToInternalProtocol(c.NodeID, c.GossipAddr, "", c.SeedNodes),
		HealthCheck: c.HealthCheck.ToInternal(),
		TLS:         c.TLS.ToInternal(),
		BackendTLS:  c.BackendTLS.ToInternal(),
	}
}

// ToInternal converts TLSCfg to the runtime types.TLSConfig.
func (t TLSCfg) ToInternal() types.TLSConfig {
	return types.TLSConfig{
		Enabled:        t.Enabled,
		CertFile:       t.CertFile,
		KeyFile:        t.KeyFile,
		MinVersion:     t.MinVersion,
		ClientCAFile:   t.ClientCAFile,
		AutoReload:     t.AutoReload,
		ReloadInterval: t.ReloadInterval.Duration,
	}
}

// ToInternal converts BackendTLSCfg to the runtime types.BackendTLSConfig.
func (b BackendTLSCfg) ToInternal() types.BackendTLSConfig {
	return types.BackendTLSConfig{
		Enabled:    b.Enabled,
		SkipVerify: b.SkipVerify,
		CAFile:     b.CAFile,
	}
}

// ToInternal converts a HealthCheckCfg to the runtime types.HealthCheckConfig.
func (h HealthCheckCfg) ToInternal() types.HealthCheckConfig {
	return types.HealthCheckConfig{
		Enabled:            h.Enabled,
		Interval:           h.Interval.Duration,
		Timeout:            h.Timeout.Duration,
		Path:               h.Path,
		HealthyThreshold:   h.HealthyThreshold,
		UnhealthyThreshold: h.UnhealthyThreshold,
		DegradedLatency:    h.DegradedLatency.Duration,
	}
}

// ToInternal converts a NodeConfig to the existing types.ProtocolConfig.
// (Backend nodes don't have a separate top-level config struct; they use
// ProtocolConfig directly.)
func (c NodeConfig) ToInternal() types.ProtocolConfig {
	return c.Protocol.ToInternalProtocol(c.ID, c.GossipAddr, c.ServiceAddr, c.SeedNodes)
}
