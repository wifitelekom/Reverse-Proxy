package config

import (
	"fmt"
	"strings"
)

// validStrategies lists every strategy the load balancer recognizes today.
var validStrategies = map[string]bool{
	"roundrobin": true,
	"leastconn":  true,
	"p2c":        true,
	"p2c-ewma":   true,
}

// ValidateLB returns nil if the config is internally consistent.
// All errors are collected and returned together so the user sees every
// problem in one shot rather than fixing them one at a time.
func ValidateLB(cfg LBConfig) error {
	var errs []string

	if cfg.Listen == "" {
		errs = append(errs, "listen address must not be empty")
	}
	if cfg.GossipAddr == "" {
		errs = append(errs, "gossip_addr must not be empty")
	}
	if cfg.NodeID == "" {
		errs = append(errs, "node_id must not be empty")
	}
	if cfg.Strategy == "" {
		errs = append(errs, "strategy must not be empty")
	} else if !validStrategies[cfg.Strategy] {
		errs = append(errs, fmt.Sprintf("unknown strategy %q (valid: roundrobin, leastconn, p2c, p2c-ewma)", cfg.Strategy))
	}

	errs = append(errs, validateProtocol(cfg.Protocol)...)
	errs = append(errs, validateLogging(cfg.Logging)...)
	errs = append(errs, validateHealthCheck(cfg.HealthCheck)...)
	errs = append(errs, validateTLS(cfg.TLS)...)
	errs = append(errs, validateBackendTLS(cfg.BackendTLS)...)

	return collect(errs)
}

// ValidateNode returns nil if the node config is internally consistent.
func ValidateNode(cfg NodeConfig) error {
	var errs []string

	if cfg.ID == "" {
		errs = append(errs, "id must not be empty")
	}
	if cfg.GossipAddr == "" {
		errs = append(errs, "gossip_addr must not be empty")
	}
	if cfg.ServiceAddr == "" {
		errs = append(errs, "service_addr must not be empty")
	}

	errs = append(errs, validateProtocol(cfg.Protocol)...)
	errs = append(errs, validateLogging(cfg.Logging)...)

	return collect(errs)
}

func validateProtocol(p ProtocolCfg) []string {
	var errs []string
	if p.IndirectNodes <= 0 {
		errs = append(errs, fmt.Sprintf("protocol.indirect_nodes must be > 0, got %d", p.IndirectNodes))
	}
	if p.PingTimeout.Duration > 0 && p.IndirectTimeout.Duration > 0 &&
		p.PingTimeout.Duration >= p.IndirectTimeout.Duration {
		errs = append(errs,
			fmt.Sprintf("protocol.ping_timeout (%v) must be < indirect_timeout (%v)",
				p.PingTimeout.Duration, p.IndirectTimeout.Duration))
	}
	if p.SuspectTimeout.Duration > 0 && p.IndirectTimeout.Duration > 0 &&
		p.SuspectTimeout.Duration <= p.IndirectTimeout.Duration {
		errs = append(errs,
			fmt.Sprintf("protocol.suspect_timeout (%v) must be > indirect_timeout (%v)",
				p.SuspectTimeout.Duration, p.IndirectTimeout.Duration))
	}
	return errs
}

func validateTLS(t TLSCfg) []string {
	if !t.Enabled {
		return nil
	}
	var errs []string
	if t.CertFile == "" {
		errs = append(errs, "tls.cert_file required when TLS enabled")
	}
	if t.KeyFile == "" {
		errs = append(errs, "tls.key_file required when TLS enabled")
	}
	switch t.MinVersion {
	case "", "1.2", "1.3":
		// OK
	default:
		errs = append(errs, fmt.Sprintf("tls.min_version %q invalid (must be 1.2 or 1.3)", t.MinVersion))
	}
	if t.AutoReload && t.ReloadInterval.Duration <= 0 {
		errs = append(errs, "tls.reload_interval must be > 0 when auto_reload is enabled")
	}
	return errs
}

func validateBackendTLS(b BackendTLSCfg) []string {
	// SkipVerify with Enabled is a smell but not an error — operators
	// legitimately need it in dev/test. The runtime logs a loud warning.
	return nil
}

func validateHealthCheck(h HealthCheckCfg) []string {
	if !h.Enabled {
		return nil
	}
	var errs []string
	if h.Interval.Duration <= 0 {
		errs = append(errs, "health_check.interval must be > 0 when enabled")
	}
	if h.Timeout.Duration <= 0 {
		errs = append(errs, "health_check.timeout must be > 0 when enabled")
	}
	if h.Timeout.Duration >= h.Interval.Duration && h.Interval.Duration > 0 {
		errs = append(errs, fmt.Sprintf(
			"health_check.timeout (%v) must be < interval (%v)",
			h.Timeout.Duration, h.Interval.Duration,
		))
	}
	if h.Path == "" {
		errs = append(errs, "health_check.path must not be empty when enabled")
	}
	if h.UnhealthyThreshold <= 0 {
		errs = append(errs, "health_check.unhealthy_threshold must be > 0 when enabled")
	}
	if h.HealthyThreshold <= 0 {
		errs = append(errs, "health_check.healthy_threshold must be > 0 when enabled")
	}
	return errs
}

func validateLogging(l LoggingCfg) []string {
	var errs []string
	switch l.Level {
	case "", "debug", "info", "warn", "error":
		// ok ("" is fine — defaults will fill it in)
	default:
		errs = append(errs, fmt.Sprintf("logging.level %q invalid (debug|info|warn|error)", l.Level))
	}
	switch l.Format {
	case "", "text", "json":
		// ok
	default:
		errs = append(errs, fmt.Sprintf("logging.format %q invalid (text|json)", l.Format))
	}
	return errs
}

func collect(errs []string) error {
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("config invalid:\n  - %s", strings.Join(errs, "\n  - "))
}
