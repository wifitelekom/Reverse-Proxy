package types

import (
	"fmt"
	"time"
)

// ProtocolConfig holds the SWIM protocol parameters.
// All timing values are controlled centrally from here.
//
// Lifeguard extensions (HashiCorp's production-hardening of SWIM, see
// arXiv 1707.00788) are gated behind LifeguardEnabled. When enabled:
//   - Self-Awareness scales ping/indirect timeouts based on local health
//   - Dogpile shortens the suspect timer as independent confirmations arrive
//   - Buddy System sends suspect notifications directly to the suspected node
//
// When LifeguardEnabled is false the protocol behaves exactly like vanilla
// SWIM (the historical default), so existing tests remain valid.
type ProtocolConfig struct {
	ProtocolInterval time.Duration // T_protocol: gossip loop interval
	PingTimeout      time.Duration // T_ping: direct ping timeout
	IndirectTimeout  time.Duration // T_indirect: indirect ping timeout
	SuspectTimeout   time.Duration // T_suspect: suspect → dead transition window

	IndirectNodes int // K: number of intermediaries used for indirect probing

	GossipAddr  string // this node's UDP gossip address (e.g. ":9001")
	ServiceAddr string // this node's HTTP service address (e.g. ":8001")
	NodeID      string // unique node identifier

	SeedNodes []string // bootstrap seed nodes (gossip UDP addresses)

	// Lifeguard extensions — opt-in. When false, the protocol behaves
	// identically to vanilla SWIM and the *Min/*Max bounds are ignored.
	LifeguardEnabled bool

	// SelfAwarenessMax is the maximum value of the noop counter that drives
	// timeout scaling. A higher cap allows longer maximum timeouts under
	// stress but slows recovery. Sensible range: 4–16. Default: 8.
	SelfAwarenessMax int

	// PingTimeoutMin/Max bracket the dynamically scaled direct ping timeout.
	// At noop=0 the timeout is Min (= PingTimeout by default); at noop=Max
	// it grows to PingTimeoutMax.
	PingTimeoutMin time.Duration
	PingTimeoutMax time.Duration

	// IndirectTimeoutMin/Max bracket the indirect ping timeout the same way.
	IndirectTimeoutMin time.Duration
	IndirectTimeoutMax time.Duration

	// SuspectTimeoutMin/Max bracket the Dogpile suspect timer. Initial value
	// is Max; each independent confirmation pulls it logarithmically down
	// toward Min.
	SuspectTimeoutMin time.Duration
	SuspectTimeoutMax time.Duration
}

// DefaultConfig returns the production-ready default SWIM parameters.
// Lifeguard is OFF by default so the protocol stays bit-for-bit identical
// to its previous behaviour. Operators opt in via LifeguardEnabled=true.
func DefaultConfig() ProtocolConfig {
	return ProtocolConfig{
		ProtocolInterval: 1 * time.Second,
		PingTimeout:      500 * time.Millisecond,
		IndirectTimeout:  1 * time.Second,
		SuspectTimeout:   3 * time.Second,
		IndirectNodes:    3,

		// Lifeguard defaults — these are read only when LifeguardEnabled.
		// 8x scaling factor follows the recommendation in §3 of the paper.
		LifeguardEnabled:   false,
		SelfAwarenessMax:   8,
		PingTimeoutMin:     500 * time.Millisecond,
		PingTimeoutMax:     4 * time.Second,
		IndirectTimeoutMin: 1 * time.Second,
		IndirectTimeoutMax: 8 * time.Second,
		SuspectTimeoutMin:  3 * time.Second,
		SuspectTimeoutMax:  24 * time.Second,
	}
}

// Validate catches configuration inconsistencies early.
// Must be called before the service starts.
func (c ProtocolConfig) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("NodeID must not be empty")
	}
	if c.GossipAddr == "" {
		return fmt.Errorf("GossipAddr must not be empty")
	}
	if c.IndirectNodes <= 0 {
		return fmt.Errorf("IndirectNodes must be > 0, got %d", c.IndirectNodes)
	}
	if c.PingTimeout >= c.IndirectTimeout {
		return fmt.Errorf("PingTimeout (%v) must be < IndirectTimeout (%v)", c.PingTimeout, c.IndirectTimeout)
	}
	if c.SuspectTimeout <= c.IndirectTimeout {
		return fmt.Errorf("SuspectTimeout (%v) must be > IndirectTimeout (%v)", c.SuspectTimeout, c.IndirectTimeout)
	}

	// Lifeguard *Min/*Max bound consistency is validated unconditionally —
	// not just when LifeguardEnabled is true. The reasoning: an inverted
	// range (Min > Max) is never a valid configuration regardless of the
	// master switch. Catching it now means flipping LifeguardEnabled from
	// false to true at runtime can never surface a stale misconfiguration.
	// Same logic applies to the cross-field ordering between Ping and
	// Indirect timeouts.
	if c.PingTimeoutMin > c.PingTimeoutMax {
		return fmt.Errorf("PingTimeoutMin (%v) > PingTimeoutMax (%v) — inverted range",
			c.PingTimeoutMin, c.PingTimeoutMax)
	}
	if c.IndirectTimeoutMin > c.IndirectTimeoutMax {
		return fmt.Errorf("IndirectTimeoutMin (%v) > IndirectTimeoutMax (%v) — inverted range",
			c.IndirectTimeoutMin, c.IndirectTimeoutMax)
	}
	if c.SuspectTimeoutMin > c.SuspectTimeoutMax {
		return fmt.Errorf("SuspectTimeoutMin (%v) > SuspectTimeoutMax (%v) — inverted range",
			c.SuspectTimeoutMin, c.SuspectTimeoutMax)
	}
	// Cross-field ordering is only checked when both bounds are set, so a
	// half-configured ProtocolConfig (e.g. one constructed manually
	// without DefaultConfig) doesn't false-positive on zero values.
	if c.PingTimeoutMin > 0 && c.IndirectTimeoutMin > 0 &&
		c.PingTimeoutMin >= c.IndirectTimeoutMin {
		return fmt.Errorf("PingTimeoutMin (%v) must be < IndirectTimeoutMin (%v)",
			c.PingTimeoutMin, c.IndirectTimeoutMin)
	}

	// SelfAwarenessMax > 0 is only meaningful when Lifeguard is enabled —
	// the value is read solely by SelfAwareness.ScaleTimeout, which never
	// runs unless LifeguardEnabled is true. Operators with the feature off
	// shouldn't be forced to set this field.
	if c.LifeguardEnabled && c.SelfAwarenessMax <= 0 {
		return fmt.Errorf("Lifeguard: SelfAwarenessMax must be > 0, got %d", c.SelfAwarenessMax)
	}
	return nil
}

// TLSConfig configures the optional HTTPS listener for client→LB traffic.
// When Enabled is false (the default) the LB serves plain HTTP exactly as
// before. When true, both CertFile and KeyFile must be supplied.
//
// MinVersion accepts "1.2" or "1.3" — modern TLS only; 1.0/1.1 are
// intentionally not exposed because they have known vulnerabilities and
// are deprecated by every major browser. Default: "1.2".
//
// mTLS (mutual TLS) is enabled by setting ClientCAFile. Clients without a
// certificate signed by that CA are rejected at handshake time.
//
// AutoReload: when true, the LB periodically re-reads the cert/key from
// disk so a new certificate (e.g. from cert-manager) is picked up without
// a process restart. ReloadInterval defaults to 1 hour.
type TLSConfig struct {
	Enabled        bool
	CertFile       string
	KeyFile        string
	MinVersion     string // "1.2" or "1.3"
	ClientCAFile   string // mTLS — empty means mTLS off
	AutoReload     bool
	ReloadInterval time.Duration
}

// BackendTLSConfig configures the optional LB→backend HTTPS leg.
// Independent from frontend TLS: a load balancer can terminate TLS at the
// edge and re-encrypt to the backends, or do straight pass-through, or
// neither.
//
// SkipVerify disables certificate validation — useful for self-signed
// certs in dev/test, dangerous in production. The validator will log a
// warning when it's true with Enabled.
//
// CAFile points at a PEM bundle of additional trusted CAs for backend
// certs. Empty means "use the system trust store".
type BackendTLSConfig struct {
	Enabled    bool
	SkipVerify bool
	CAFile     string
}

// HealthCheckConfig configures the optional HTTP-level checker that runs
// alongside SWIM gossip. When Enabled is false (the default) the
// LoadBalancer behaves exactly as before — every gossip-alive node is
// eligible for traffic, no probes are sent.
type HealthCheckConfig struct {
	Enabled            bool          // master switch
	Interval           time.Duration // tick rate (default 5s)
	Timeout            time.Duration // per-request timeout (default 2s)
	Path               string        // probed URL path (default "/health")
	HealthyThreshold   int           // consecutive successes to leave Unhealthy
	UnhealthyThreshold int           // consecutive failures to enter Unhealthy
	DegradedLatency    time.Duration // 2xx above this counts as Degraded
}

// AdminConfig restricts who can reach the LB's /_lb/* admin endpoints
// (health dump, metrics scrape). Both controls are off by default —
// preserving the historical "anyone on the listen port can scrape"
// behaviour — and any combination is valid:
//
//   - AllowCIDRs only: source IP must fall in one of the listed CIDRs.
//   - BearerToken only: request must carry `Authorization: Bearer <t>`.
//   - Both set: ALL configured controls must pass (defense in depth).
//   - Neither set: open access (back-compat).
//
// Practical guidance: behind nginx (the deployment shape the README
// recommends) the only client reaching the LB on the data port IS
// nginx — set AllowCIDRs to nginx's IP and the metrics scraper's IP,
// or front the admin paths off nginx's auth tier and rely on token.
//
// Note: AllowCIDRs is matched against r.RemoteAddr. If a reverse
// proxy fronts the LB, RemoteAddr is the proxy's address, not the
// real client's — set CIDRs accordingly. We deliberately do NOT
// honour X-Forwarded-For for admin auth: the trust chain there is
// fragile and getting it wrong silently bypasses access control.
type AdminConfig struct {
	AllowCIDRs  []string // CIDR allowlist matched against r.RemoteAddr
	BearerToken string   // shared secret in `Authorization: Bearer ...`
}

// LBConfig holds the load balancer configuration.
//
// ShutdownTimeout bounds graceful shutdown when the parent context is
// canceled. The previous hardcoded 5s was actively unsafe alongside the
// streaming features we ship: gRPC server-streaming RPCs and long-lived
// WebSocket sessions get their connections forcibly closed at the 5s
// mark even when they're making progress. Default 30s is generous
// enough for most streaming workloads but not infinite — operators
// should tune based on their typical request lifetime. Must be > 0.
type LBConfig struct {
	ListenAddr      string // LB's HTTP listen address (e.g. ":8080")
	Strategy        string // "roundrobin" | "leastconn" | "p2c" | "p2c-ewma"
	GossipAddr      string // LB's UDP gossip address (e.g. ":9000")
	NodeID          string // LB's gossip node ID
	SeedNodes       []string
	ShutdownTimeout time.Duration // graceful shutdown budget; default 30s
	Protocol        ProtocolConfig
	HealthCheck     HealthCheckConfig
	TLS             TLSConfig
	BackendTLS      BackendTLSConfig
	Admin           AdminConfig
}
