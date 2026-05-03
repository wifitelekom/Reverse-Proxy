package types

import (
	"fmt"
	"time"
)

// ProtocolConfig SWIM protokol parametrelerini tutar.
// Tüm timing değerleri buradan merkezi olarak kontrol edilir.
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
	ProtocolInterval time.Duration // T_protocol: gossip döngü aralığı
	PingTimeout      time.Duration // T_ping: direkt ping timeout
	IndirectTimeout  time.Duration // T_indirect: dolaylı ping timeout
	SuspectTimeout   time.Duration // T_suspect: suspect → dead geçiş süresi

	IndirectNodes int // K: dolaylı ping için kullanılan aracı sayısı

	GossipAddr  string // Bu node'un UDP gossip adresi (örn: ":9001")
	ServiceAddr string // Bu node'un HTTP servis adresi (örn: ":8001")
	NodeID      string // Benzersiz node kimliği

	SeedNodes []string // Bootstrap seed node'ları (gossip UDP adresleri)

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

// DefaultConfig üretim ortamı için varsayılan SWIM parametrelerini döner.
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

// Validate konfigürasyon tutarsızlıklarını erken yakalar.
// Servis başlamadan önce çağrılmalı.
func (c ProtocolConfig) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("NodeID boş olamaz")
	}
	if c.GossipAddr == "" {
		return fmt.Errorf("GossipAddr boş olamaz")
	}
	if c.IndirectNodes <= 0 {
		return fmt.Errorf("IndirectNodes > 0 olmalı, alınan: %d", c.IndirectNodes)
	}
	if c.PingTimeout >= c.IndirectTimeout {
		return fmt.Errorf("PingTimeout (%v) < IndirectTimeout (%v) olmalı", c.PingTimeout, c.IndirectTimeout)
	}
	if c.SuspectTimeout <= c.IndirectTimeout {
		return fmt.Errorf("SuspectTimeout (%v) > IndirectTimeout (%v) olmalı", c.SuspectTimeout, c.IndirectTimeout)
	}
	if c.LifeguardEnabled {
		if c.SelfAwarenessMax <= 0 {
			return fmt.Errorf("Lifeguard: SelfAwarenessMax must be > 0, got %d", c.SelfAwarenessMax)
		}
		if c.PingTimeoutMin > c.PingTimeoutMax {
			return fmt.Errorf("Lifeguard: PingTimeoutMin (%v) > PingTimeoutMax (%v)", c.PingTimeoutMin, c.PingTimeoutMax)
		}
		if c.IndirectTimeoutMin > c.IndirectTimeoutMax {
			return fmt.Errorf("Lifeguard: IndirectTimeoutMin (%v) > IndirectTimeoutMax (%v)", c.IndirectTimeoutMin, c.IndirectTimeoutMax)
		}
		if c.SuspectTimeoutMin > c.SuspectTimeoutMax {
			return fmt.Errorf("Lifeguard: SuspectTimeoutMin (%v) > SuspectTimeoutMax (%v)", c.SuspectTimeoutMin, c.SuspectTimeoutMax)
		}
		if c.PingTimeoutMin >= c.IndirectTimeoutMin {
			return fmt.Errorf("Lifeguard: PingTimeoutMin (%v) must be < IndirectTimeoutMin (%v)", c.PingTimeoutMin, c.IndirectTimeoutMin)
		}
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

// LBConfig load balancer yapılandırmasını tutar.
type LBConfig struct {
	ListenAddr  string // LB'nin HTTP dinleme adresi (örn: ":8080")
	Strategy    string // "roundrobin" | "leastconn" | "p2c" | "p2c-ewma"
	GossipAddr  string // LB'nin UDP gossip adresi (örn: ":9000")
	NodeID      string // LB'nin gossip node ID'si
	SeedNodes   []string
	Protocol    ProtocolConfig
	HealthCheck HealthCheckConfig
	TLS         TLSConfig
	BackendTLS  BackendTLSConfig
}
