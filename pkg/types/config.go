package types

import (
	"fmt"
	"time"
)

// ProtocolConfig SWIM protokol parametrelerini tutar.
// Tüm timing değerleri buradan merkezi olarak kontrol edilir.
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
}

// DefaultConfig üretim ortamı için varsayılan SWIM parametrelerini döner.
func DefaultConfig() ProtocolConfig {
	return ProtocolConfig{
		ProtocolInterval: 1 * time.Second,
		PingTimeout:      500 * time.Millisecond,
		IndirectTimeout:  1 * time.Second,
		SuspectTimeout:   3 * time.Second,
		IndirectNodes:    3,
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
	return nil
}

// LBConfig load balancer yapılandırmasını tutar.
type LBConfig struct {
	ListenAddr string // LB'nin HTTP dinleme adresi (örn: ":8080")
	Strategy   string // "roundrobin" | "leastconn"
	GossipAddr string // LB'nin UDP gossip adresi (örn: ":9000")
	NodeID     string // LB'nin gossip node ID'si
	SeedNodes  []string
	Protocol   ProtocolConfig
}
