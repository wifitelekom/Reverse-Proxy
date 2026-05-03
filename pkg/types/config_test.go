package types

import (
	"strings"
	"testing"
	"time"
)

// validBaseConfig returns a ProtocolConfig with the minimum identity
// fields populated and DefaultConfig timing values. Tests start from
// this and mutate one field per case so the failure attributable to
// that field is unambiguous.
func validBaseConfig() ProtocolConfig {
	c := DefaultConfig()
	c.NodeID = "node-x"
	c.GossipAddr = ":9001"
	return c
}

// =====================================================================
// Defaults & happy path
// =====================================================================

func TestValidate_DefaultConfigWithIdentityFieldsPasses(t *testing.T) {
	if err := validBaseConfig().Validate(); err != nil {
		t.Fatalf("default + identity should validate, got: %v", err)
	}
}

func TestValidate_DefaultConfigWithLifeguardEnabledPasses(t *testing.T) {
	// DefaultConfig sets all the *Min/*Max bounds to coherent values, so
	// a single boolean flip should be enough to enable Lifeguard cleanly.
	c := validBaseConfig()
	c.LifeguardEnabled = true
	if err := c.Validate(); err != nil {
		t.Fatalf("default + Lifeguard should validate, got: %v", err)
	}
}

// =====================================================================
// Existing invariants (regression — touched the validator, must not break)
// =====================================================================

func TestValidate_RejectsEmptyNodeID(t *testing.T) {
	c := validBaseConfig()
	c.NodeID = ""
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for empty NodeID")
	}
}

func TestValidate_RejectsEmptyGossipAddr(t *testing.T) {
	c := validBaseConfig()
	c.GossipAddr = ""
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for empty GossipAddr")
	}
}

func TestValidate_RejectsPingTimeoutGEIndirectTimeout(t *testing.T) {
	c := validBaseConfig()
	c.PingTimeout = 2 * time.Second
	c.IndirectTimeout = 1 * time.Second
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "PingTimeout") {
		t.Errorf("expected PingTimeout/IndirectTimeout ordering error, got: %v", err)
	}
}

// =====================================================================
// H-7: Lifeguard bounds validated unconditionally
// =====================================================================

func TestValidate_RejectsInvertedPingMinMax_EvenWhenLifeguardDisabled(t *testing.T) {
	// The whole point of H-7: an inverted range is never valid, regardless
	// of the master switch. Operator might have Lifeguard off today but
	// flip it on tomorrow; we want validation to catch the misconfig now.
	c := validBaseConfig()
	c.LifeguardEnabled = false // explicitly disabled
	c.PingTimeoutMin = 4 * time.Second
	c.PingTimeoutMax = 500 * time.Millisecond // inverted

	err := c.Validate()
	if err == nil {
		t.Fatal("expected validation error for inverted PingTimeoutMin/Max")
	}
	if !strings.Contains(err.Error(), "PingTimeoutMin") {
		t.Errorf("error message should mention PingTimeoutMin, got: %v", err)
	}
	if !strings.Contains(err.Error(), "inverted") {
		t.Errorf("error message should mention 'inverted' for clarity, got: %v", err)
	}
}

func TestValidate_RejectsInvertedIndirectMinMax_EvenWhenLifeguardDisabled(t *testing.T) {
	c := validBaseConfig()
	c.LifeguardEnabled = false
	c.IndirectTimeoutMin = 8 * time.Second
	c.IndirectTimeoutMax = 1 * time.Second // inverted

	err := c.Validate()
	if err == nil {
		t.Fatal("expected validation error for inverted IndirectTimeoutMin/Max")
	}
	if !strings.Contains(err.Error(), "IndirectTimeout") {
		t.Errorf("error should mention IndirectTimeout, got: %v", err)
	}
}

func TestValidate_RejectsInvertedSuspectMinMax_EvenWhenLifeguardDisabled(t *testing.T) {
	c := validBaseConfig()
	c.LifeguardEnabled = false
	c.SuspectTimeoutMin = 24 * time.Second
	c.SuspectTimeoutMax = 3 * time.Second // inverted

	err := c.Validate()
	if err == nil {
		t.Fatal("expected validation error for inverted SuspectTimeoutMin/Max")
	}
	if !strings.Contains(err.Error(), "SuspectTimeout") {
		t.Errorf("error should mention SuspectTimeout, got: %v", err)
	}
}

func TestValidate_RejectsInvertedRangesWithLifeguardEnabledToo(t *testing.T) {
	// Sanity: the same inversion fails when Lifeguard is on. Belt-and-suspenders.
	c := validBaseConfig()
	c.LifeguardEnabled = true
	c.PingTimeoutMin = 4 * time.Second
	c.PingTimeoutMax = 500 * time.Millisecond
	if err := c.Validate(); err == nil {
		t.Fatal("expected validation error with Lifeguard enabled too")
	}
}

func TestValidate_RejectsPingMinGECrossingIndirectMin(t *testing.T) {
	// Same cross-field ordering invariant as PingTimeout/IndirectTimeout,
	// but for the Lifeguard scaling bounds. Always enforced when both
	// values are set.
	c := validBaseConfig()
	c.LifeguardEnabled = false
	c.PingTimeoutMin = 2 * time.Second
	c.IndirectTimeoutMin = 1 * time.Second // ping >= indirect
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for PingTimeoutMin >= IndirectTimeoutMin")
	}
	if !strings.Contains(err.Error(), "PingTimeoutMin") {
		t.Errorf("error should mention PingTimeoutMin, got: %v", err)
	}
}

func TestValidate_AcceptsZeroBoundsWhenHalfConfigured(t *testing.T) {
	// A ProtocolConfig built manually (without DefaultConfig) might leave
	// the Lifeguard fields at zero. With Lifeguard off this is fine —
	// the validator must not false-positive on the cross-field ordering
	// rule when one side is zero.
	c := ProtocolConfig{
		NodeID:           "n",
		GossipAddr:       ":9001",
		ProtocolInterval: 1 * time.Second,
		PingTimeout:      500 * time.Millisecond,
		IndirectTimeout:  1 * time.Second,
		SuspectTimeout:   3 * time.Second,
		IndirectNodes:    3,
		// All Lifeguard fields zero, LifeguardEnabled false.
	}
	if err := c.Validate(); err != nil {
		t.Errorf("zero Lifeguard fields should be acceptable when feature off; got %v", err)
	}
}

// =====================================================================
// SelfAwarenessMax: only checked when Lifeguard is enabled
// =====================================================================

func TestValidate_SelfAwarenessZeroAllowedWhenLifeguardDisabled(t *testing.T) {
	// SelfAwarenessMax is only ever read when LifeguardEnabled is true.
	// Forcing it to >0 with the feature off would be needless friction.
	c := validBaseConfig()
	c.LifeguardEnabled = false
	c.SelfAwarenessMax = 0
	if err := c.Validate(); err != nil {
		t.Errorf("SelfAwarenessMax=0 should be ok with Lifeguard off; got: %v", err)
	}
}

func TestValidate_RejectsSelfAwarenessZeroWhenLifeguardEnabled(t *testing.T) {
	c := validBaseConfig()
	c.LifeguardEnabled = true
	c.SelfAwarenessMax = 0
	err := c.Validate()
	if err == nil {
		t.Fatal("SelfAwarenessMax=0 with Lifeguard enabled should fail")
	}
	if !strings.Contains(err.Error(), "SelfAwarenessMax") {
		t.Errorf("error should mention SelfAwarenessMax, got: %v", err)
	}
}
