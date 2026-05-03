package config

import (
	"strings"
	"testing"
	"time"
)

// =====================================================================
// ShutdownTimeout — defaults
// =====================================================================

func TestDefaultLBConfig_ShutdownTimeoutIs30Seconds(t *testing.T) {
	// The default chosen for streaming workloads. If this changes, every
	// downstream consumer that relies on it should be updated together.
	got := DefaultLBConfig().ShutdownTimeout.Duration
	if got != 30*time.Second {
		t.Errorf("default ShutdownTimeout: got %v, want 30s", got)
	}
}

func TestDefaultNodeConfig_ShutdownTimeoutIs30Seconds(t *testing.T) {
	got := DefaultNodeConfig().ShutdownTimeout.Duration
	if got != 30*time.Second {
		t.Errorf("default node ShutdownTimeout: got %v, want 30s", got)
	}
}

// =====================================================================
// Validation: rejects zero and negative
// =====================================================================

func TestValidateLB_RejectsZeroShutdownTimeout(t *testing.T) {
	cfg := DefaultLBConfig()
	cfg.ShutdownTimeout = Duration{0}
	err := ValidateLB(cfg)
	if err == nil {
		t.Fatal("expected validation error for ShutdownTimeout=0")
	}
	if !strings.Contains(err.Error(), "shutdown_timeout") {
		t.Errorf("error should mention shutdown_timeout, got: %v", err)
	}
}

func TestValidateLB_RejectsNegativeShutdownTimeout(t *testing.T) {
	// Negative durations would translate to immediately-canceled contexts —
	// silently breaking graceful shutdown. Catch them.
	cfg := DefaultLBConfig()
	cfg.ShutdownTimeout = Duration{-5 * time.Second}
	if err := ValidateLB(cfg); err == nil {
		t.Fatal("expected validation error for negative ShutdownTimeout")
	}
}

func TestValidateNode_RejectsZeroShutdownTimeout(t *testing.T) {
	cfg := DefaultNodeConfig()
	cfg.ID = "n"
	cfg.GossipAddr = ":9001"
	cfg.ServiceAddr = ":8001"
	cfg.ShutdownTimeout = Duration{0}
	err := ValidateNode(cfg)
	if err == nil {
		t.Fatal("expected validation error for ShutdownTimeout=0")
	}
	if !strings.Contains(err.Error(), "shutdown_timeout") {
		t.Errorf("error should mention shutdown_timeout, got: %v", err)
	}
}

// =====================================================================
// Loader: applies default when file omits the field
// =====================================================================

func TestLoadLB_ShutdownTimeoutDefaultedWhenAbsent(t *testing.T) {
	// A config file that doesn't mention shutdown_timeout should pick up
	// the 30s default — operators upgrading from older configs shouldn't
	// be forced to add a field they didn't know about.
	body := `{"listen": ":9090"}`
	path := writeTemp(t, "minimal.json", body)
	cfg, err := LoadLB(path)
	if err != nil {
		t.Fatalf("LoadLB: %v", err)
	}
	if cfg.ShutdownTimeout.Duration != 30*time.Second {
		t.Errorf("expected 30s default, got %v", cfg.ShutdownTimeout.Duration)
	}
}

func TestLoadLB_ShutdownTimeoutHonorsFileValue(t *testing.T) {
	body := `{"listen": ":9090", "shutdown_timeout": "2m"}`
	path := writeTemp(t, "custom.json", body)
	cfg, err := LoadLB(path)
	if err != nil {
		t.Fatalf("LoadLB: %v", err)
	}
	if cfg.ShutdownTimeout.Duration != 2*time.Minute {
		t.Errorf("file value lost: got %v want 2m", cfg.ShutdownTimeout.Duration)
	}
}

func TestLoadNode_ShutdownTimeoutDefaultedWhenAbsent(t *testing.T) {
	body := `{"id":"n","gossip_addr":":9001","service_addr":":8001"}`
	path := writeTemp(t, "node.json", body)
	cfg, err := LoadNode(path)
	if err != nil {
		t.Fatalf("LoadNode: %v", err)
	}
	if cfg.ShutdownTimeout.Duration != 30*time.Second {
		t.Errorf("node default lost: got %v", cfg.ShutdownTimeout.Duration)
	}
}

// =====================================================================
// Merge: CLI flag overrides file value
// =====================================================================

func TestMergeLB_ShutdownTimeoutFlagOverrides(t *testing.T) {
	cfg := DefaultLBConfig()
	cfg.ShutdownTimeout = Duration{30 * time.Second} // baseline
	out := MergeLB(cfg, LBFlags{ShutdownTimeout: 90 * time.Second})
	if out.ShutdownTimeout.Duration != 90*time.Second {
		t.Errorf("flag override failed: got %v want 90s", out.ShutdownTimeout.Duration)
	}
}

func TestMergeLB_ShutdownTimeoutZeroFlagPreservesFile(t *testing.T) {
	// Zero CLI flag means "not set" — the file value should survive.
	cfg := DefaultLBConfig()
	cfg.ShutdownTimeout = Duration{45 * time.Second}
	out := MergeLB(cfg, LBFlags{ShutdownTimeout: 0})
	if out.ShutdownTimeout.Duration != 45*time.Second {
		t.Errorf("zero-flag clobbered file value: got %v", out.ShutdownTimeout.Duration)
	}
}

func TestMergeNode_ShutdownTimeoutFlagOverrides(t *testing.T) {
	cfg := DefaultNodeConfig()
	cfg.ShutdownTimeout = Duration{30 * time.Second}
	out := MergeNode(cfg, NodeFlags{ShutdownTimeout: 10 * time.Second})
	if out.ShutdownTimeout.Duration != 10*time.Second {
		t.Errorf("flag override failed: got %v", out.ShutdownTimeout.Duration)
	}
}

// =====================================================================
// Env var: P2PLB_SHUTDOWN_TIMEOUT
// =====================================================================

func TestApplyEnvLB_ShutdownTimeoutOverrides(t *testing.T) {
	t.Setenv("P2PLB_SHUTDOWN_TIMEOUT", "15s")
	cfg := DefaultLBConfig()
	cfg = ApplyEnvLB(cfg)
	if cfg.ShutdownTimeout.Duration != 15*time.Second {
		t.Errorf("env override failed: got %v want 15s", cfg.ShutdownTimeout.Duration)
	}
}

func TestApplyEnvLB_ShutdownTimeoutInvalidIgnored(t *testing.T) {
	// Bad values from env shouldn't crash; they should be silently
	// ignored so the existing (file/default) value survives.
	t.Setenv("P2PLB_SHUTDOWN_TIMEOUT", "not-a-duration")
	cfg := DefaultLBConfig()
	cfg = ApplyEnvLB(cfg)
	if cfg.ShutdownTimeout.Duration != 30*time.Second {
		t.Errorf("default lost on bad env: got %v", cfg.ShutdownTimeout.Duration)
	}
}

func TestApplyEnvLB_ShutdownTimeoutNegativeIgnored(t *testing.T) {
	// A negative value parses as a valid duration but is semantically
	// invalid — env loader should reject so subsequent validation passes.
	t.Setenv("P2PLB_SHUTDOWN_TIMEOUT", "-5s")
	cfg := DefaultLBConfig()
	cfg = ApplyEnvLB(cfg)
	if cfg.ShutdownTimeout.Duration != 30*time.Second {
		t.Errorf("default lost on negative env: got %v", cfg.ShutdownTimeout.Duration)
	}
}

// =====================================================================
// ToInternal pass-through
// =====================================================================

func TestLBConfig_ToInternal_PropagatesShutdownTimeout(t *testing.T) {
	// The whole point of the field is to reach types.LBConfig where the
	// LB actually uses it. Verify the pass-through.
	cfg := DefaultLBConfig()
	cfg.ShutdownTimeout = Duration{42 * time.Second}
	out := cfg.ToInternal()
	if out.ShutdownTimeout != 42*time.Second {
		t.Errorf("ShutdownTimeout not propagated to types.LBConfig: got %v", out.ShutdownTimeout)
	}
}
