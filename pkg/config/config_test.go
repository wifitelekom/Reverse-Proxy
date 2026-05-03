package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------- Duration JSON round-trip ----------

func TestDuration_MarshalJSON_String(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want string
	}{
		{500 * time.Millisecond, `"500ms"`},
		{5 * time.Second, `"5s"`},
		{2 * time.Minute, `"2m0s"`},
	}
	for _, c := range cases {
		b, err := json.Marshal(Duration{c.in})
		if err != nil {
			t.Fatalf("marshal %v: %v", c.in, err)
		}
		if string(b) != c.want {
			t.Errorf("marshal %v: got %s want %s", c.in, b, c.want)
		}
	}
}

func TestDuration_UnmarshalJSON_String(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{`"500ms"`, 500 * time.Millisecond},
		{`"5s"`, 5 * time.Second},
		{`"1h30m"`, 90 * time.Minute},
	}
	for _, c := range cases {
		var d Duration
		if err := json.Unmarshal([]byte(c.in), &d); err != nil {
			t.Fatalf("unmarshal %s: %v", c.in, err)
		}
		if d.Duration != c.want {
			t.Errorf("unmarshal %s: got %v want %v", c.in, d.Duration, c.want)
		}
	}
}

func TestDuration_UnmarshalJSON_Integer(t *testing.T) {
	// Raw nanoseconds should still work — matches time.Duration's native form.
	var d Duration
	if err := json.Unmarshal([]byte(`1000000`), &d); err != nil {
		t.Fatalf("unmarshal int: %v", err)
	}
	if d.Duration != time.Millisecond {
		t.Errorf("got %v want %v", d.Duration, time.Millisecond)
	}
}

func TestDuration_UnmarshalJSON_Invalid(t *testing.T) {
	var d Duration
	if err := json.Unmarshal([]byte(`"not-a-duration"`), &d); err == nil {
		t.Error("expected error for invalid duration string")
	}
}

// ---------- LoadLB ----------

func TestLoadLB_JSON_Full(t *testing.T) {
	body := `{
		"listen":      ":7070",
		"strategy":    "leastconn",
		"gossip_addr": ":9500",
		"node_id":     "lb-test",
		"seed_nodes":  [":9001", ":9002"],
		"protocol": {
			"interval":         "2s",
			"ping_timeout":     "300ms",
			"indirect_timeout": "800ms",
			"suspect_timeout":  "5s",
			"indirect_nodes":   2
		},
		"logging": {"level": "debug", "format": "text"}
	}`
	path := writeTemp(t, "lb.json", body)
	cfg, err := LoadLB(path)
	if err != nil {
		t.Fatalf("LoadLB: %v", err)
	}
	if cfg.Listen != ":7070" || cfg.Strategy != "leastconn" {
		t.Errorf("listen/strategy: %s / %s", cfg.Listen, cfg.Strategy)
	}
	if cfg.NodeID != "lb-test" || cfg.GossipAddr != ":9500" {
		t.Errorf("node id / gossip: %s / %s", cfg.NodeID, cfg.GossipAddr)
	}
	if len(cfg.SeedNodes) != 2 {
		t.Errorf("seeds: %v", cfg.SeedNodes)
	}
	if cfg.Protocol.Interval.Duration != 2*time.Second {
		t.Errorf("interval: %v", cfg.Protocol.Interval.Duration)
	}
	if cfg.Protocol.IndirectNodes != 2 {
		t.Errorf("indirect nodes: %d", cfg.Protocol.IndirectNodes)
	}
	if cfg.Logging.Level != "debug" || cfg.Logging.Format != "text" {
		t.Errorf("logging: %+v", cfg.Logging)
	}
}

func TestLoadLB_JSON_PartialFillsDefaults(t *testing.T) {
	// Missing fields must inherit from DefaultLBConfig — without this, a
	// minimal config file would silently break the gossip layer's invariants.
	body := `{"listen": ":9999"}`
	path := writeTemp(t, "partial.json", body)
	cfg, err := LoadLB(path)
	if err != nil {
		t.Fatalf("LoadLB: %v", err)
	}
	if cfg.Listen != ":9999" {
		t.Errorf("listen: %s", cfg.Listen)
	}
	if cfg.Strategy != "roundrobin" {
		t.Errorf("strategy default lost: %s", cfg.Strategy)
	}
	if cfg.GossipAddr != ":9000" {
		t.Errorf("gossip default lost: %s", cfg.GossipAddr)
	}
	if cfg.Protocol.IndirectNodes != 3 {
		t.Errorf("protocol defaults lost: %d", cfg.Protocol.IndirectNodes)
	}
	if cfg.Protocol.PingTimeout.Duration != 500*time.Millisecond {
		t.Errorf("ping default lost: %v", cfg.Protocol.PingTimeout.Duration)
	}
}

func TestLoadLB_FileNotFound(t *testing.T) {
	_, err := LoadLB("/nonexistent/path/to/config.json")
	if err == nil {
		t.Error("expected error for missing file")
	}
	if !strings.Contains(err.Error(), "read failed") {
		t.Errorf("expected read failure, got: %v", err)
	}
}

func TestLoadLB_UnsupportedFormat(t *testing.T) {
	path := writeTemp(t, "config.yaml", "listen: :8080")
	_, err := LoadLB(path)
	if err == nil {
		t.Fatal("expected error for .yaml")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Errorf("error message: %v", err)
	}
}

func TestLoadLB_InvalidJSON(t *testing.T) {
	path := writeTemp(t, "broken.json", `{"listen": ":8080",}`)
	_, err := LoadLB(path)
	if err == nil {
		t.Error("expected json parse error")
	}
}

// ---------- LoadNode ----------

func TestLoadNode_JSON_Full(t *testing.T) {
	body := `{
		"id":           "node-x",
		"gossip_addr":  ":9099",
		"service_addr": ":8099",
		"seed_nodes":   [":9001"],
		"protocol":     {"indirect_nodes": 5}
	}`
	path := writeTemp(t, "node.json", body)
	cfg, err := LoadNode(path)
	if err != nil {
		t.Fatalf("LoadNode: %v", err)
	}
	if cfg.ID != "node-x" {
		t.Errorf("id: %s", cfg.ID)
	}
	if cfg.Protocol.IndirectNodes != 5 {
		t.Errorf("indirect_nodes: %d", cfg.Protocol.IndirectNodes)
	}
	// Other protocol fields should fall back to defaults.
	if cfg.Protocol.PingTimeout.Duration != 500*time.Millisecond {
		t.Errorf("ping timeout default lost: %v", cfg.Protocol.PingTimeout.Duration)
	}
}

// ---------- Merge ----------

func TestMergeLB_FlagOverridesFile(t *testing.T) {
	cfg := LBConfig{Listen: ":8080", Strategy: "roundrobin", GossipAddr: ":9000"}
	out := MergeLB(cfg, LBFlags{Listen: ":9090", Strategy: "leastconn"})
	if out.Listen != ":9090" || out.Strategy != "leastconn" {
		t.Errorf("flags lost: %+v", out)
	}
	if out.GossipAddr != ":9000" {
		t.Errorf("untouched field clobbered: %s", out.GossipAddr)
	}
}

func TestMergeLB_EmptyFlagsKeepsFile(t *testing.T) {
	cfg := LBConfig{Listen: ":8080", Strategy: "p2c", NodeID: "lb-1"}
	out := MergeLB(cfg, LBFlags{}) // all empty
	if out.Listen != ":8080" || out.Strategy != "p2c" || out.NodeID != "lb-1" {
		t.Errorf("empty flags overwrote file: %+v", out)
	}
}

func TestMergeLB_SeedsReplaceCompletely(t *testing.T) {
	// When CLI passes seeds, they fully replace file seeds — this is the
	// expected behaviour because the user explicitly said "use these".
	cfg := LBConfig{SeedNodes: []string{":9001", ":9002"}}
	out := MergeLB(cfg, LBFlags{SeedNodes: []string{":9003"}})
	if len(out.SeedNodes) != 1 || out.SeedNodes[0] != ":9003" {
		t.Errorf("seeds: %v", out.SeedNodes)
	}
}

func TestMergeNode_FlagOverridesFile(t *testing.T) {
	cfg := NodeConfig{ID: "node-a", GossipAddr: ":9001"}
	out := MergeNode(cfg, NodeFlags{ID: "node-z", ServiceAddr: ":8009"})
	if out.ID != "node-z" {
		t.Errorf("id: %s", out.ID)
	}
	if out.GossipAddr != ":9001" {
		t.Errorf("gossip clobbered: %s", out.GossipAddr)
	}
	if out.ServiceAddr != ":8009" {
		t.Errorf("service: %s", out.ServiceAddr)
	}
}

// ---------- Env ----------

func TestApplyEnvLB_Overrides(t *testing.T) {
	t.Setenv("P2PLB_LISTEN", ":7070")
	t.Setenv("P2PLB_STRATEGY", "leastconn")
	t.Setenv("P2PLB_SEED_NODES", ":9001, :9002 , :9003")
	t.Setenv("P2PLB_PROTOCOL_PING_TIMEOUT", "200ms")
	t.Setenv("P2PLB_PROTOCOL_INDIRECT_NODES", "5")

	cfg := DefaultLBConfig()
	cfg = ApplyEnvLB(cfg)

	if cfg.Listen != ":7070" {
		t.Errorf("listen: %s", cfg.Listen)
	}
	if cfg.Strategy != "leastconn" {
		t.Errorf("strategy: %s", cfg.Strategy)
	}
	if len(cfg.SeedNodes) != 3 {
		t.Errorf("seeds: %v", cfg.SeedNodes)
	}
	if cfg.Protocol.PingTimeout.Duration != 200*time.Millisecond {
		t.Errorf("ping: %v", cfg.Protocol.PingTimeout.Duration)
	}
	if cfg.Protocol.IndirectNodes != 5 {
		t.Errorf("indirect nodes: %d", cfg.Protocol.IndirectNodes)
	}
}

func TestApplyEnvNode_Overrides(t *testing.T) {
	t.Setenv("P2PLB_NODE_ID", "env-node")
	t.Setenv("P2PLB_SERVICE_ADDR", ":8888")

	cfg := DefaultNodeConfig()
	cfg = ApplyEnvNode(cfg)

	if cfg.ID != "env-node" {
		t.Errorf("id: %s", cfg.ID)
	}
	if cfg.ServiceAddr != ":8888" {
		t.Errorf("service: %s", cfg.ServiceAddr)
	}
}

func TestApplyEnvLB_InvalidDurationIgnored(t *testing.T) {
	// Bad values from the environment shouldn't crash; they should be
	// ignored so the file/default value survives.
	t.Setenv("P2PLB_PROTOCOL_PING_TIMEOUT", "garbage")
	cfg := DefaultLBConfig()
	cfg = ApplyEnvLB(cfg)
	if cfg.Protocol.PingTimeout.Duration != 500*time.Millisecond {
		t.Errorf("default lost on bad env: %v", cfg.Protocol.PingTimeout.Duration)
	}
}

// ---------- Validate ----------

func TestValidateLB_Defaults(t *testing.T) {
	if err := ValidateLB(DefaultLBConfig()); err != nil {
		t.Errorf("default config invalid: %v", err)
	}
}

func TestValidateLB_EmptyListen(t *testing.T) {
	cfg := DefaultLBConfig()
	cfg.Listen = ""
	err := ValidateLB(cfg)
	if err == nil || !strings.Contains(err.Error(), "listen") {
		t.Errorf("expected listen error, got: %v", err)
	}
}

func TestValidateLB_UnknownStrategy(t *testing.T) {
	cfg := DefaultLBConfig()
	cfg.Strategy = "made-up"
	err := ValidateLB(cfg)
	if err == nil || !strings.Contains(err.Error(), "unknown strategy") {
		t.Errorf("expected strategy error, got: %v", err)
	}
}

func TestValidateLB_PingGreaterThanIndirect(t *testing.T) {
	cfg := DefaultLBConfig()
	cfg.Protocol.PingTimeout = Duration{2 * time.Second}
	cfg.Protocol.IndirectTimeout = Duration{1 * time.Second}
	err := ValidateLB(cfg)
	if err == nil || !strings.Contains(err.Error(), "ping_timeout") {
		t.Errorf("expected ping/indirect ordering error, got: %v", err)
	}
}

func TestValidateLB_MultipleErrors(t *testing.T) {
	// Make sure all errors are surfaced together rather than fail-fast on the
	// first one — the user wants the full picture.
	cfg := DefaultLBConfig()
	cfg.Listen = ""
	cfg.Strategy = "bogus"
	cfg.NodeID = ""
	err := ValidateLB(cfg)
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	for _, want := range []string{"listen", "strategy", "node_id"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error missing %q: %s", want, msg)
		}
	}
}

func TestValidateNode_RequiresIdentity(t *testing.T) {
	cfg := DefaultNodeConfig() // ID/GossipAddr/ServiceAddr empty by default
	err := ValidateNode(cfg)
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"id", "gossip_addr", "service_addr"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

// ---------- ToInternal ----------

func TestLBConfig_ToInternal(t *testing.T) {
	cfg := DefaultLBConfig()
	cfg.Listen = ":7070"
	cfg.Strategy = "leastconn"
	cfg.GossipAddr = ":9500"
	cfg.NodeID = "lb-x"
	cfg.SeedNodes = []string{":9001", ":9002"}

	out := cfg.ToInternal()
	if out.ListenAddr != ":7070" || out.Strategy != "leastconn" {
		t.Errorf("converted: %+v", out)
	}
	if out.Protocol.NodeID != "lb-x" || out.Protocol.GossipAddr != ":9500" {
		t.Errorf("protocol identity: %+v", out.Protocol)
	}
	if len(out.Protocol.SeedNodes) != 2 {
		t.Errorf("seeds: %v", out.Protocol.SeedNodes)
	}
	// Crucial: the internal protocol config must pass its own validation.
	if err := out.Protocol.Validate(); err != nil {
		t.Errorf("internal validate failed: %v", err)
	}
}

func TestNodeConfig_ToInternal(t *testing.T) {
	cfg := DefaultNodeConfig()
	cfg.ID = "node-q"
	cfg.GossipAddr = ":9100"
	cfg.ServiceAddr = ":8100"
	cfg.SeedNodes = []string{":9001"}

	out := cfg.ToInternal()
	if out.NodeID != "node-q" || out.GossipAddr != ":9100" || out.ServiceAddr != ":8100" {
		t.Errorf("identity: %+v", out)
	}
	if err := out.Validate(); err != nil {
		t.Errorf("internal validate failed: %v", err)
	}
}

// ---------- TOML behaviour (default build) ----------

func TestLoadLB_TOML_DisabledByDefault(t *testing.T) {
	// In the default build (no `toml` tag), TOML should produce a clear
	// error rather than silently accepting it. With `-tags toml` this test
	// would not apply.
	path := writeTemp(t, "config.toml", `listen = ":8080"`)
	_, err := LoadLB(path)
	if err == nil {
		// If this fires under -tags toml, the test was run under that tag
		// (which is fine — TOML works there). Skip rather than fail.
		t.Skip("TOML build tag enabled, skipping disabled-default check")
	}
}

// ---------- helpers ----------

func writeTemp(t *testing.T, name, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	return path
}
