package loadbalancer

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"p2p-loadbalancer/pkg/gossip"
	"p2p-loadbalancer/pkg/metrics"
	"p2p-loadbalancer/pkg/types"
)

// =====================================================================
// H-2': Admin endpoint access control
//
// The admin endpoints (/_lb/health, /_lb/metrics) carry operational
// signal that should not be world-readable on a flat network. The
// audit found these endpoints had only a rate limiter; an attacker
// reaching the LB on its data port could enumerate backend health and
// scrape Prometheus metrics. These tests pin the gate.
// =====================================================================

// newAdminTestLB builds a load balancer with admin auth configured per
// the supplied AdminConfig. Returns the LB and a server URL pointing at
// the admin handler. Backends list is empty — admin endpoints don't
// need them, and an empty list lets us focus on auth without selector
// noise.
func newAdminTestLB(t *testing.T, admin types.AdminConfig) (*LoadBalancer, string) {
	t.Helper()
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)

	cfg := defaultLBConfig(":0")
	cfg.Admin = admin

	m := metrics.New()
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, m)
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(lb.handleRequest))
	t.Cleanup(srv.Close)
	return lb, srv.URL
}

// TestAdminAuth_DefaultUnchanged: without any AllowCIDRs or BearerToken
// configured, the admin endpoints stay open — back-compat for every
// existing deployment that doesn't opt in.
func TestAdminAuth_DefaultUnchanged(t *testing.T) {
	_, base := newAdminTestLB(t, types.AdminConfig{})

	resp, err := http.Get(base + "/_lb/metrics")
	if err != nil {
		t.Fatalf("metrics scrape: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("default config should allow metrics scrape, got %d", resp.StatusCode)
	}
}

// TestAdminAuth_CIDRAllowMatchesLoopback: 127.0.0.1/32 in the allowlist
// must accept httptest's loopback connections.
func TestAdminAuth_CIDRAllowMatchesLoopback(t *testing.T) {
	_, base := newAdminTestLB(t, types.AdminConfig{
		AllowCIDRs: []string{"127.0.0.1/32"},
	})

	resp, err := http.Get(base + "/_lb/metrics")
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("loopback CIDR should pass, got %d", resp.StatusCode)
	}
}

// TestAdminAuth_CIDRRejectsOutOfRange: an allowlist that excludes
// loopback (e.g. only 10.0.0.0/8) must reject the test client.
// We assert 404 specifically — refusing to confirm path existence to
// an unauthorised caller is the chosen leakage policy.
func TestAdminAuth_CIDRRejectsOutOfRange(t *testing.T) {
	_, base := newAdminTestLB(t, types.AdminConfig{
		AllowCIDRs: []string{"10.0.0.0/8"},
	})

	resp, err := http.Get(base + "/_lb/metrics")
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("out-of-range CIDR should 404, got %d", resp.StatusCode)
	}
}

// TestAdminAuth_BearerRequiresMatch: with a token configured but no
// CIDR, the request must carry the matching Authorization header.
func TestAdminAuth_BearerRequiresMatch(t *testing.T) {
	const tok = "s3cr3t-token-aaaaaaaaaaaaaaaaaaaa"
	_, base := newAdminTestLB(t, types.AdminConfig{
		BearerToken: tok,
	})

	// No header → reject.
	resp, _ := http.Get(base + "/_lb/metrics")
	if resp != nil {
		resp.Body.Close()
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Errorf("missing token: got %v, want 404", resp)
	}

	// Wrong token → reject.
	req, _ := http.NewRequest("GET", base+"/_lb/metrics", nil)
	req.Header.Set("Authorization", "Bearer wrongwrongwrongwrongwrongwrongwro")
	resp, _ = http.DefaultClient.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Errorf("wrong token: got %v, want 404", resp)
	}

	// Right token → pass.
	req, _ = http.NewRequest("GET", base+"/_lb/metrics", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("right token: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("matching token should pass, got %d", resp.StatusCode)
	}
}

// TestAdminAuth_BearerWrongScheme: only "Bearer <token>" counts.
// "Basic", a bare token, or other schemes must fail — they reveal
// either a misconfigured client (which we want to break loudly) or
// an attacker fishing for accepted shapes.
func TestAdminAuth_BearerWrongScheme(t *testing.T) {
	const tok = "s3cr3t-token-aaaaaaaaaaaaaaaaaaaa"
	_, base := newAdminTestLB(t, types.AdminConfig{BearerToken: tok})

	cases := []string{
		tok,              // no scheme
		"Basic " + tok,   // wrong scheme
		"bearer " + tok,  // case-sensitive: must be "Bearer"
		"Bearer  " + tok, // double space (token would have a leading space)
		"Token " + tok,   // close-but-no
	}
	for _, h := range cases {
		req, _ := http.NewRequest("GET", base+"/_lb/metrics", nil)
		req.Header.Set("Authorization", h)
		resp, _ := http.DefaultClient.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		if resp == nil || resp.StatusCode != http.StatusNotFound {
			t.Errorf("Authorization=%q should fail, got %v", h, resp)
		}
	}
}

// TestAdminAuth_CIDRAndBearerBothRequired: when both are configured,
// failure of either rejects. AND semantics — defense in depth.
func TestAdminAuth_CIDRAndBearerBothRequired(t *testing.T) {
	const tok = "two-gate-token-aaaaaaaaaaaaaaaaaa"
	_, base := newAdminTestLB(t, types.AdminConfig{
		AllowCIDRs:  []string{"127.0.0.1/32"},
		BearerToken: tok,
	})

	// In CIDR but missing token → fail.
	resp, _ := http.Get(base + "/_lb/metrics")
	if resp != nil {
		resp.Body.Close()
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Errorf("CIDR-only (missing token) should fail, got %v", resp)
	}

	// In CIDR + correct token → pass.
	req, _ := http.NewRequest("GET", base+"/_lb/metrics", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("both-correct: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("both correct should pass, got %d", resp.StatusCode)
	}
}

// TestAdminAuth_BadCIDRFailsStartup: a malformed CIDR in the config
// must surface at construction. Silently dropping bad entries would be
// a security regression — the operator typoed `127.0.0/32` and ended
// up with no allowlist at all.
func TestAdminAuth_BadCIDRFailsStartup(t *testing.T) {
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	cfg := defaultLBConfig(":0")
	cfg.Admin = types.AdminConfig{
		AllowCIDRs: []string{"not-a-cidr"},
	}
	_, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, metrics.New())
	if err == nil {
		t.Fatal("malformed CIDR should fail NewLoadBalancer; it didn't")
	}
	if !strings.Contains(err.Error(), "admin") || !strings.Contains(err.Error(), "CIDR") {
		t.Errorf("error should mention admin/CIDR, got: %v", err)
	}
}

// TestAdminAuth_AuthRunsBeforeRateLimit pins the order-of-gates
// invariant: an unauthorised caller must NOT consume the rate-limit
// budget. If auth ran second, an attacker could send 100 requests/s
// from a forbidden source and starve the legitimate scraper.
//
// We exercise this by:
//  1. Configuring CIDR-only auth that excludes loopback.
//  2. Firing many forbidden requests in a row — each must 404 (not 429).
//  3. The success of step 2 alone is the contract; if rate-limit
//     fired first, some of those would have come back as 429.
func TestAdminAuth_AuthRunsBeforeRateLimit(t *testing.T) {
	_, base := newAdminTestLB(t, types.AdminConfig{
		AllowCIDRs: []string{"10.0.0.0/8"}, // excludes loopback
	})

	// AdminRateLimitBurst is 20; fire 50 requests so the limiter would
	// definitely engage if it ran first.
	for i := 0; i < 50; i++ {
		resp, err := http.Get(base + "/_lb/metrics")
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("request %d: got %d, want 404 (auth must run before rate limit)",
				i, resp.StatusCode)
		}
	}
}

// TestAuthorizeAdmin_Unit_BadRemoteAddr is a direct unit test of the
// authorizeAdmin helper for the malformed-RemoteAddr edge case. The
// stdlib server normally guarantees host:port shape, but a future
// custom listener or middleware could violate that — we choose to
// fail closed.
func TestAuthorizeAdmin_Unit_BadRemoteAddr(t *testing.T) {
	logger := discardLogger()
	ml := gossip.NewMembershipList("lb", logger)
	cfg := defaultLBConfig(":0")
	cfg.Admin = types.AdminConfig{AllowCIDRs: []string{"127.0.0.1/32"}}
	lb, err := NewLoadBalancer(cfg, ml, NewRoundRobin(), logger, nil, metrics.New())
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}

	cases := []string{
		"",               // empty
		"not-an-address", // missing colon
		"[::1",           // unbalanced bracket
		"127.0.0.1",      // missing port
		"hostname:9000",  // not an IP literal
	}
	for _, ra := range cases {
		req := httptest.NewRequest("GET", "/_lb/metrics", nil)
		req.RemoteAddr = ra
		if lb.authorizeAdmin(req) {
			t.Errorf("RemoteAddr=%q should be rejected (fail-closed), but was authorized", ra)
		}
	}
}
