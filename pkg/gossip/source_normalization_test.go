package gossip

import "testing"

// TestCanonicalSourceAddr_PreservesNormalIPv4 — the no-op case must
// stay no-op so the fix doesn't perturb existing IPv4 deployments.
// If this regresses, every previously-pinned source is invalidated.
func TestCanonicalSourceAddr_PreservesNormalIPv4(t *testing.T) {
	cases := []string{
		"192.0.2.1:9001",
		"127.0.0.1:9000",
		"10.0.0.5:54321",
		"0.0.0.0:8080",
	}
	for _, in := range cases {
		if got := canonicalSourceAddr(in); got != in {
			t.Errorf("canonicalSourceAddr(%q) = %q, want %q (no-op expected)", in, got, in)
		}
	}
}

// TestCanonicalSourceAddr_StripsIPv6Zone is the headline H-1' fix:
// link-local addresses arriving on different interfaces would otherwise
// look like impersonation. The zone is metadata about the receive path,
// not identity, so it must be stripped before pinning / comparison.
func TestCanonicalSourceAddr_StripsIPv6Zone(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"[fe80::1%lo0]:9001", "[fe80::1]:9001"},
		{"[fe80::1%eth0]:9001", "[fe80::1]:9001"},
		{"[fe80::dead:beef%en0]:1234", "[fe80::dead:beef]:1234"},
	}
	for _, c := range cases {
		if got := canonicalSourceAddr(c.in); got != c.want {
			t.Errorf("canonicalSourceAddr(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestCanonicalSourceAddr_TwoZonesEqualSameAddr proves the operational
// invariant: the same nodeID arriving via two zone-tagged sources must
// canonicalise identically. This is the property C-2's spoof check
// depends on.
func TestCanonicalSourceAddr_TwoZonesEqualSameAddr(t *testing.T) {
	a := canonicalSourceAddr("[fe80::1%lo0]:9001")
	b := canonicalSourceAddr("[fe80::1%eth0]:9001")
	if a != b {
		t.Errorf("zoned variants must canonicalise to the same form: %q vs %q", a, b)
	}
}

// TestCanonicalSourceAddr_UnmapsIPv4Mapped folds IPv4-mapped IPv6 into
// plain IPv4. A dual-stack receiver may report the same peer in either
// shape depending on which socket fielded the datagram; treating them
// as identity-distinct breaks the pin.
func TestCanonicalSourceAddr_UnmapsIPv4Mapped(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"[::ffff:192.0.2.1]:9001", "192.0.2.1:9001"},
		{"[::ffff:10.0.0.5]:443", "10.0.0.5:443"},
	}
	for _, c := range cases {
		if got := canonicalSourceAddr(c.in); got != c.want {
			t.Errorf("canonicalSourceAddr(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestCanonicalSourceAddr_PreservesGlobalIPv6 — non-link-local IPv6
// addresses have no zone and shouldn't be touched. Round-trips through
// netip canonicalise the textual form (lowercase hex, :: compression),
// which is the expected, harmless side effect.
func TestCanonicalSourceAddr_PreservesGlobalIPv6(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"[2001:db8::1]:9001", "[2001:db8::1]:9001"},
		// netip canonicalises mixed case and zero compression, which is
		// exactly the behaviour we want for stable comparisons.
		{"[2001:DB8::0001]:9001", "[2001:db8::1]:9001"},
	}
	for _, c := range cases {
		if got := canonicalSourceAddr(c.in); got != c.want {
			t.Errorf("canonicalSourceAddr(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestCanonicalSourceAddr_FallsBackOnGarbage — the function must not
// panic on malformed input. Returning the input unchanged means any
// downstream comparison falls back to byte-equality, which is safe
// (different garbage strings won't accidentally collide).
func TestCanonicalSourceAddr_FallsBackOnGarbage(t *testing.T) {
	cases := []string{
		"",
		"not an address",
		"hostname:9001",
		"192.0.2.1", // missing port
		"[fe80::1]", // missing port
		":::::",
	}
	for _, in := range cases {
		// Must not panic; output may equal input or any deterministic
		// transformation. We only assert the no-panic property and that
		// equal inputs produce equal outputs.
		got1 := canonicalSourceAddr(in)
		got2 := canonicalSourceAddr(in)
		if got1 != got2 {
			t.Errorf("canonicalSourceAddr(%q) is non-deterministic: %q vs %q", in, got1, got2)
		}
	}
}

// TestVerifyOrPinSource_ZoneDriftDoesNotImpersonate is the
// integration-level proof for H-1':
//
// A node initially gossips from `[fe80::1%eth0]:9001`; we pin that as
// its source. Later, the same node's traffic arrives via the loopback
// interface (`[fe80::1%lo0]:9001`) — a normal occurrence on a host
// with multi-homed interfaces or after a routing-table change.
// Pre-fix: this would look like impersonation and the message would
// be rejected. Post-fix: the zone is stripped before comparison and
// the message matches.
func TestVerifyOrPinSource_ZoneDriftDoesNotImpersonate(t *testing.T) {
	logger := silentLogger()
	ml := NewMembershipList("local", logger)
	ml.AddOrUpdate(NodeInfo{ID: "peer", Addr: "[fe80::1]:9001", State: StateAlive, Incarnation: 1})

	// First contact arrives on eth0 — pin it.
	if got := ml.VerifyOrPinSource("peer", "[fe80::1%eth0]:9001"); got != SourcePinnedNow {
		t.Fatalf("first VerifyOrPinSource: got %v, want SourcePinnedNow", got)
	}

	// Same peer, different zone (e.g. arrived via lo0 this time).
	// Without zone normalisation this would be SourceMismatch — and
	// would silently drop legitimate gossip.
	if got := ml.VerifyOrPinSource("peer", "[fe80::1%lo0]:9001"); got != SourceMatch {
		t.Errorf("zone-drifted VerifyOrPinSource: got %v, want SourceMatch (zone normalisation broken)", got)
	}

	// And a real impersonation attempt — different IP entirely — must
	// still reject. We didn't widen the gate, only canonicalised it.
	if got := ml.VerifyOrPinSource("peer", "[fe80::99]:9001"); got != SourceMismatch {
		t.Errorf("genuine impersonation: got %v, want SourceMismatch", got)
	}
}

// TestPinSourceIfAbsent_StoresCanonicalForm confirms the write-side
// canonicalisation: a pin with a zone-tagged input stores the bare
// form, so future reads of any zone variant see SourceMatch.
func TestPinSourceIfAbsent_StoresCanonicalForm(t *testing.T) {
	logger := silentLogger()
	ml := NewMembershipList("local", logger)
	ml.AddOrUpdate(NodeInfo{ID: "peer", Addr: "[fe80::1]:9001", State: StateAlive, Incarnation: 1})

	if !ml.PinSourceIfAbsent("peer", "[fe80::1%eth0]:9001") {
		t.Fatal("PinSourceIfAbsent should have succeeded for fresh entry")
	}

	// Subsequent verify with a different zone must match — proves the
	// stored value was canonicalised, not the raw input.
	if got := ml.VerifyOrPinSource("peer", "[fe80::1%lo0]:9001"); got != SourceMatch {
		t.Errorf("post-pin verify with different zone: got %v, want SourceMatch", got)
	}
}
