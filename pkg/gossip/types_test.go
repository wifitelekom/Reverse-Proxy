package gossip

import "testing"

// TestNodeStateString exhaustively covers the enum's String() mapping,
// including the UNKNOWN fallback for out-of-range values. The protocol
// uses these strings in metric labels and log lines; an inadvertent
// rename would break dashboards and grep queries downstream.
func TestNodeStateString(t *testing.T) {
	tests := []struct {
		state NodeState
		want  string
	}{
		{StateAlive, "ALIVE"},
		{StateSuspect, "SUSPECT"},
		{StateDead, "DEAD"},
		{NodeState(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Fatalf("state=%v: expected %q, got %q", tt.state, tt.want, got)
		}
	}
}

// TestAtomicLoadHelpers exercises the small atomic wrappers for
// NodeInfo.Load. They're used from the load-balancer hot path; a
// regression here would silently break LeastConn / P2C correctness.
func TestAtomicLoadHelpers(t *testing.T) {
	var load int32
	if got := AtomicIncLoad(&load); got != 1 {
		t.Fatalf("after Inc: expected 1, got %d", got)
	}
	if got := AtomicDecLoad(&load); got != 0 {
		t.Fatalf("after Dec: expected 0, got %d", got)
	}
}
