package gossip

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func newTestML(localID string) *MembershipList {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	return NewMembershipList(localID, logger)
}

func aliveNode(id, addr string, incarnation uint64) NodeInfo {
	return NodeInfo{
		ID:          id,
		Addr:        addr,
		ServiceAddr: addr,
		State:       StateAlive,
		Incarnation: incarnation,
	}
}

// TestAddOrUpdate_NewNode: a brand-new node must be accepted.
func TestAddOrUpdate_NewNode(t *testing.T) {
	ml := newTestML("self")
	updated := ml.AddOrUpdate(aliveNode("node-b", ":9002", 1))
	if !updated {
		t.Fatal("AddOrUpdate must return true for a brand-new node")
	}
	members := ml.GetAliveMembers()
	if len(members) != 1 || members[0].ID != "node-b" {
		t.Fatalf("expected exactly node-b in alive set, got: %v", members)
	}
}

// TestAddOrUpdate_StaleIgnored: lower incarnation must be ignored.
func TestAddOrUpdate_StaleIgnored(t *testing.T) {
	ml := newTestML("self")
	ml.AddOrUpdate(aliveNode("node-b", ":9002", 5))

	// Lower incarnation → ignore.
	updated := ml.AddOrUpdate(aliveNode("node-b", ":9002", 3))
	if updated {
		t.Fatal("stale info (lower incarnation) must not be accepted")
	}

	// Incarnation 5 must still be intact.
	info, found := ml.GetMember("node-b")
	if !found || info.Incarnation != 5 {
		t.Fatalf("incarnation lost: expected 5, got %v", info.Incarnation)
	}
}

// TestAddOrUpdate_WorseStateWins: at the same incarnation, Dead > Alive.
func TestAddOrUpdate_WorseStateWins(t *testing.T) {
	ml := newTestML("self")
	ml.AddOrUpdate(aliveNode("node-b", ":9002", 1))

	// Same incarnation, worse state → accept.
	dead := NodeInfo{ID: "node-b", Addr: ":9002", ServiceAddr: ":9002", State: StateDead, Incarnation: 1}
	updated := ml.AddOrUpdate(dead)
	if !updated {
		t.Fatal("Dead must beat Alive at the same incarnation")
	}

	// Dead must NOT show up in GetAliveMembers.
	alive := ml.GetAliveMembers()
	for _, m := range alive {
		if m.ID == "node-b" {
			t.Fatal("dead node leaked into the alive set")
		}
	}
}

// TestAddOrUpdate_HigherIncarnationAlwaysWins: higher incarnation beats any state.
func TestAddOrUpdate_HigherIncarnationAlwaysWins(t *testing.T) {
	ml := newTestML("self")

	// Insert as Dead.
	dead := NodeInfo{ID: "node-b", Addr: ":9002", ServiceAddr: ":9002", State: StateDead, Incarnation: 3}
	ml.AddOrUpdate(dead)

	// Higher incarnation with Alive → accept.
	updated := ml.AddOrUpdate(aliveNode("node-b", ":9002", 5))
	if !updated {
		t.Fatal("Alive at higher incarnation must be accepted")
	}

	alive := ml.GetAliveMembers()
	if len(alive) != 1 {
		t.Fatalf("node-b should appear in alive set, got: %v", alive)
	}
}

// TestMarkSuspect_BecomesDeadAfterTimeout: suspect timer fires the dead callback.
func TestMarkSuspect_BecomesDeadAfterTimeout(t *testing.T) {
	ml := newTestML("self")
	ml.AddOrUpdate(aliveNode("node-b", ":9002", 1))

	deadCalled := make(chan string, 1)
	ml.MarkSuspect("node-b", 50*time.Millisecond, func(id string) {
		deadCalled <- id
	})

	// State must transition to Suspect immediately.
	info, _ := ml.GetMember("node-b")
	if info.State != StateSuspect {
		t.Fatalf("expected StateSuspect, got %v", info.State)
	}

	// The dead callback must fire after the timeout elapses.
	select {
	case id := <-deadCalled:
		if id != "node-b" {
			t.Fatalf("expected 'node-b', got '%s'", id)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("dead callback did not fire within budget")
	}

	// State must be Dead now.
	info, _ = ml.GetMember("node-b")
	if info.State != StateDead {
		t.Fatalf("expected StateDead, got %v", info.State)
	}
}

// TestMarkSuspect_AliveBeforeTimeout: an Alive update before the timer fires
// must cancel the suspect timer.
func TestMarkSuspect_AliveBeforeTimeout(t *testing.T) {
	ml := newTestML("self")
	ml.AddOrUpdate(aliveNode("node-b", ":9002", 1))

	deadCalled := make(chan string, 1)
	ml.MarkSuspect("node-b", 200*time.Millisecond, func(id string) {
		deadCalled <- id
	})

	// Send an Alive update with a higher incarnation before the timer expires.
	ml.AddOrUpdate(aliveNode("node-b", ":9002", 2))

	// Timer must have been canceled — no dead callback should fire.
	select {
	case id := <-deadCalled:
		t.Fatalf("dead callback should not have fired, but did for: %s", id)
	case <-time.After(400 * time.Millisecond):
		// expected: silence ✓
	}

	info, _ := ml.GetMember("node-b")
	if info.State != StateAlive {
		t.Fatalf("expected StateAlive, got %v", info.State)
	}
}

// TestDefend_IncrementsIncarnation: Defend must bump incarnation and
// flip state to Alive on both the table entry and the caller's pointer.
func TestDefend_IncrementsIncarnation(t *testing.T) {
	ml := newTestML("self")
	local := aliveNode("self", ":9001", 3)
	ml.AddOrUpdate(local)

	ml.Defend(&local)

	info, _ := ml.GetMember("self")
	if info.Incarnation != 4 {
		t.Fatalf("expected incarnation 4, got %v", info.Incarnation)
	}
	if info.State != StateAlive {
		t.Fatalf("expected StateAlive after Defend")
	}
	if local.Incarnation != 4 {
		t.Fatalf("caller's pointer not updated: expected 4, got %v", local.Incarnation)
	}
}

// TestGetAliveMembers_ExcludesLB: nodes with empty ServiceAddr (the LB
// itself) must be filtered out of the alive set.
func TestGetAliveMembers_ExcludesLB(t *testing.T) {
	ml := newTestML("lb")

	// The LB itself — empty ServiceAddr.
	lb := NodeInfo{ID: "lb", Addr: ":9000", ServiceAddr: "", State: StateAlive, Incarnation: 1}
	ml.AddOrUpdate(lb)

	// A backend node.
	ml.AddOrUpdate(aliveNode("node-a", ":9001", 1))

	alive := ml.GetAliveMembers()
	if len(alive) != 1 || alive[0].ID != "node-a" {
		t.Fatalf("LB should be filtered out; expected only node-a, got: %v", alive)
	}
}

// TestGetAliveMembers_ExcludesSelf: localID must be excluded from results.
func TestGetAliveMembers_ExcludesSelf(t *testing.T) {
	ml := newTestML("node-a")
	ml.AddOrUpdate(aliveNode("node-a", ":9001", 1))
	ml.AddOrUpdate(aliveNode("node-b", ":9002", 1))

	alive := ml.GetAliveMembers()
	for _, m := range alive {
		if m.ID == "node-a" {
			t.Fatal("localID (node-a) leaked into the alive set")
		}
	}
	if len(alive) != 1 || alive[0].ID != "node-b" {
		t.Fatalf("expected only node-b, got: %v", alive)
	}
}

// TestGetRandomMembers_ExcludesGiven: nodes in excludeIDs must be skipped.
func TestGetRandomMembers_ExcludesGiven(t *testing.T) {
	ml := newTestML("self")
	ml.AddOrUpdate(aliveNode("node-a", ":9001", 1))
	ml.AddOrUpdate(aliveNode("node-b", ":9002", 1))
	ml.AddOrUpdate(aliveNode("node-c", ":9003", 1))

	members := ml.GetRandomMembers(10, "node-b") // exclude node-b
	for _, m := range members {
		if m.ID == "node-b" || m.ID == "self" {
			t.Fatalf("excluded ID returned: %s", m.ID)
		}
	}
}
