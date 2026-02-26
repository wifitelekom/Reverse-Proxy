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

// TestAddOrUpdate_NewNode: yeni node eklenmeli
func TestAddOrUpdate_NewNode(t *testing.T) {
	ml := newTestML("self")
	updated := ml.AddOrUpdate(aliveNode("node-b", ":9002", 1))
	if !updated {
		t.Fatal("yeni node eklenirken true dönmeli")
	}
	members := ml.GetAliveMembers()
	if len(members) != 1 || members[0].ID != "node-b" {
		t.Fatalf("beklenen 1 üye, alınan: %v", members)
	}
}

// TestAddOrUpdate_StaleIgnored: düşük incarnation yoksayılmalı
func TestAddOrUpdate_StaleIgnored(t *testing.T) {
	ml := newTestML("self")
	ml.AddOrUpdate(aliveNode("node-b", ":9002", 5))

	// Daha düşük incarnation → yoksay
	updated := ml.AddOrUpdate(aliveNode("node-b", ":9002", 3))
	if updated {
		t.Fatal("stale bilgi (düşük incarnation) kabul edilmemeli")
	}

	// Incarnation 5 hâlâ korunmuş olmalı
	info, found := ml.GetMember("node-b")
	if !found || info.Incarnation != 5 {
		t.Fatalf("incarnation korunmadı, beklenen 5 alınan %v", info.Incarnation)
	}
}

// TestAddOrUpdate_WorseStateWins: aynı incarnation'da Dead > Alive
func TestAddOrUpdate_WorseStateWins(t *testing.T) {
	ml := newTestML("self")
	ml.AddOrUpdate(aliveNode("node-b", ":9002", 1))

	// Aynı incarnation, daha kötü state → kabul et
	dead := NodeInfo{ID: "node-b", Addr: ":9002", ServiceAddr: ":9002", State: StateDead, Incarnation: 1}
	updated := ml.AddOrUpdate(dead)
	if !updated {
		t.Fatal("aynı incarnation'da Dead > Alive olmalı")
	}

	// GetAliveMembers'ta görünmemeli
	alive := ml.GetAliveMembers()
	for _, m := range alive {
		if m.ID == "node-b" {
			t.Fatal("dead node alive listesinde görünmemeli")
		}
	}
}

// TestAddOrUpdate_HigherIncarnationAlwaysWins: yüksek incarnation her zaman kazanır
func TestAddOrUpdate_HigherIncarnationAlwaysWins(t *testing.T) {
	ml := newTestML("self")

	// Dead olarak ekle
	dead := NodeInfo{ID: "node-b", Addr: ":9002", ServiceAddr: ":9002", State: StateDead, Incarnation: 3}
	ml.AddOrUpdate(dead)

	// Daha yüksek incarnation ile Alive → kabul et
	updated := ml.AddOrUpdate(aliveNode("node-b", ":9002", 5))
	if !updated {
		t.Fatal("daha yüksek incarnation ile Alive kabul edilmeli")
	}

	alive := ml.GetAliveMembers()
	if len(alive) != 1 {
		t.Fatalf("node-b alive listesinde olmalı, alınan: %v", alive)
	}
}

// TestMarkSuspect_BecomesDeadAfterTimeout: suspect timer → dead callback
func TestMarkSuspect_BecomesDeadAfterTimeout(t *testing.T) {
	ml := newTestML("self")
	ml.AddOrUpdate(aliveNode("node-b", ":9002", 1))

	deadCalled := make(chan string, 1)
	ml.MarkSuspect("node-b", 50*time.Millisecond, func(id string) {
		deadCalled <- id
	})

	// Suspect state kontrol
	info, _ := ml.GetMember("node-b")
	if info.State != StateSuspect {
		t.Fatalf("beklenen StateSuspect, alınan %v", info.State)
	}

	// Dead callback 50ms sonra gelmeli
	select {
	case id := <-deadCalled:
		if id != "node-b" {
			t.Fatalf("beklenen 'node-b', alınan '%s'", id)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("dead callback zamanında gelmedi")
	}

	// Dead state kontrol
	info, _ = ml.GetMember("node-b")
	if info.State != StateDead {
		t.Fatalf("beklenen StateDead, alınan %v", info.State)
	}
}

// TestMarkSuspect_AliveBeforeTimeout: alive gelince suspect timer iptal edilmeli
func TestMarkSuspect_AliveBeforeTimeout(t *testing.T) {
	ml := newTestML("self")
	ml.AddOrUpdate(aliveNode("node-b", ":9002", 1))

	deadCalled := make(chan string, 1)
	ml.MarkSuspect("node-b", 200*time.Millisecond, func(id string) {
		deadCalled <- id
	})

	// Suspect zamanlamadan önce daha yüksek incarnation ile Alive gönder
	ml.AddOrUpdate(aliveNode("node-b", ":9002", 2))

	// Timer iptal edilmiş olmalı — dead callback gelmemeli
	select {
	case id := <-deadCalled:
		t.Fatalf("dead callback gelmemeli, geldi: %s", id)
	case <-time.After(400 * time.Millisecond):
		// Beklenen: callback yok ✓
	}

	info, _ := ml.GetMember("node-b")
	if info.State != StateAlive {
		t.Fatalf("beklenen StateAlive, alınan %v", info.State)
	}
}

// TestDefend_IncrementsIncarnation: incarnation++ + StateAlive
func TestDefend_IncrementsIncarnation(t *testing.T) {
	ml := newTestML("self")
	local := aliveNode("self", ":9001", 3)
	ml.AddOrUpdate(local)

	ml.Defend(&local)

	info, _ := ml.GetMember("self")
	if info.Incarnation != 4 {
		t.Fatalf("beklenen incarnation 4, alınan %v", info.Incarnation)
	}
	if info.State != StateAlive {
		t.Fatalf("defend sonrası StateAlive bekleniyor")
	}
	if local.Incarnation != 4 {
		t.Fatalf("localInfo pointer güncellenmeli: beklenen 4, alınan %v", local.Incarnation)
	}
}

// TestGetAliveMembers_ExcludesLB: ServiceAddr="" olanlar (LB) filtrelenmeli
func TestGetAliveMembers_ExcludesLB(t *testing.T) {
	ml := newTestML("lb")

	// LB kendisi — ServiceAddr boş
	lb := NodeInfo{ID: "lb", Addr: ":9000", ServiceAddr: "", State: StateAlive, Incarnation: 1}
	ml.AddOrUpdate(lb)

	// Backend node
	ml.AddOrUpdate(aliveNode("node-a", ":9001", 1))

	alive := ml.GetAliveMembers()
	if len(alive) != 1 || alive[0].ID != "node-a" {
		t.Fatalf("LB filtrelenmeli, sadece node-a görünmeli. Alınan: %v", alive)
	}
}

// TestGetAliveMembers_ExcludesSelf: localID hariç tutulmalı
func TestGetAliveMembers_ExcludesSelf(t *testing.T) {
	ml := newTestML("node-a")
	ml.AddOrUpdate(aliveNode("node-a", ":9001", 1))
	ml.AddOrUpdate(aliveNode("node-b", ":9002", 1))

	alive := ml.GetAliveMembers()
	for _, m := range alive {
		if m.ID == "node-a" {
			t.Fatal("localID (node-a) alive listesinde olmamalı")
		}
	}
	if len(alive) != 1 || alive[0].ID != "node-b" {
		t.Fatalf("sadece node-b bekleniyor, alınan: %v", alive)
	}
}

// TestGetRandomMembers_ExcludesGiven: excludeIDs'dekiler hariç tutulmalı
func TestGetRandomMembers_ExcludesGiven(t *testing.T) {
	ml := newTestML("self")
	ml.AddOrUpdate(aliveNode("node-a", ":9001", 1))
	ml.AddOrUpdate(aliveNode("node-b", ":9002", 1))
	ml.AddOrUpdate(aliveNode("node-c", ":9003", 1))

	members := ml.GetRandomMembers(10, "node-b") // node-b hariç
	for _, m := range members {
		if m.ID == "node-b" || m.ID == "self" {
			t.Fatalf("exclude listesindeki ID döndürülmemeli: %s", m.ID)
		}
	}
}
