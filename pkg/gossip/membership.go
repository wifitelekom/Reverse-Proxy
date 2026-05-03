package gossip

import (
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// nodeEntry iç temsil — suspectTimer da burada saklanır.
// Dışarıya asla pointer verilmez, sadece NodeInfo kopyası döner.
//
// suspectConfirmers is the Lifeguard "Dogpile" vote set: each unique
// confirmer ID counts as one independent suspicion. Cleared automatically
// when the node is observed alive again or transitions to dead.
type nodeEntry struct {
	info              NodeInfo
	suspectTimer      *time.Timer
	suspectCancel     func()
	suspectConfirmers map[string]bool
}

// MembershipList thread-safe SWIM üyelik tablosudur.
// Tüm okuma/yazma işlemleri RWMutex ile korunur.
// Dışarıya SADECE snapshot (kopya) verir — caller state'i bozamaz.
type MembershipList struct {
	mu      sync.RWMutex
	members map[string]*nodeEntry
	localID string
	logger  *slog.Logger

	// metrics is non-nil; defaults to a no-op so emitObserved can fire
	// without a branch in the hot path. State-change observations are
	// always made AFTER ml.mu is released to avoid deadlocking against
	// the metrics hook (which is itself lock-free, but defensive).
	metrics MetricsHook
}

// NewMembershipList yeni bir MembershipList oluşturur.
func NewMembershipList(localID string, logger *slog.Logger) *MembershipList {
	return &MembershipList{
		members: make(map[string]*nodeEntry),
		localID: localID,
		logger:  logger,
		metrics: noopMetricsHook{},
	}
}

// SetMetricsHook installs a metrics sink. Pass nil to restore the no-op.
// State transitions detected after this call are observed via the hook.
func (ml *MembershipList) SetMetricsHook(h MetricsHook) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if h == nil {
		ml.metrics = noopMetricsHook{}
		return
	}
	ml.metrics = h
}

// AddOrUpdate bir node'u ekler veya günceller.
// SWIM incarnation number kuralı:
//   - Yeni incarnation > mevcut → her zaman kabul et
//   - Aynı incarnation → daha kötü state kabul edilir (Dead > Suspect > Alive)
//   - Yeni incarnation < mevcut → stale bilgi, yoksay
//
// Dönüş: true ise state değişti, false ise yoksayıldı.
func (ml *MembershipList) AddOrUpdate(info NodeInfo) bool {
	// We do all critical-section work under the lock, capture what the
	// metrics hook needs, then notify outside the lock. Avoids any chance
	// of deadlock if a future hook implementation calls back into the
	// membership list.
	var (
		accepted     bool
		emitTrans    bool
		fromState    string // "" if this is a join, else previous state name
		toState      string
	)

	ml.mu.Lock()
	hook := ml.metrics
	existing, exists := ml.members[info.ID]
	switch {
	case !exists:
		ml.members[info.ID] = &nodeEntry{info: info}
		ml.logger.Info("node joined", "id", info.ID, "addr", info.Addr)
		accepted = true
		emitTrans = true
		fromState = "" // empty signals "new arrival" downstream
		toState = info.State.String()

	case info.Incarnation < existing.info.Incarnation:
		// Stale bilgi — yoksay.

	case info.Incarnation == existing.info.Incarnation && info.State <= existing.info.State:
		// Aynı incarnation, state aynı veya daha iyi — yoksay.

	default:
		// Alive geliyorsa aktif suspect timer'ı iptal et + Dogpile oylarını sıfırla
		if info.State == StateAlive && existing.suspectCancel != nil {
			existing.suspectCancel()
			existing.suspectTimer = nil
			existing.suspectCancel = nil
			// Lifeguard: revival cancels prior suspect votes — a fresh
			// suspicion in the future must accumulate confirmations from
			// scratch.
			existing.suspectConfirmers = nil
		}
		prev := existing.info.State
		existing.info = info
		ml.logger.Info("node updated",
			"id", info.ID,
			"state", info.State.String(),
			"incarnation", info.Incarnation,
		)
		accepted = true
		if prev != info.State {
			emitTrans = true
			fromState = prev.String()
			toState = info.State.String()
		}
	}
	ml.mu.Unlock()

	if emitTrans {
		hook.OnStateChange(info.ID, fromState, toState)
	}
	return accepted
}

// MarkSuspect bir node'u suspect olarak işaretler.
// suspectTimeout sonra Dead'e geçirecek timer başlatır.
// deadCallback: node dead olduğunda çağrılır (gossip yaymak için).
// Deadlock önlemi: callback lock dışında çağrılır.
func (ml *MembershipList) MarkSuspect(
	nodeID string,
	suspectTimeout time.Duration,
	deadCallback func(nodeID string),
) {
	ml.mu.Lock()
	hook := ml.metrics
	entry, exists := ml.members[nodeID]
	if !exists || entry.info.State >= StateSuspect {
		ml.mu.Unlock()
		return
	}

	prev := entry.info.State
	entry.info.State = StateSuspect
	ml.logger.Warn("node suspect", "id", nodeID)

	// time.AfterFunc: goroutine açmadan timer — memory leak riski yok
	timer := time.AfterFunc(suspectTimeout, func() {
		ml.markDead(nodeID, deadCallback)
	})
	entry.suspectTimer = timer
	entry.suspectCancel = func() { timer.Stop() }
	ml.mu.Unlock()

	hook.OnStateChange(nodeID, prev.String(), StateSuspect.String())
}

// markDead timer callback'ten çağrılır — ayrı lock alır.
// Deadlock önlemi: callback lock dışında çağrılır.
func (ml *MembershipList) markDead(nodeID string, callback func(string)) {
	ml.mu.Lock()
	hook := ml.metrics
	entry, exists := ml.members[nodeID]
	if !exists || entry.info.State == StateDead {
		ml.mu.Unlock()
		return
	}
	prev := entry.info.State
	entry.info.State = StateDead
	entry.suspectTimer = nil
	entry.suspectCancel = nil
	entry.suspectConfirmers = nil // Dogpile votes irrelevant once dead
	ml.logger.Error("node dead", "id", nodeID)
	ml.mu.Unlock()

	// Both notifications fire outside the lock to keep slow callbacks
	// from blocking the membership table.
	hook.OnStateChange(nodeID, prev.String(), StateDead.String())
	if callback != nil {
		callback(nodeID)
	}
}

// ConfirmSuspect records a Lifeguard "Dogpile" vote and reschedules the
// suspect timer using the supplied computeTimeout function.
//
//   - If the node is unknown or already Dead, this is a no-op (returns 0,false).
//   - If the node is currently Alive, it transitions to Suspect.
//   - confirmerID dedupes votes — a confirmer ID can vote at most once.
//   - computeTimeout receives the current vote count and returns the timeout
//     to apply; this lets the caller plug in the formula without leaking
//     timing constants into the membership package.
//
// Returns the post-vote count and true on success.
func (ml *MembershipList) ConfirmSuspect(
	nodeID, confirmerID string,
	computeTimeout func(votes int) time.Duration,
	deadCallback func(string),
) (int, bool) {
	ml.mu.Lock()
	hook := ml.metrics

	entry, exists := ml.members[nodeID]
	if !exists || entry.info.State == StateDead {
		ml.mu.Unlock()
		return 0, false
	}

	if entry.suspectConfirmers == nil {
		entry.suspectConfirmers = make(map[string]bool)
	}
	entry.suspectConfirmers[confirmerID] = true
	votes := len(entry.suspectConfirmers)

	// Transition to Suspect on first vote.
	transitioned := false
	prevState := entry.info.State
	if entry.info.State == StateAlive {
		entry.info.State = StateSuspect
		transitioned = true
		ml.logger.Warn("node suspect (lifeguard)", "id", nodeID, "votes", votes)
	}

	timeout := computeTimeout(votes)

	// Reschedule timer: cancel any existing one first.
	if entry.suspectCancel != nil {
		entry.suspectCancel()
	}
	timer := time.AfterFunc(timeout, func() {
		ml.markDead(nodeID, deadCallback)
	})
	entry.suspectTimer = timer
	entry.suspectCancel = func() { timer.Stop() }

	ml.mu.Unlock()

	if transitioned {
		hook.OnStateChange(nodeID, prevState.String(), StateSuspect.String())
	}
	return votes, true
}

// GetAliveMembers load balancer için ALIVE node'ları döner.
// Filtreler: StateAlive + ServiceAddr != "" (LB'nin kendisi hariç)
// Her çağrıda kopya döner — caller state'i değiştiremez.
func (ml *MembershipList) GetAliveMembers() []NodeInfo {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	result := make([]NodeInfo, 0, len(ml.members))
	for _, entry := range ml.members {
		if entry.info.State == StateAlive &&
			entry.info.ID != ml.localID &&
			entry.info.ServiceAddr != "" {
			result = append(result, entry.info)
		}
	}
	return result
}

// GetAllMembers gossip piggyback için tüm üyeleri döner.
// Dead node'lar da dahildir — convergence için gerekli.
func (ml *MembershipList) GetAllMembers() []NodeInfo {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	result := make([]NodeInfo, 0, len(ml.members))
	for _, entry := range ml.members {
		result = append(result, entry.info)
	}
	return result
}

// GetRandomMembers K adet rastgele üye döner.
// Kendisi ve excludeIDs listesindekiler hariç, Dead node'lar hariç.
// Fisher-Yates shuffle ile uniform dağılım.
func (ml *MembershipList) GetRandomMembers(count int, excludeIDs ...string) []NodeInfo {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	exclude := make(map[string]bool, len(excludeIDs)+1)
	exclude[ml.localID] = true
	for _, id := range excludeIDs {
		exclude[id] = true
	}

	candidates := make([]NodeInfo, 0, len(ml.members))
	for _, entry := range ml.members {
		if !exclude[entry.info.ID] && entry.info.State != StateDead {
			candidates = append(candidates, entry.info)
		}
	}

	// Fisher-Yates shuffle
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	if count > len(candidates) {
		count = len(candidates)
	}
	return candidates[:count]
}

// Defend local node'un incarnation'ını artırır ve Alive'a çeker.
// Başka bir node bizi suspect/dead ilan ettiğinde çağrılır.
func (ml *MembershipList) Defend(localInfo *NodeInfo) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	entry, exists := ml.members[localInfo.ID]
	if !exists {
		return
	}
	entry.info.Incarnation++
	entry.info.State = StateAlive
	localInfo.Incarnation = entry.info.Incarnation
	ml.logger.Info("node defended self",
		"id", localInfo.ID,
		"incarnation", entry.info.Incarnation,
	)
}

// GetMember belirli bir node'un bilgisini döner.
// Bulunamazsa false döner.
func (ml *MembershipList) GetMember(nodeID string) (NodeInfo, bool) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	entry, exists := ml.members[nodeID]
	if !exists {
		return NodeInfo{}, false
	}
	return entry.info, true
}
