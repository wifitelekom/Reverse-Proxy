package gossip

import (
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// nodeEntry iç temsil — suspectTimer da burada saklanır.
// Dışarıya asla pointer verilmez, sadece NodeInfo kopyası döner.
type nodeEntry struct {
	info          NodeInfo
	suspectTimer  *time.Timer
	suspectCancel func()
}

// MembershipList thread-safe SWIM üyelik tablosudur.
// Tüm okuma/yazma işlemleri RWMutex ile korunur.
// Dışarıya SADECE snapshot (kopya) verir — caller state'i bozamaz.
type MembershipList struct {
	mu      sync.RWMutex
	members map[string]*nodeEntry
	localID string
	logger  *slog.Logger
}

// NewMembershipList yeni bir MembershipList oluşturur.
func NewMembershipList(localID string, logger *slog.Logger) *MembershipList {
	return &MembershipList{
		members: make(map[string]*nodeEntry),
		localID: localID,
		logger:  logger,
	}
}

// AddOrUpdate bir node'u ekler veya günceller.
// SWIM incarnation number kuralı:
//   - Yeni incarnation > mevcut → her zaman kabul et
//   - Aynı incarnation → daha kötü state kabul edilir (Dead > Suspect > Alive)
//   - Yeni incarnation < mevcut → stale bilgi, yoksay
//
// Dönüş: true ise state değişti, false ise yoksayıldı.
func (ml *MembershipList) AddOrUpdate(info NodeInfo) bool {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	existing, exists := ml.members[info.ID]
	if !exists {
		ml.members[info.ID] = &nodeEntry{info: info}
		ml.logger.Info("node joined", "id", info.ID, "addr", info.Addr)
		return true
	}

	if info.Incarnation < existing.info.Incarnation {
		return false // Stale bilgi
	}

	if info.Incarnation == existing.info.Incarnation {
		// Aynı incarnation: daha kötü state kazanır (Dead > Suspect > Alive)
		if info.State <= existing.info.State {
			return false
		}
	}

	// Alive geliyorsa aktif suspect timer'ı iptal et
	if info.State == StateAlive && existing.suspectCancel != nil {
		existing.suspectCancel()
		existing.suspectTimer = nil
		existing.suspectCancel = nil
	}

	existing.info = info
	ml.logger.Info("node updated",
		"id", info.ID,
		"state", info.State.String(),
		"incarnation", info.Incarnation,
	)
	return true
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
	defer ml.mu.Unlock()

	entry, exists := ml.members[nodeID]
	if !exists || entry.info.State >= StateSuspect {
		return // Zaten suspect/dead ya da bilinmiyor
	}

	entry.info.State = StateSuspect
	ml.logger.Warn("node suspect", "id", nodeID)

	// time.AfterFunc: goroutine açmadan timer — memory leak riski yok
	timer := time.AfterFunc(suspectTimeout, func() {
		ml.markDead(nodeID, deadCallback)
	})
	entry.suspectTimer = timer
	entry.suspectCancel = func() { timer.Stop() }
}

// markDead timer callback'ten çağrılır — ayrı lock alır.
// Deadlock önlemi: callback lock dışında çağrılır.
func (ml *MembershipList) markDead(nodeID string, callback func(string)) {
	ml.mu.Lock()
	entry, exists := ml.members[nodeID]
	if !exists || entry.info.State == StateDead {
		ml.mu.Unlock()
		return
	}
	entry.info.State = StateDead
	entry.suspectTimer = nil
	entry.suspectCancel = nil
	ml.logger.Error("node dead", "id", nodeID)
	ml.mu.Unlock()

	// Callback lock dışında — deadlock riski yok
	if callback != nil {
		callback(nodeID)
	}
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
