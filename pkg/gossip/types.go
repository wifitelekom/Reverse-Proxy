package gossip

import (
	"sync/atomic"
	"time"
)

// NodeState SWIM protokolü durum makinesi.
// iota sıralı atanır: Alive < Suspect < Dead — kıyaslama için kullanılır.
type NodeState int32

const (
	StateAlive   NodeState = iota // 0 - Normal çalışıyor
	StateSuspect                  // 1 - Şüpheli, doğrulanıyor
	StateDead                     // 2 - Ölü, trafikten çıkarıldı
)

func (s NodeState) String() string {
	switch s {
	case StateAlive:
		return "ALIVE"
	case StateSuspect:
		return "SUSPECT"
	case StateDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// NodeInfo bir gossip üyesinin anlık durum bilgisi.
// Gossip mesajlarında JSON olarak iletilir — her zaman kopya (value) olarak geçirilir.
type NodeInfo struct {
	ID            string    `json:"id"`
	Addr          string    `json:"addr"`         // UDP gossip adresi (örn: "localhost:9001")
	ServiceAddr   string    `json:"service_addr"` // HTTP servis adresi (örn: "localhost:8001")
	State         NodeState `json:"state"`
	Incarnation   uint64    `json:"incarnation"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Load          int32     `json:"load"` // aktif bağlantı sayısı
}

// MessageType gossip mesaj türleri.
type MessageType string

const (
	MsgPing    MessageType = "PING"
	MsgAck     MessageType = "ACK"
	MsgPingReq MessageType = "PING_REQ"
	MsgDead    MessageType = "DEAD"
	MsgAlive   MessageType = "ALIVE"
	MsgSuspect MessageType = "SUSPECT"
)

// GossipMessage UDP üzerinden iletilen paket yapısı.
// Members alanı piggyback gossip için kullanılır — üyelik bilgisi her mesajla taşınır.
type GossipMessage struct {
	Type     MessageType `json:"type"`
	SenderID string      `json:"sender_id"`
	TargetID string      `json:"target_id,omitempty"` // PING_REQ için dolaylı hedef
	Members  []NodeInfo  `json:"members,omitempty"`   // piggyback üyelik bilgisi
	SeqNum   uint64      `json:"seq_num"`             // ACK eşleştirmesi için
}

// AtomicIncLoad NodeInfo.Load alanını thread-safe artırır.
func AtomicIncLoad(load *int32) int32 {
	return atomic.AddInt32(load, 1)
}

// AtomicDecLoad NodeInfo.Load alanını thread-safe azaltır.
func AtomicDecLoad(load *int32) int32 {
	return atomic.AddInt32(load, -1)
}
