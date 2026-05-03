package gossip

import (
	"sync/atomic"
	"time"
)

// NodeState is the SWIM protocol state machine. iota-assigned in
// order so that comparison expresses worsening severity:
// Alive < Suspect < Dead.
type NodeState int32

const (
	StateAlive   NodeState = iota // 0 - operating normally
	StateSuspect                  // 1 - under suspicion, being verified
	StateDead                     // 2 - dead, removed from rotation
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

// NodeInfo is the snapshot of a gossip member's state. It is
// transmitted as JSON over UDP and always passed by value (a copy)
// so callers cannot mutate state held inside the membership table.
type NodeInfo struct {
	ID            string    `json:"id"`
	Addr          string    `json:"addr"`         // UDP gossip address (e.g. "localhost:9001")
	ServiceAddr   string    `json:"service_addr"` // HTTP service address (e.g. "localhost:8001")
	State         NodeState `json:"state"`
	Incarnation   uint64    `json:"incarnation"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Load          int32     `json:"load"` // active connection count
}

// MessageType identifies the gossip message kinds.
type MessageType string

const (
	MsgPing    MessageType = "PING"
	MsgAck     MessageType = "ACK"
	MsgPingReq MessageType = "PING_REQ"
	MsgDead    MessageType = "DEAD"
	MsgAlive   MessageType = "ALIVE"
	MsgSuspect MessageType = "SUSPECT"
)

// GossipMessage is the wire format for UDP gossip packets. The
// Members field carries piggyback gossip — a snapshot of membership
// is shipped with every message so updates spread without dedicated
// fan-out broadcasts.
type GossipMessage struct {
	Type     MessageType `json:"type"`
	SenderID string      `json:"sender_id"`
	TargetID string      `json:"target_id,omitempty"` // indirect target for PING_REQ
	Members  []NodeInfo  `json:"members,omitempty"`   // piggyback membership snapshot
	SeqNum   uint64      `json:"seq_num"`             // for ACK matching
}

// AtomicIncLoad atomically increments the supplied NodeInfo.Load.
func AtomicIncLoad(load *int32) int32 {
	return atomic.AddInt32(load, 1)
}

// AtomicDecLoad atomically decrements the supplied NodeInfo.Load.
func AtomicDecLoad(load *int32) int32 {
	return atomic.AddInt32(load, -1)
}
