package gossip

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"p2p-loadbalancer/pkg/types"
)

// Protocol SWIM protokol döngüsünü yönetir.
// 2 goroutine: gossipLoop (ping/ack döngüsü) + handleMessages (gelen mesaj işleyici)
// Bu iki goroutine pendingPings (sync.Map) üzerinden ACK'ları koordine eder.
type Protocol struct {
	config     types.ProtocolConfig
	membership *MembershipList
	transport  *Transport
	localInfo  NodeInfo
	seqCounter uint64   // atomic artan sequence number
	pendingPings sync.Map // map[uint64]chan bool — uçuşta PING'ler
	logger     *slog.Logger
}

// NewProtocol yeni bir SWIM protokol instance'ı oluşturur.
func NewProtocol(
	cfg types.ProtocolConfig,
	ml *MembershipList,
	transport *Transport,
	localInfo NodeInfo,
	logger *slog.Logger,
) *Protocol {
	return &Protocol{
		config:    cfg,
		membership: ml,
		transport: transport,
		localInfo: localInfo,
		logger:    logger,
	}
}

// Start iki goroutine başlatır — non-blocking.
// ctx iptal edildiğinde her iki goroutine da temiz çıkar.
func (p *Protocol) Start(ctx context.Context) {
	go p.gossipLoop(ctx)
	go p.handleMessages(ctx)
}

// gossipLoop T_protocol aralığında çalışan ana döngü.
// Her tick'te bir gossip round çalıştırır.
func (p *Protocol) gossipLoop(ctx context.Context) {
	ticker := time.NewTicker(p.config.ProtocolInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.runProtocolRound(ctx)
		}
	}
}

// runProtocolRound tek bir SWIM protokol round'unu yürütür.
// Direct ping → timeout → indirect ping → timeout → suspect
func (p *Protocol) runProtocolRound(ctx context.Context) {
	targets := p.membership.GetRandomMembers(1)
	if len(targets) == 0 {
		return // Ağda başka üye yok
	}
	target := targets[0]
	seqNum := p.nextSeq()

	// ACK kanalı: buffer=1, en fazla 1 ACK bekliyoruz
	replyCh := make(chan bool, 1)
	p.pendingPings.Store(seqNum, replyCh)

	pingMsg := GossipMessage{
		Type:     MsgPing,
		SenderID: p.localInfo.ID,
		TargetID: target.ID,
		SeqNum:   seqNum,
		Members:  p.membership.GetAllMembers(), // piggyback
	}

	if err := p.transport.Send(target.Addr, pingMsg); err != nil {
		p.logger.Error("ping send failed", "target", target.ID, "err", err)
		p.pendingPings.Delete(seqNum)
		return
	}

	// Adım 1: T_ping timeout ile direkt ACK bekle
	directTimer := time.NewTimer(p.config.PingTimeout)
	defer directTimer.Stop()

	select {
	case <-ctx.Done():
		p.pendingPings.Delete(seqNum)
		return
	case <-replyCh:
		p.pendingPings.Delete(seqNum)
		p.logger.Debug("direct ACK received", "target", target.ID)
		return
	case <-directTimer.C:
		// Direct ping timeout — indirect'e geç
	}
	p.pendingPings.Delete(seqNum)

	// Adım 2: K adet araca PING_REQ gönder
	intermediaries := p.membership.GetRandomMembers(p.config.IndirectNodes, target.ID)
	if len(intermediaries) == 0 {
		p.suspectNode(target.ID)
		return
	}

	indirectSeq := p.nextSeq()
	// buffer = K: her aracı bir kez cevap verebilir
	indirectCh := make(chan bool, len(intermediaries))
	p.pendingPings.Store(indirectSeq, indirectCh)

	pingReqMsg := GossipMessage{
		Type:     MsgPingReq,
		SenderID: p.localInfo.ID,
		TargetID: target.ID,
		SeqNum:   indirectSeq,
	}
	for _, inter := range intermediaries {
		if err := p.transport.Send(inter.Addr, pingReqMsg); err != nil {
			p.logger.Error("ping-req send failed", "intermediary", inter.ID, "err", err)
		}
	}

	// Adım 3: T_indirect timeout ile dolaylı ACK bekle
	indirectTimer := time.NewTimer(p.config.IndirectTimeout)
	defer indirectTimer.Stop()

	select {
	case <-ctx.Done():
		p.pendingPings.Delete(indirectSeq)
		return
	case <-indirectCh:
		p.pendingPings.Delete(indirectSeq)
		p.logger.Debug("indirect ACK received", "target", target.ID)
		return
	case <-indirectTimer.C:
		// Indirect ping de timeout — suspect yap
	}
	p.pendingPings.Delete(indirectSeq)

	p.suspectNode(target.ID)
}

// suspectNode bir node'u suspect ilan eder ve gossip yayar.
func (p *Protocol) suspectNode(nodeID string) {
	p.logger.Warn("marking node suspect", "id", nodeID)

	p.membership.MarkSuspect(
		nodeID,
		p.config.SuspectTimeout,
		p.gossipDead, // timer dolduğunda çağrılır
	)

	suspectMsg := GossipMessage{
		Type:     MsgSuspect,
		SenderID: p.localInfo.ID,
		TargetID: nodeID,
		SeqNum:   p.nextSeq(),
		Members:  p.membership.GetAllMembers(),
	}
	p.broadcastToK(suspectMsg)
}

// gossipDead bir node dead olduğunda tüm komşulara yayar.
// MembershipList timer callback'inden çağrılır.
func (p *Protocol) gossipDead(nodeID string) {
	deadMsg := GossipMessage{
		Type:     MsgDead,
		SenderID: p.localInfo.ID,
		TargetID: nodeID,
		SeqNum:   p.nextSeq(),
		Members:  p.membership.GetAllMembers(),
	}
	p.broadcastToK(deadMsg)
}

// broadcastToK rastgele K üyeye mesaj gönderir (gossip fan-out).
func (p *Protocol) broadcastToK(msg GossipMessage) {
	targets := p.membership.GetRandomMembers(p.config.IndirectNodes)
	for _, t := range targets {
		if err := p.transport.Send(t.Addr, msg); err != nil {
			p.logger.Error("broadcast failed", "target", t.ID, "err", err)
		}
	}
}

// handleMessages transport'tan gelen mesajları işler — ayrı goroutine.
func (p *Protocol) handleMessages(ctx context.Context) {
	recvCh := p.transport.Receive()

	for {
		select {
		case <-ctx.Done():
			return
		case received, ok := <-recvCh:
			if !ok {
				return // channel kapatıldı
			}
			p.processMessage(received)
		}
	}
}

// processMessage gelen mesaj türüne göre dispatch eder.
// Piggyback üyelik bilgisi her mesajda önce işlenir.
func (p *Protocol) processMessage(received ReceivedMessage) {
	msg := received.Msg

	// Piggyback üyelik bilgisini işle
	for _, memberInfo := range msg.Members {
		if memberInfo.ID == p.localInfo.ID {
			// Kendimiz hakkında kötü gossip → incarnation++ ile savun
			if memberInfo.State > StateAlive {
				p.membership.Defend(&p.localInfo)
				p.announceAlive()
			}
			continue
		}
		p.membership.AddOrUpdate(memberInfo)
	}

	switch msg.Type {
	case MsgPing:
		p.handlePing(received.From, msg)
	case MsgAck:
		p.handleAck(msg)
	case MsgPingReq:
		p.handlePingReq(msg)
	case MsgDead:
		p.handleDeadMsg(msg)
	case MsgSuspect:
		p.handleSuspectMsg(msg)
	case MsgAlive:
		// Piggyback zaten işlendi
	}
}

// handlePing PING'e ACK döner, kendi membership listesini piggyback eder.
func (p *Protocol) handlePing(from *net.UDPAddr, msg GossipMessage) {
	ack := GossipMessage{
		Type:     MsgAck,
		SenderID: p.localInfo.ID,
		TargetID: msg.SenderID,
		SeqNum:   msg.SeqNum,
		Members:  p.membership.GetAllMembers(),
	}
	if err := p.transport.Send(from.String(), ack); err != nil {
		p.logger.Error("ACK send failed", "err", err)
	}
}

// handleAck bekleyen PING için channel'a bildirim gönderir.
func (p *Protocol) handleAck(msg GossipMessage) {
	if ch, loaded := p.pendingPings.LoadAndDelete(msg.SeqNum); loaded {
		replyCh := ch.(chan bool)
		// Non-blocking send — zaten timeout olmuş olabilir
		select {
		case replyCh <- true:
		default:
		}
	}
}

// handlePingReq: aracı olarak hedef node'a ping at, sonucu requester'a ilet.
// Ayrı goroutine'da çalışır — blocking ping içerir.
func (p *Protocol) handlePingReq(msg GossipMessage) {
	go func() {
		// Hedef node adresini bul
		target, found := p.membership.GetMember(msg.TargetID)
		if !found {
			return
		}

		// Requester adresini bul
		requester, found := p.membership.GetMember(msg.SenderID)
		if !found {
			return
		}

		// Hedef için ping at
		indirectSeq := p.nextSeq()
		replyCh := make(chan bool, 1)
		p.pendingPings.Store(indirectSeq, replyCh)
		defer p.pendingPings.Delete(indirectSeq)

		pingMsg := GossipMessage{
			Type:     MsgPing,
			SenderID: p.localInfo.ID,
			TargetID: msg.TargetID,
			SeqNum:   indirectSeq,
		}
		if err := p.transport.Send(target.Addr, pingMsg); err != nil {
			return
		}

		// T_indirect timeout bekle
		timer := time.NewTimer(p.config.IndirectTimeout)
		defer timer.Stop()

		select {
		case <-replyCh:
			// Hedef cevap verdi — requester'a ACK ilet
			ackMsg := GossipMessage{
				Type:     MsgAck,
				SenderID: p.localInfo.ID,
				TargetID: msg.SenderID,
				SeqNum:   msg.SeqNum, // orijinal seq num
			}
			p.transport.Send(requester.Addr, ackMsg)
		case <-timer.C:
			// Hedef cevap vermedi — sessiz kal (requester kendi timeout'unu bekler)
		}
	}()
}

// handleDeadMsg: gelen DEAD mesajını membership'e uygula.
func (p *Protocol) handleDeadMsg(msg GossipMessage) {
	if msg.TargetID == p.localInfo.ID {
		// Bizi dead ilan ettiler — savun
		p.membership.Defend(&p.localInfo)
		p.announceAlive()
		return
	}
	// TargetID'nin state'ini güncelle — piggyback zaten işlendi
	// Ama explicit DEAD mesajı için de uygula
	deadInfo := NodeInfo{
		ID:    msg.TargetID,
		State: StateDead,
	}
	if existing, found := p.membership.GetMember(msg.TargetID); found {
		// Mevcut kayıt var — incarnation'ı koru
		deadInfo.Addr = existing.Addr
		deadInfo.ServiceAddr = existing.ServiceAddr
		deadInfo.Incarnation = existing.Incarnation
	} else {
		// Bilinmeyen node — piggyback Members'tan addr/incarnation bul
		for _, m := range msg.Members {
			if m.ID == msg.TargetID {
				deadInfo.Addr = m.Addr
				deadInfo.ServiceAddr = m.ServiceAddr
				deadInfo.Incarnation = m.Incarnation
				break
			}
		}
		if deadInfo.Addr == "" {
			// Adres bilgisi yoksa ekleyemeyiz — warning logla
			p.logger.Warn("dead msg for unknown node without addr info", "target", msg.TargetID)
			return
		}
	}
	p.membership.AddOrUpdate(deadInfo)
}

// handleSuspectMsg: gelen SUSPECT mesajını membership'e uygula.
func (p *Protocol) handleSuspectMsg(msg GossipMessage) {
	if msg.TargetID == p.localInfo.ID {
		// Bizi suspect ilan ettiler — savun
		p.membership.Defend(&p.localInfo)
		p.announceAlive()
		return
	}
	if existing, found := p.membership.GetMember(msg.TargetID); found {
		p.membership.MarkSuspect(msg.TargetID, p.config.SuspectTimeout, p.gossipDead)
		_ = existing
	}
}

// announceAlive kendimizin ALIVE olduğunu gossip ile yayar.
func (p *Protocol) announceAlive() {
	aliveMsg := GossipMessage{
		Type:     MsgAlive,
		SenderID: p.localInfo.ID,
		SeqNum:   p.nextSeq(),
		Members:  []NodeInfo{p.localInfo},
	}
	p.broadcastToK(aliveMsg)
}

// nextSeq thread-safe sequence number üretir.
func (p *Protocol) nextSeq() uint64 {
	return atomic.AddUint64(&p.seqCounter, 1)
}
