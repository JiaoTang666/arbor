package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/shard"
	"fmt"
	"os"
	"time"
)

func (p *PbftConsensusNode) validConsensusMember(n *shard.Node) bool {
	if n == nil || n.ShardID != p.ShardID {
		return false
	}
	_, ok := p.ip_nodeTable[p.ShardID][n.NodeID]
	return ok
}

func (p *PbftConsensusNode) voteKey(sequence uint64, digest []byte) string {
	return fmt.Sprintf("%d/%d/%x", p.view.Load(), sequence, digest)
}

// All round transitions run under pbftLock. Seeing a proposal is not the same
// as validating it at the current ledger head. Only the latter enables voting.
func (p *PbftConsensusNode) acceptPrePrepareLocked(pp *message.PrePrepare) bool {
	if pp.RequestMsg == nil || pp.SeqID != p.sequenceID || string(getDigest(pp.RequestMsg)) != string(pp.Digest) {
		return false
	}
	digest := string(pp.Digest)
	if p.currentPreparedSequence == pp.SeqID {
		return p.currentPreparedDigest == digest // duplicate: no second phase transition
	}
	if old, ok := p.height2Digest[pp.SeqID]; ok && old != digest {
		return false
	}
	if !p.ihm.HandleinPrePrepare(pp) {
		return false
	}
	p.requestPool[digest] = pp.RequestMsg
	p.height2Digest[pp.SeqID] = digest
	p.currentPreparedSequence, p.currentPreparedDigest = pp.SeqID, digest
	p.pbftStage.Store(2)
	p.conditionalVarpbftLock.Broadcast()
	pre := message.Prepare{Digest: pp.Digest, SeqID: pp.SeqID, SenderNode: p.RunningNode}
	frame := message.MergeMessage(message.CPrepare, epochJSON(pre))
	p.broadcastConsensus(frame)
	return true
}

func (p *PbftConsensusNode) broadcastConsensus(frame []byte) {
	if os.Getenv("BLOCK_EMULATOR_CLPA_EPOCH_V1") != "1" {
		networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), frame)
		networks.TcpDial(frame, p.RunningNode.IPaddr)
		return
	}
	for _, address := range p.getNeighborNodes() {
		p.sendEpochFrame(frame, address)
	}
}

func (p *PbftConsensusNode) sendEpochFrame(frame []byte, address string) {
	kind, _ := message.SplitMessage(frame)
	networks.TcpDialObserved(frame, address, func(_ string, _ time.Time, err error) {
		if err != nil && !p.stopSignal.Load() {
			panic(fmt.Errorf("epoch send kind=%s peer=%s: %w", kind, address, err))
		}
	})
}

func (p *PbftConsensusNode) preparedCurrent(digest []byte) bool {
	return p.currentPreparedSequence == p.sequenceID && p.currentPreparedDigest == string(digest)
}

func (p *PbftConsensusNode) resumeCachedPrePrepareLocked() {
	if digest, ok := p.height2Digest[p.sequenceID]; ok {
		if r := p.requestPool[digest]; r != nil {
			p.acceptPrePrepareLocked(&message.PrePrepare{RequestMsg: r, Digest: []byte(digest), SeqID: p.sequenceID})
		}
	}
}

// A future quorum can start recovery even if its proposal was cached and the
// missing current round never reached Prepare. Network IO is outside the lock.
func (p *PbftConsensusNode) requestCommittedHistoryLocked(end uint64) {
	leader := uint64(p.view.Load())
	peer := &shard.Node{ShardID: p.ShardID, NodeID: leader, IPaddr: p.ip_nodeTable[p.ShardID][leader]}
	req := p.beginHistoryAttempt(end, peer, time.Now())
	if req == nil {
		return
	}
	frame := message.MergeMessage(message.CRequestOldrequest, epochJSON(req))
	p.pl.Plog.Printf("history requested start=%d end=%d", req.SeqStartHeight, req.SeqEndHeight)
	go func() {
		if err := networks.SendHistoryBounded(frame, peer.IPaddr, 5*time.Second); err != nil {
			p.pl.Plog.Printf("history initial send failed: %v", err)
		}
	}()
}

// Only successful contiguous applications are eligible to serve as history.
func (p *PbftConsensusNode) committedHistory(start, end uint64) []*message.Request {
	if start > end || end == ^uint64(0) {
		return nil
	}
	var requests []*message.Request
	for seq := start; seq <= end; seq++ {
		digest, ok := p.height2Digest[seq]
		if !ok || !p.isReply[digest] || seq >= p.sequenceID || p.requestPool[digest] == nil {
			break
		}
		requests = append(requests, p.requestPool[digest])
	}
	return requests
}
