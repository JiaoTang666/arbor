package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/shard"
	"fmt"
	"time"
)

// No mutex is held across network messages. Retries are bounded by the
// recovery scheduler; exhausted recovery permanently halts this process's consensus.
const historyAttemptTTL = 10 * time.Second

type historyAttempt struct {
	id         string
	start, end uint64
	view       int32
	peer       uint64
	deadline   time.Time
}

func (n *PbftConsensusNode) beginHistoryAttempt(end uint64, peer *shard.Node, now time.Time) *message.RequestOldMessage {
	if n.historyRecoveryFailed.Load() || peer == nil || end < n.sequenceID || end == ^uint64(0) {
		return nil
	}
	n.refreshHistoryProgress()
	j := n.historyRecovery
	if j == nil || j.view != n.view.Load() || n.sequenceID > j.end {
		j = &historyRecoveryState{end: end, progress: n.sequenceID, peer: *peer, view: n.view.Load()}
		n.historyRecovery = j
	}
	if end > j.end {
		j.end = end
	}
	// Extend the recovery target, but preserve the identity/range of the
	// already issued request so its valid response remains acceptable.
	if p := n.historyPending; p != nil && now.Before(p.deadline) && p.view == n.view.Load() {
		return nil
	}
	if j.failed || j.attempts >= 3 {
		n.failHistoryRecovery()
		return nil
	}
	j.attempts++
	j.next = now.Add(historyAttemptTTL)
	end = j.end
	peer = &j.peer
	n.historyGeneration++
	id := fmt.Sprintf("%d-%d-%d-%d", n.NodeID, n.view.Load(), now.UnixNano(), n.historyGeneration)
	n.historyPending = &historyAttempt{id: id, start: n.sequenceID, end: end, view: n.view.Load(), peer: peer.NodeID, deadline: now.Add(historyAttemptTTL)}
	return &message.RequestOldMessage{RequestID: id, SeqStartHeight: n.sequenceID, SeqEndHeight: end, ServerNode: peer, SenderNode: n.RunningNode}
}
func (n *PbftConsensusNode) matchesHistoryAttempt(s *message.SendOldMessage, now time.Time) bool {
	p := n.historyPending
	if p == nil {
		return false
	}
	if !now.Before(p.deadline) || p.view != n.view.Load() {
		n.historyPending = nil
		return false
	}
	return s != nil && s.RequestID == p.id && s.SeqStartHeight == p.start && s.SeqEndHeight == p.end && s.SenderNode != nil && s.SenderNode.NodeID == p.peer && s.SenderNode.ShardID == n.ShardID
}
