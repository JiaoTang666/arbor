package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/shard"
	"encoding/json"
	"time"
)

type historyRecoveryState struct {
	end      uint64
	progress uint64 // next sequence after the most recent real state application
	peer     shard.Node
	view     int32
	attempts int
	next     time.Time
	failed   bool
}

// Called under pbftLock. Count retries only while execution makes no progress.
// New messages or a larger recovery target do not replenish the budget.
func (n *PbftConsensusNode) refreshHistoryProgress() {
	j := n.historyRecovery
	if j == nil || j.failed {
		return
	}
	if n.sequenceID > j.end {
		n.historyRecovery, n.historyPending = nil, nil
		return
	}
	if n.sequenceID > j.progress {
		j.progress = n.sequenceID
		j.attempts = 0
		j.next = time.Time{}
	}
}

// Pure scheduling under pbftLock; deterministic tests advance time explicitly.
func (n *PbftConsensusNode) pollHistoryRecovery(now time.Time) *message.RequestOldMessage {
	n.refreshHistoryProgress()
	j := n.historyRecovery
	if j == nil || j.failed {
		return nil
	}
	if n.stopSignal.Load() || j.view != n.view.Load() || n.sequenceID > j.end {
		n.historyRecovery = nil
		n.historyPending = nil
		return nil
	}
	if now.Before(j.next) {
		return nil
	}
	n.historyPending = nil
	if j.attempts >= 3 {
		n.failHistoryRecovery()
		return nil
	}
	return n.beginHistoryAttempt(j.end, &j.peer, now)
}

// Called under pbftLock. Keep storage open for diagnosis and normal shutdown,
// but never let a view change or late message resume this node's consensus.
func (n *PbftConsensusNode) failHistoryRecovery() {
	n.historyRecoveryFailed.Store(true)
	n.historyPending = nil
	if j := n.historyRecovery; j != nil {
		j.failed = true
		n.pl.Plog.Printf("HISTORY_RECOVERY_FAILED start=%d end=%d attempts=%d consensus_halted=true", n.sequenceID, j.end, j.attempts)
	}
	n.releaseSequenceLockIfHeld("history_recovery_failed")
	n.conditionalVarpbftLock.Broadcast()
}

func (n *PbftConsensusNode) runHistoryRecovery() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for now := range ticker.C {
		if n.stopSignal.Load() {
			return
		}
		n.pbftLock.Lock()
		request := n.pollHistoryRecovery(now)
		n.pbftLock.Unlock()
		if request != nil {
			data, err := json.Marshal(request)
			if err != nil {
				continue
			}
			// Do not hold consensus locks during connection establishment or sending.
			if err := networks.SendHistoryBounded(message.MergeMessage(message.CRequestOldrequest, data), request.ServerNode.IPaddr, 5*time.Second); err != nil {
				n.pl.Plog.Printf("history retry send failed: %v", err)
			}
		}
	}
}
