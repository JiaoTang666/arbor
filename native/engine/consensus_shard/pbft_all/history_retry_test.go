package pbft_all

import (
	"blockEmulator/shard"
	"testing"
	"time"
)

func TestHistoryRecoveryRetriesWithoutNotificationsAndStops(t *testing.T) {
	n := historyIngressNode(&historyTestHook{})
	n.RunningNode = &shard.Node{}
	now := time.Now()
	first := n.beginHistoryAttempt(4, &shard.Node{}, now)
	if first == nil {
		t.Fatal("initial attempt missing")
	}
	n.historyPending = nil // rejected response; no later commit notifications
	second := n.pollHistoryRecovery(now.Add(historyAttemptTTL))
	if second == nil || second.SeqStartHeight != 2 || second.RequestID == first.RequestID {
		t.Fatal("suffix retry missing")
	}
	if n.pollHistoryRecovery(now.Add(2*historyAttemptTTL)) == nil {
		t.Fatal("third attempt missing")
	}
	if n.pollHistoryRecovery(now.Add(3*historyAttemptTTL)) != nil || !n.historyRecovery.failed || n.historyPending != nil {
		t.Fatal("retry bound not enforced")
	}
	if n.beginHistoryAttempt(4, &shard.Node{}, now.Add(4*historyAttemptTTL)) != nil {
		t.Fatal("notification restarted exhausted recovery")
	}
	if !n.historyRecoveryFailed.Load() || n.stopSignal.Load() {
		t.Fatal("failure must halt consensus without impersonating completed shutdown")
	}
	seq, view := n.sequenceID, n.view.Load()
	for _, handler := range []func([]byte){n.handlePrePrepare, n.handlePrepare, n.handleCommit, n.handleSendOldSeq, n.handleViewChangeMsg, n.handleNewViewMsg} {
		handler([]byte(`{}`))
	}
	n.viewChangePropose()
	if n.sequenceID != seq || n.view.Load() != view || len(n.requestPool) != 0 || len(n.height2Digest) != 0 {
		t.Fatal("late consensus input advanced failed node")
	}
	n.view.Store(view + 1)
	if n.beginHistoryAttempt(5, &shard.Node{}, now.Add(5*historyAttemptTTL)) != nil {
		t.Fatal("view change bypassed sticky recovery failure")
	}
}
func TestHistoryRecoveryRetiresOnCompletionAndViewChange(t *testing.T) {
	for _, changeView := range []bool{false, true} {
		n := historyIngressNode(&historyTestHook{})
		now := time.Now()
		n.beginHistoryAttempt(4, &shard.Node{}, now)
		if changeView {
			n.view.Store(1)
		} else {
			n.sequenceID = 5
		}
		if n.pollHistoryRecovery(now.Add(historyAttemptTTL)) != nil || n.historyPending != nil || n.historyRecovery != nil {
			t.Fatal("finished or old-view recovery remained active")
		}
	}
}
