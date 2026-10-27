package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/shard"
	"testing"
	"time"
)

func TestHistoryAttemptRangeTimeoutAndView(t *testing.T) {
	n := historyIngressNode(&historyTestHook{accept: true})
	peer := &shard.Node{}
	now := time.Now()
	first := n.beginHistoryAttempt(4, peer, now)
	if first == nil || first.SeqStartHeight != 2 || first.SeqEndHeight != 4 {
		t.Fatal("request does not cover next uncommitted sequence")
	}
	if n.beginHistoryAttempt(4, peer, now) != nil {
		t.Fatal("duplicate outstanding request")
	}
	second := n.beginHistoryAttempt(4, peer, now.Add(historyAttemptTTL))
	if second == nil || second.RequestID == first.RequestID {
		t.Fatal("timeout did not permit fresh attempt")
	}
	stale := &message.SendOldMessage{RequestID: first.RequestID, SeqStartHeight: 2, SeqEndHeight: 4, SenderNode: peer}
	if n.matchesHistoryAttempt(stale, now.Add(historyAttemptTTL)) {
		t.Fatal("stale response accepted")
	}
	n.view.Store(1)
	if n.matchesHistoryAttempt(&message.SendOldMessage{RequestID: second.RequestID, SeqStartHeight: 2, SeqEndHeight: 4, SenderNode: peer}, now.Add(historyAttemptTTL)) {
		t.Fatal("old view accepted")
	}
	if n.historyPending != nil {
		t.Fatal("view mismatch retained attempt")
	}
	n.sequenceID = 3
	next := n.beginHistoryAttempt(4, peer, now.Add(historyAttemptTTL))
	if next == nil || next.SeqStartHeight != 3 {
		t.Fatal("retry did not use successful-prefix progress")
	}
}
