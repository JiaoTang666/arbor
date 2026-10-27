package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/shard"
	"encoding/json"
	"testing"
	"time"
)

func historyReply(request *message.RequestOldMessage, peer *shard.Node) []byte {
	s := message.SendOldMessage{RequestID: request.RequestID, SeqStartHeight: request.SeqStartHeight,
		SeqEndHeight: request.SeqEndHeight, SenderNode: peer}
	for seq := s.SeqStartHeight; seq <= s.SeqEndHeight; seq++ {
		s.OldRequest = append(s.OldRequest, &message.Request{RequestType: message.BlockRequest,
			ReqTime: time.Unix(int64(seq), 0)})
	}
	data, _ := json.Marshal(s)
	return data
}

func TestHistoryResponseAfterProgressRetriesOnlySuffix(t *testing.T) {
	h := &historyTestHook{accept: true}
	n := historyIngressNode(h)
	peer, now := &shard.Node{}, time.Now()
	first := n.beginHistoryAttempt(4, peer, now)
	n.recordHistoricalProgress(&message.Request{RequestType: message.BlockRequest}, 2)
	n.handleSendOldSeq(historyReply(first, peer))
	if h.calls != 0 || n.sequenceID != 3 || n.historyRecovery.attempts != 0 {
		t.Fatal("late overlapping reply was reapplied or consumed no-progress budget")
	}
	second := n.pollHistoryRecovery(time.Now().Add(time.Second))
	if second == nil || second.SeqStartHeight != 3 || second.SeqEndHeight != 4 {
		t.Fatal("fresh suffix was not requested")
	}
	n.handleSendOldSeq(historyReply(second, peer))
	if h.calls != 1 || n.sequenceID != 5 || n.historyRecovery != nil || n.historyPending != nil {
		t.Fatal("valid suffix did not finish recovery")
	}
	n.handleSendOldSeq(historyReply(second, peer))
	if h.calls != 1 || n.sequenceID != 5 {
		t.Fatal("duplicate suffix replayed state")
	}
}

func TestHistoryLateReplyAfterCompleteProgressIsIgnored(t *testing.T) {
	h := &historyTestHook{accept: true}
	n := historyIngressNode(h)
	peer := &shard.Node{}
	request := n.beginHistoryAttempt(2, peer, time.Now())
	n.recordHistoricalProgress(&message.Request{RequestType: message.BlockRequest}, 2)
	n.handleSendOldSeq(historyReply(request, peer))
	if h.calls != 0 || n.sequenceID != 3 || n.historyRecovery != nil || n.historyPending != nil {
		t.Fatal("completed recovery remained live or re-executed")
	}
}

func TestHistoryProgressResetsBudgetButTargetGrowthDoesNot(t *testing.T) {
	n := historyIngressNode(&historyTestHook{})
	peer, now := &shard.Node{}, time.Now()
	n.beginHistoryAttempt(20, peer, now)
	for step := 1; step <= 5; step++ {
		n.sequenceID++ // simulate successful normal contiguous commits
		request := n.pollHistoryRecovery(now.Add(time.Duration(step) * historyAttemptTTL))
		if request == nil || n.historyRecovery.attempts != 1 || n.historyRecoveryFailed.Load() {
			t.Fatal("real progress did not reset recovery budget")
		}
	}
	for step := 6; step <= 7; step++ {
		if n.beginHistoryAttempt(uint64(20+step), peer, now.Add(time.Duration(step)*historyAttemptTTL)) == nil {
			t.Fatal("missing bounded no-progress retry")
		}
	}
	if n.pollHistoryRecovery(now.Add(8*historyAttemptTTL)) != nil || !n.historyRecoveryFailed.Load() {
		t.Fatal("target growth bypassed the no-progress bound")
	}
}

func TestNormalCommitReplenishesHistoryBudget(t *testing.T) {
	h := &orderedCommitHook{acceptCommit: true}
	n, _ := orderTestNode(t, h)
	r := &message.Request{RequestType: message.BlockRequest, ReqTime: time.Unix(10, 0)}
	digest := getDigest(r)
	n.requestPool[string(digest)] = r
	n.height2Digest[10] = string(digest)
	n.currentPreparedSequence, n.currentPreparedDigest = 10, string(digest)
	n.pbftStage.Store(3)
	peer := &shard.Node{ShardID: 0, NodeID: 0}
	request := n.beginHistoryAttempt(12, peer, time.Now())
	n.historyRecovery.attempts = 3 // the next timeout would halt without progress
	for _, id := range []uint64{0, 2, 3} {
		n.handleCommit(orderJSON(t, &message.Commit{Digest: digest, SeqID: 10,
			SenderNode: &shard.Node{ShardID: 0, NodeID: id}}))
	}
	if h.commitCalls != 1 || n.sequenceID != 11 || n.historyRecovery.attempts != 0 || n.historyRecoveryFailed.Load() {
		t.Fatal("normal successful commit did not reset no-progress budget")
	}
	n.handleSendOldSeq(historyReply(request, peer))
	if n.sequenceID != 11 || n.historyPending != nil || n.historyRecoveryFailed.Load() {
		t.Fatal("late overlapping response changed normal commit progress")
	}
}
