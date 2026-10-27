package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/message"
	"blockEmulator/shard"
	"encoding/json"
	"io"
	"log"
	"sync"
	"testing"
	"time"
)

type historyTestHook struct {
	ExtraOpInConsensus
	calls  int
	accept bool
}

func (h *historyTestHook) HandleforSequentialRequest(*message.SendOldMessage) bool {
	h.calls++
	return h.accept
}

func TestHistoryIngressRejectsBeforeMutation(t *testing.T) {
	r := &message.Request{RequestType: message.BlockRequest}
	cases := map[string]*message.SendOldMessage{
		"short":    {SeqStartHeight: 2, SeqEndHeight: 3, OldRequest: []*message.Request{r}},
		"reversed": {SeqStartHeight: 3, SeqEndHeight: 2},
		"nil":      {SeqStartHeight: 2, SeqEndHeight: 2, OldRequest: []*message.Request{nil}},
		"gap":      {SeqStartHeight: 3, SeqEndHeight: 3, OldRequest: []*message.Request{r}},
		"stale":    {SeqStartHeight: 1, SeqEndHeight: 1, OldRequest: []*message.Request{r}},
	}
	for name, som := range cases {
		t.Run(name, func(t *testing.T) {
			h := &historyTestHook{accept: true}
			n := historyIngressNode(h)
			data := historyResponseForTest(n, som)
			n.handleSendOldSeq(data)
			if h.calls != 0 || n.sequenceID != 2 || len(n.requestPool) != 0 || len(n.height2Digest) != 0 || len(n.isReply) != 0 || n.hasCommittedBlock.Load() || n.lastCommitTime.Load() != 0 {
				t.Fatal("invalid history mutated ingress state or reached execution")
			}
		})
	}
}
func historyIngressNode(h *historyTestHook) *PbftConsensusNode {
	n := &PbftConsensusNode{sequenceID: 2, ihm: h, requestPool: make(map[string]*message.Request), height2Digest: make(map[uint64]string), isReply: make(map[string]bool), pl: &pbft_log.PbftLog{Plog: log.New(io.Discard, "", 0)}}
	n.conditionalVarpbftLock = *sync.NewCond(&n.pbftLock)
	return n
}
func TestHistoryIngressHonorsHookRejection(t *testing.T) {
	h := &historyTestHook{}
	n := historyIngressNode(h)
	data := historyResponseForTest(n, &message.SendOldMessage{SeqStartHeight: 2, SeqEndHeight: 2, OldRequest: []*message.Request{{RequestType: message.BlockRequest}}})
	n.handleSendOldSeq(data)
	if h.calls != 1 || n.sequenceID != 2 || len(n.requestPool) != 0 || len(n.height2Digest) != 0 || len(n.isReply) != 0 || n.hasCommittedBlock.Load() {
		t.Fatal("hook rejection ignored")
	}
}

func TestHistoryIngressAcceptsContiguousThenIgnoresReplay(t *testing.T) {
	h := &historyTestHook{accept: true}
	n := historyIngressNode(h)
	r := &message.Request{RequestType: message.BlockRequest}
	data := historyResponseForTest(n, &message.SendOldMessage{SeqStartHeight: 2, SeqEndHeight: 2, OldRequest: []*message.Request{r}})
	n.handleSendOldSeq(data)
	if h.calls != 1 || n.sequenceID != 3 || len(n.requestPool) != 1 || len(n.height2Digest) != 1 || len(n.isReply) != 1 || !n.hasCommittedBlock.Load() {
		t.Fatal("valid history did not update ingress")
	}
	n.handleSendOldSeq(data)
	if h.calls != 1 || n.sequenceID != 3 || len(n.requestPool) != 1 || len(n.height2Digest) != 1 {
		t.Fatal("replay re-executed or regressed sequence")
	}
}

func historyResponseForTest(n *PbftConsensusNode, s *message.SendOldMessage) []byte {
	peer := &shard.Node{ShardID: n.ShardID, NodeID: 0}
	end := s.SeqEndHeight
	if end < n.sequenceID {
		end = n.sequenceID
	}
	request := n.beginHistoryAttempt(end, peer, time.Now())
	s.RequestID = request.RequestID
	s.SenderNode = peer
	data, _ := json.Marshal(s)
	return data
}
