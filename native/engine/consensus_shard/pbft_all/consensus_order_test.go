package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/message"
	"blockEmulator/shard"
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"
)

type orderedCommitHook struct {
	ExtraOpInConsensus
	commitCalls  int
	acceptCommit bool
}

func (h *orderedCommitHook) HandleinPrePrepare(*message.PrePrepare) bool { return true }
func (h *orderedCommitHook) HandleinPrepare(*message.Prepare) bool       { return true }
func (h *orderedCommitHook) HandleinCommit(*message.Commit) bool {
	h.commitCalls++
	return h.acceptCommit
}
func (h *orderedCommitHook) HandleforSequentialRequest(*message.SendOldMessage) bool { return true }

type orderTestFrame struct {
	kind    message.MessageType
	content []byte
}

// A local sink captures production transport frames. Tests explicitly deliver
// the captured self Prepare, so no consensus correctness depends on scheduling.
func orderTestNode(t *testing.T, hook *orderedCommitHook) (*PbftConsensusNode, <-chan orderTestFrame) {
	t.Helper()
	t.Setenv("BLOCK_EMULATOR_CLPA_EPOCH_V1", "1")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	frames := make(chan orderTestFrame, 32)
	acceptDone := make(chan struct{})
	var connectionsMu sync.Mutex
	var connections []net.Conn
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connectionsMu.Lock()
			connections = append(connections, conn)
			connectionsMu.Unlock()
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					frame, err := reader.ReadBytes('\n')
					if err != nil {
						return
					}
					if len(frame) < 31 {
						continue
					}
					kind, content := message.SplitMessage(frame[:len(frame)-1])
					select {
					case frames <- orderTestFrame{kind, append([]byte(nil), content...)}:
					default:
					}
				}
			}()
		}
	}()
	address := listener.Addr().String()
	n := &PbftConsensusNode{
		RunningNode: &shard.Node{NodeID: 1, ShardID: 0, IPaddr: address},
		ShardID:     0, NodeID: 1, node_nums: 4, malicious_nums: 1,
		ihm: hook, sequenceID: 10,
		requestPool:       make(map[string]*message.Request),
		height2Digest:     make(map[uint64]string),
		cntPrepareConfirm: make(map[string]map[*shard.Node]bool),
		cntCommitConfirm:  make(map[string]map[*shard.Node]bool),
		isCommitBordcast:  make(map[string]bool), isReply: make(map[string]bool),
		ip_nodeTable: map[uint64]map[uint64]string{0: {0: address, 1: address, 2: address, 3: address}},
		pl:           &pbft_log.PbftLog{Plog: log.New(io.Discard, "", 0)},
	}
	n.conditionalVarpbftLock = *sync.NewCond(&n.pbftLock)
	n.pbftStage.Store(1)
	t.Cleanup(func() {
		n.stopSignal.Store(true)
		n.pbftLock.Lock()
		n.conditionalVarpbftLock.Broadcast()
		n.pbftLock.Unlock()
		listener.Close()
		<-acceptDone
		connectionsMu.Lock()
		for _, conn := range connections {
			conn.Close()
		}
		connectionsMu.Unlock()
	})
	return n, frames
}

func orderJSON(t *testing.T, value interface{}) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func orderCall(t *testing.T, n *PbftConsensusNode, handler func([]byte), raw []byte) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); handler(raw) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		n.stopSignal.Store(true)
		n.pbftLock.Lock()
		n.conditionalVarpbftLock.Broadcast()
		n.pbftLock.Unlock()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		t.Fatal("consensus handler waited for a missing earlier round instead of progressing or deferring")
	}
}

func orderRequest(label string) *message.Request {
	return &message.Request{RequestType: message.BlockRequest, Msg: message.RawMessage{Content: []byte(label)}}
}

func orderCommit(t *testing.T, n *PbftConsensusNode, r *message.Request, sequence, member uint64) {
	t.Helper()
	orderCall(t, n, n.handleCommit, orderJSON(t, &message.Commit{
		Digest: getDigest(r), SeqID: sequence,
		SenderNode: &shard.Node{ShardID: 0, NodeID: member, IPaddr: n.ip_nodeTable[0][member]},
	}))
}

func TestCachedFutureCommitStartsHistoryWithoutExecution(t *testing.T) {
	for _, stage := range []int32{1, 3} {
		t.Run(map[int32]string{1: "preprepare", 3: "commit"}[stage], func(t *testing.T) {
			hook := &orderedCommitHook{acceptCommit: true}
			n, _ := orderTestNode(t, hook)
			n.pbftStage.Store(stage)
			future := orderRequest("future-sequence-11")
			digest := string(getDigest(future))
			n.requestPool[digest], n.height2Digest[11] = future, digest
			for _, member := range []uint64{0, 2, 3} {
				orderCommit(t, n, future, 11, member)
			}
			if hook.commitCalls != 0 || n.sequenceID != 10 || n.isReply[digest] {
				t.Fatal("cached future request executed or advanced commit progress")
			}
			if n.historyPending == nil || n.historyPending.start != 10 || n.historyPending.end != 11 {
				t.Fatalf("missing contiguous history request 10..11: %+v", n.historyPending)
			}
		})
	}
}

func TestRepeatedMemberCannotFormCommitQuorum(t *testing.T) {
	hook := &orderedCommitHook{acceptCommit: true}
	n, _ := orderTestNode(t, hook)
	n.pbftStage.Store(3)
	future := orderRequest("duplicate-votes")
	digest := string(getDigest(future))
	n.requestPool[digest], n.height2Digest[11] = future, digest
	for i := 0; i < 3; i++ {
		orderCommit(t, n, future, 11, 2)
	}
	if len(n.cntCommitConfirm[n.voteKey(11, getDigest(future))]) != 1 || n.historyPending != nil || hook.commitCalls != 0 || n.sequenceID != 10 {
		t.Fatal("three independently decoded copies of one member formed a quorum")
	}
}

func TestRejectedCommitDoesNotAdvanceProgress(t *testing.T) {
	hook := &orderedCommitHook{acceptCommit: false}
	n, _ := orderTestNode(t, hook)
	r := orderRequest("rejected-application")
	digest := string(getDigest(r))
	orderCall(t, n, n.handlePrePrepare, orderJSON(t, &message.PrePrepare{RequestMsg: r, Digest: getDigest(r), SeqID: 10}))
	for _, member := range []uint64{0, 2, 3} {
		orderCall(t, n, n.handlePrepare, orderJSON(t, &message.Prepare{
			Digest: getDigest(r), SeqID: 10,
			SenderNode: &shard.Node{ShardID: 0, NodeID: member, IPaddr: n.ip_nodeTable[0][member]},
		}))
	}
	if n.pbftStage.Load() != 3 {
		t.Fatal("matching prepares did not reach commit stage")
	}
	for _, member := range []uint64{0, 2, 3} {
		orderCommit(t, n, r, 10, member)
	}
	if hook.commitCalls != 1 || n.sequenceID != 10 || n.isReply[digest] || n.hasCommittedBlock.Load() {
		t.Fatal("rejected state application was recorded as a successful commit")
	}
}

func TestHistoryCompletionResetsStageAndRestoresCachedPrepare(t *testing.T) {
	for _, cachedNext := range []bool{false, true} {
		name := "idle"
		if cachedNext {
			name = "cached_next"
		}
		t.Run(name, func(t *testing.T) {
			hook := &orderedCommitHook{acceptCommit: true}
			n, frames := orderTestNode(t, hook)
			n.pbftStage.Store(3)
			r := orderRequest("committed-history-10")
			next := orderRequest("cached-next-11")
			nextDigest := string(getDigest(next))
			if cachedNext {
				n.requestPool[nextDigest], n.height2Digest[11] = next, nextDigest
			}
			attempt := n.beginHistoryAttempt(10, &shard.Node{ShardID: 0, NodeID: 0}, time.Now())
			response := &message.SendOldMessage{RequestID: attempt.RequestID, SeqStartHeight: 10, SeqEndHeight: 10, OldRequest: []*message.Request{r}, SenderNode: &shard.Node{ShardID: 0, NodeID: 0}}
			orderCall(t, n, n.handleSendOldSeq, orderJSON(t, response))
			if n.sequenceID != 11 || !n.isReply[string(getDigest(r))] || hook.commitCalls != 0 {
				t.Fatal("history progress was not restored exactly once")
			}
			if !cachedNext {
				if n.pbftStage.Load() != 1 {
					t.Fatal("completed history retained a stale commit stage")
				}
				return
			}
			if n.pbftStage.Load() != 2 {
				t.Fatal("cached next request did not enter prepare stage")
			}
			deadline := time.After(time.Second)
			for {
				select {
				case frame := <-frames:
					if frame.kind != message.CPrepare {
						continue
					}
					var prepare message.Prepare
					if err := json.Unmarshal(frame.content, &prepare); err != nil {
						t.Fatal(err)
					}
					if prepare.SeqID != 11 || string(prepare.Digest) != nextDigest || prepare.SenderNode == nil || prepare.SenderNode.NodeID != 1 {
						t.Fatal("restored self Prepare has the wrong request identity")
					}
					orderCall(t, n, n.handlePrepare, frame.content)
					if len(n.cntPrepareConfirm[n.voteKey(11, getDigest(next))]) != 1 {
						t.Fatal("restored prepare did not include the local member")
					}
					return
				case <-deadline:
					t.Fatal("cached-next recovery omitted the self Prepare")
				}
			}
		})
	}
}

func TestHistoryServesOnlyCommittedContiguousRequests(t *testing.T) {
	n, _ := orderTestNode(t, &orderedCommitHook{})
	n.sequenceID = 12
	first, cached := orderRequest("committed-10"), orderRequest("uncommitted-11")
	for sequence, request := range map[uint64]*message.Request{10: first, 11: cached} {
		digest := string(getDigest(request))
		n.requestPool[digest], n.height2Digest[sequence] = request, digest
	}
	n.isReply[string(getDigest(first))] = true
	if requests := n.committedHistory(10, 11); len(requests) != 1 || requests[0] != first {
		t.Fatal("history exposed a cached request that had not successfully committed")
	}
	n.isReply[string(getDigest(cached))] = true
	if requests := n.committedHistory(10, 11); len(requests) != 2 || requests[1] != cached {
		t.Fatal("successfully committed contiguous history was omitted")
	}
	n.sequenceID = 11
	if requests := n.committedHistory(10, 11); len(requests) != 1 {
		t.Fatal("history returned the current unexecuted sequence despite its stale reply marker")
	}
}

func TestHistoryTargetExtendsWithoutInvalidatingInFlightResponse(t *testing.T) {
	hook := &orderedCommitHook{acceptCommit: true}
	n, _ := orderTestNode(t, hook)
	peer := &shard.Node{ShardID: 0, NodeID: 0, IPaddr: n.ip_nodeTable[0][0]}
	now := time.Now()
	// Explicit clock advancement creates one expired attempt followed by the
	// active retry; successful progress must reset this consumed retry budget.
	n.beginHistoryAttempt(11, peer, now.Add(-historyAttemptTTL))
	attempt := n.beginHistoryAttempt(11, peer, now)
	if attempt == nil || n.historyRecovery.attempts != 2 {
		t.Fatal("failed to construct active second attempt")
	}
	future := orderRequest("new-committed-target-20")
	for _, member := range []uint64{0, 2, 3} {
		orderCommit(t, n, future, 20, member)
	}
	if n.historyRecovery.end != 20 || n.historyPending.id != attempt.RequestID || n.historyPending.end != 11 {
		t.Fatal("new target either was lost or changed the outstanding response identity")
	}
	response := &message.SendOldMessage{
		RequestID: attempt.RequestID, SeqStartHeight: 10, SeqEndHeight: 11,
		OldRequest: []*message.Request{orderRequest("history-10"), orderRequest("history-11")},
		SenderNode: peer,
	}
	orderCall(t, n, n.handleSendOldSeq, orderJSON(t, response))
	if n.sequenceID != 12 || n.historyPending == nil || n.historyPending.start != 12 || n.historyPending.end != 20 || n.historyPending.id == attempt.RequestID {
		t.Fatalf("successful old range did not immediately continue with 12..20: %+v", n.historyPending)
	}
	if n.historyRecovery.attempts != 1 || n.historyRecoveryFailed.Load() || hook.commitCalls != 0 {
		t.Fatal("real contiguous progress did not renew the suffix retry budget")
	}
}

func TestIdenticalDigestDifferentSequencesCannotCombineVotes(t *testing.T) {
	hook := &orderedCommitHook{acceptCommit: true}
	n, _ := orderTestNode(t, hook)
	r := orderRequest("same-digest-different-sequences")
	orderCommit(t, n, r, 11, 0)
	orderCommit(t, n, r, 11, 2)
	orderCommit(t, n, r, 12, 3)
	if len(n.cntCommitConfirm[n.voteKey(11, getDigest(r))]) != 2 || len(n.cntCommitConfirm[n.voteKey(12, getDigest(r))]) != 1 || n.historyPending != nil || n.sequenceID != 10 {
		t.Fatal("votes for different sequence identities were combined into one quorum")
	}
	orderCommit(t, n, r, 11, 3)
	if n.historyPending == nil || n.historyPending.end != 11 || hook.commitCalls != 0 {
		t.Fatal("a legitimate same-sequence quorum did not request its own committed range")
	}
}

func TestDuplicatePrePrepareDoesNotAdvanceOrResetStage(t *testing.T) {
	n, _ := orderTestNode(t, &orderedCommitHook{acceptCommit: true})
	r := orderRequest("duplicate-current-preprepare")
	raw := orderJSON(t, &message.PrePrepare{RequestMsg: r, Digest: getDigest(r), SeqID: 10})
	for i := 0; i < 3; i++ {
		orderCall(t, n, n.handlePrePrepare, raw)
	}
	if n.pbftStage.Load() != 2 || !n.preparedCurrent(getDigest(r)) {
		t.Fatal("duplicate PrePrepare advanced the round more than once")
	}
	for _, member := range []uint64{0, 2, 3} {
		orderCall(t, n, n.handlePrepare, orderJSON(t, &message.Prepare{
			Digest: getDigest(r), SeqID: 10,
			SenderNode: &shard.Node{ShardID: 0, NodeID: member, IPaddr: n.ip_nodeTable[0][member]},
		}))
	}
	if n.pbftStage.Load() != 3 {
		t.Fatal("prepare quorum failed to reach commit stage")
	}
	orderCall(t, n, n.handlePrePrepare, raw)
	if n.pbftStage.Load() != 3 || n.sequenceID != 10 {
		t.Fatal("late duplicate PrePrepare reset a round already in commit")
	}
}
