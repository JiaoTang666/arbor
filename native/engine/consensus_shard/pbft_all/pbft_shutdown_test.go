package pbft_all

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/message"
	"blockEmulator/shard"
)

type blockingCommitHook struct {
	started chan struct{}
	release chan struct{}
	called  chan struct{}
}

func (h *blockingCommitHook) HandleinPropose() (bool, *message.Request)               { return false, nil }
func (h *blockingCommitHook) HandleinPrePrepare(*message.PrePrepare) bool             { return false }
func (h *blockingCommitHook) HandleinPrepare(*message.Prepare) bool                   { return false }
func (h *blockingCommitHook) HandleReqestforOldSeq(*message.RequestOldMessage) bool   { return false }
func (h *blockingCommitHook) HandleforSequentialRequest(*message.SendOldMessage) bool { return false }

func (h *blockingCommitHook) HandleinCommit(*message.Commit) bool {
	select {
	case h.started <- struct{}{}:
	default:
	}
	<-h.release
	select {
	case h.called <- struct{}{}:
	default:
	}
	return true
}

func newShutdownTestNode(t *testing.T, hook ExtraOpInConsensus) *PbftConsensusNode {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	node := &PbftConsensusNode{
		RunningNode:             &shard.Node{NodeID: 0, ShardID: 0},
		tcpln:                   listener,
		pStop:                   make(chan uint64, 1),
		pl:                      &pbft_log.PbftLog{Plog: log.New(io.Discard, "", 0)},
		ihm:                     hook,
		sequenceID:              1,
		requestPool:             make(map[string]*message.Request),
		cntPrepareConfirm:       make(map[string]map[*shard.Node]bool),
		cntCommitConfirm:        make(map[string]map[*shard.Node]bool),
		isCommitBordcast:        make(map[string]bool),
		isReply:                 make(map[string]bool),
		height2Digest:           make(map[uint64]string),
		ip_nodeTable:            map[uint64]map[uint64]string{0: {0: listener.Addr().String(), 1: listener.Addr().String()}},
		currentPreparedSequence: 1,
		currentPreparedDigest:   "shutdown-test-digest",
	}
	node.conditionalVarpbftLock = *sync.NewCond(&node.pbftLock)
	node.pbftStage.Store(3)
	node.view.Store(0)
	return node
}

func testCommitPayload(t *testing.T) []byte {
	t.Helper()
	payload, err := json.Marshal(&message.Commit{
		Digest:     []byte("shutdown-test-digest"),
		SeqID:      1,
		SenderNode: &shard.Node{NodeID: 1, ShardID: 0},
	})
	if err != nil {
		t.Fatalf("marshal commit: %v", err)
	}
	return payload
}

func TestWaitToStopWaitsForInFlightCommit(t *testing.T) {
	hook := &blockingCommitHook{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		called:  make(chan struct{}, 1),
	}
	node := newShutdownTestNode(t, hook)
	payload := testCommitPayload(t)
	node.requestPool[string([]byte("shutdown-test-digest"))] = &message.Request{}

	commitDone := make(chan struct{})
	go func() {
		node.handleCommit(payload)
		close(commitDone)
	}()
	select {
	case <-hook.started:
	case <-time.After(time.Second):
		t.Fatal("commit hook did not start")
	}

	stopDone := make(chan struct{})
	go func() {
		node.WaitToStop()
		close(stopDone)
	}()
	deadline := time.After(time.Second)
	for !node.stopSignal.Load() {
		select {
		case <-deadline:
			t.Fatal("stop signal was not recorded")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if node.chainClosed.Load() {
		t.Fatal("chain closed while commit still held the PBFT lock")
	}
	select {
	case <-stopDone:
		t.Fatal("stop returned before the in-flight commit finished")
	default:
	}

	close(hook.release)
	select {
	case <-commitDone:
	case <-time.After(time.Second):
		t.Fatal("commit did not finish")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("stop did not finish after commit released the PBFT lock")
	}
	if !node.chainClosed.Load() {
		t.Fatal("chain close did not run after the commit drained")
	}
}

func TestWaitToStopWakesCommitWaiterWithoutPersisting(t *testing.T) {
	hook := &blockingCommitHook{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		called:  make(chan struct{}, 1),
	}
	node := newShutdownTestNode(t, hook)
	wakeReady := make(chan struct{}, 1)
	woken := make(chan struct{}, 1)
	// This uses the same condition and lock as handleCommit. Sending wakeReady
	// while holding the mutex and then acquiring it below proves Wait has
	// released the mutex before shutdown issues Broadcast.
	commitDone := make(chan struct{})
	go func() {
		node.pbftLock.Lock()
		wakeReady <- struct{}{}
		node.conditionalVarpbftLock.Wait()
		woken <- struct{}{}
		node.pbftLock.Unlock()
		close(commitDone)
	}()
	select {
	case <-wakeReady:
	case <-time.After(time.Second):
		t.Fatal("commit waiter did not acquire the PBFT lock")
	}
	node.pbftLock.Lock()
	node.pbftLock.Unlock()
	node.WaitToStop()
	select {
	case <-commitDone:
	case <-time.After(time.Second):
		t.Fatal("waiting commit did not exit after stop")
	}
	select {
	case <-woken:
	default:
		t.Fatal("shutdown did not broadcast to the PBFT condition waiter")
	}
}

func TestStoppedCommitDoesNotEnterPersistence(t *testing.T) {
	hook := &blockingCommitHook{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		called:  make(chan struct{}, 1),
	}
	node := newShutdownTestNode(t, hook)
	node.stopSignal.Store(true)
	node.handleCommit(testCommitPayload(t))
	select {
	case <-hook.started:
		t.Fatal("commit entered persistence after stop")
	default:
	}
}

func TestWaitToStopIsIdempotent(t *testing.T) {
	node := newShutdownTestNode(t, &blockingCommitHook{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		called:  make(chan struct{}, 1),
	})
	node.WaitToStop()
	node.WaitToStop()
	if !node.chainClosed.Load() {
		t.Fatal("first stop did not close the chain")
	}
	select {
	case <-node.pStop:
	default:
		t.Fatal("first stop did not notify the proposer")
	}
	select {
	case <-node.pStop:
		t.Fatal("repeated stop sent a duplicate proposer notification")
	default:
	}
}

func TestWaitForShutdownWaitsForSlowClose(t *testing.T) {
	node := newShutdownTestNode(t, &blockingCommitHook{})
	exportStarted := make(chan struct{})
	allowExport := make(chan struct{})
	// An outstanding close section represents an export slower than a proposer
	// tick. closePbft's sync.Once must wait for it before signaling completion.
	go node.chainCloseOnce.Do(func() {
		close(exportStarted)
		<-allowExport
		node.chainClosed.Store(true)
	})
	<-exportStarted
	stopDone := make(chan struct{})
	go func() { node.WaitToStop(); close(stopDone) }()
	deadline := time.After(time.Second)
	for !node.stopSignal.Load() {
		select {
		case <-deadline:
			t.Fatal("stop did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	waited := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		go func() { node.WaitForShutdown(); waited <- struct{}{} }()
	}
	select {
	case <-waited:
		t.Fatal("entrypoint returned while the audit/storage close was unfinished")
	case <-time.After(30 * time.Millisecond):
	}
	close(allowExport)
	for i := 0; i < 2; i++ {
		select {
		case <-waited:
		case <-time.After(time.Second):
			t.Fatal("shutdown waiter did not observe completed close")
		}
	}
	<-stopDone
	if !node.chainClosed.Load() {
		t.Fatal("shutdown completed before storage close")
	}
	// Waiting after completion and repeated stop requests must remain harmless.
	node.WaitToStop()
	node.WaitForShutdown()
}

func TestWaitForShutdownDoesNotImpersonateUnrequestedStop(t *testing.T) {
	node := newShutdownTestNode(t, &blockingCommitHook{})
	node.WaitForShutdown()
	if node.stopSignal.Load() || node.chainClosed.Load() {
		t.Fatal("waiting without a stop must not create one or close storage")
	}
	_ = node.tcpln.Close()
}
