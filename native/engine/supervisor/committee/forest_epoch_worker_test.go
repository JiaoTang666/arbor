package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"
	"testing"
	"time"
)

const forestTestA = "0000000000000000000000000000000000000000"
const forestTestB = "0000000000000000000000000000000000000001"
const forestTestC = "0000000000000000000000000000000000000002"

func forestTestProvider(t *testing.T) *forestEpochProvider {
	t.Helper()
	before := params.ShardNum
	params.ShardNum = 2
	t.Cleanup(func() { params.ShardNum = before })
	return &forestEpochProvider{observed: 2, prefixHash: strings.Repeat("a", 64), inputHash: strings.Repeat("b", 64), initialLedgerHash: strings.Repeat("c", 64), initialRoutes: map[string]uint64{forestTestA: 1}, close: func() error { return nil }}
}

func forestPlanReply(p *forestEpochProvider, r forestEpochRequest, delta map[string]uint64) forestEpochReply {
	return forestEpochReply{Epoch: r.Epoch, ObservedPrefix: r.ObservedPrefix, CurrentMappingSHA256: r.CurrentMappingSHA256, InitialLedgerSHA256: r.InitialLedgerSHA256, PrefixSHA256: p.prefixHash, Delta: delta, Report: json.RawMessage(`{"score_before":2,"score_after":1}`), Evidence: json.RawMessage(`{"source":"worker"}`)}
}

func TestForestEpochSparseOverridesReturnToInitialAndHashOwners(t *testing.T) {
	p := forestTestProvider(t)
	p.SetEligible([]string{forestTestB})
	current := map[string]uint64{forestTestA: 0, forestTestB: 0}
	p.call = func(r forestEpochRequest) (forestEpochReply, error) {
		if len(r.CurrentMapping) != 2 || r.CurrentMapping[forestTestA] != 0 || r.CurrentMapping[forestTestB] != 0 {
			t.Fatal("lost sparse overrides", r.CurrentMapping)
		}
		if len(r.Eligible) != 1 || r.Eligible[0] != forestTestB {
			t.Fatal("initial ledger eligibility was retransmitted", r.Eligible)
		}
		return forestPlanReply(p, r, map[string]uint64{forestTestA: 1, forestTestB: 1}), nil
	}
	desired, err := p.Plan(1, nil, current)
	if err != nil {
		t.Fatal(err)
	}
	if current[forestTestA] != 0 || current[forestTestB] != 0 {
		t.Fatal("proposal mutated actual mapping before application")
	}
	p.call = func(r forestEpochRequest) (forestEpochReply, error) {
		if len(r.CurrentMapping) != 0 {
			t.Fatal("return-to-base was treated as an update to prior overrides", r.CurrentMapping)
		}
		return forestPlanReply(p, r, map[string]uint64{}), nil
	}
	if _, err = p.Plan(2, nil, desired); err != nil {
		t.Fatal(err)
	}
}

func TestForestEpochRejectsRouteOnlyAccountAndStaleBindings(t *testing.T) {
	for _, failure := range []string{"uncreated", "prefix", "mapping", "ledger", "epoch"} {
		t.Run(failure, func(t *testing.T) {
			p := forestTestProvider(t)
			p.call = func(r forestEpochRequest) (forestEpochReply, error) {
				out := forestPlanReply(p, r, map[string]uint64{})
				switch failure {
				case "uncreated":
					out.Delta[forestTestC] = 1
				case "prefix":
					out.ObservedPrefix++
				case "mapping":
					out.CurrentMappingSHA256 = "wrong"
				case "ledger":
					out.InitialLedgerSHA256 = "wrong"
				case "epoch":
					out.Epoch++
				}
				return out, nil
			}
			// Merely present in a routing override is not creation evidence.
			if _, err := p.Plan(1, nil, map[string]uint64{forestTestA: 1, forestTestC: 0}); err == nil {
				t.Fatal("invalid forest response accepted")
			}
			if p.lastEpoch != 0 || p.report != nil {
				t.Fatal("rejected proposal advanced provider state")
			}
		})
	}
}

func TestForestEpochObservationSourceClockAndPrefix(t *testing.T) {
	p := forestTestProvider(t)
	p.observed = 0
	row, err := forestObservationFromCSV([]string{"0", "1700000123"}, 0, forestTestA, forestTestB)
	if err != nil {
		t.Fatal(err)
	}
	called := 0
	p.call = func(r forestEpochRequest) (forestEpochReply, error) {
		called++
		if r.Op != "observe" || r.ObservedPrefix != p.observed {
			t.Fatal(r)
		}
		if len(r.Rows) > 0 && r.Rows[0].SourceTime != 1700000123 {
			t.Fatal("source time replaced with wall clock")
		}
		return forestEpochReply{ObservedPrefix: p.observed + uint64(len(r.Rows)), PrefixSHA256: p.prefixHash}, nil
	}
	if err = p.Observe([]forestEpochObservation{row}); err != nil {
		t.Fatal(err)
	}
	if err = p.Observe([]forestEpochObservation{row}); err == nil {
		t.Fatal("duplicate occurrence observed twice")
	}
	if called != 1 {
		t.Fatal("bad prefix reached worker")
	}
	if err = p.Observe(nil); err != nil || p.observed != 1 {
		t.Fatal("EOF flush changed observation count", err)
	}
	if _, err = forestObservationFromCSV([]string{"0", "NaN"}, 1, forestTestA, forestTestB); err == nil {
		t.Fatal("nonfinite source clock accepted")
	}
}

func TestForestEpochCreatedEligibilityRequiresVerifiedTerminal(t *testing.T) {
	e := epochTestController(t)
	e.provider = forestTestProvider(t)
	e.initialRoutes = map[string]uint64{forestTestA: 1}
	e.ccm.modifiedMap = map[string]uint64{forestTestA: 1, forestTestC: 0}
	tx := core.NewTransaction(forestTestA, forestTestB, big.NewInt(1), 0, time.Now())
	tx.IngressSequenceAssigned = true
	tx.AdmissionID = "clpa/" + e.inputHash + "/0"
	e.hashes[0] = hex.EncodeToString(tx.TxHash)
	e.handleBlock(&message.BlockInfoMsg{SenderShardID: 0, Epoch: 0, CommittedBlockHash: "terminal", InnerShardTxs: []*core.Transaction{tx}, CommitTime: time.Now()})
	if e.failure != nil || !e.created[forestTestB] || e.created[forestTestA] || e.created[forestTestC] || len(e.created) != 1 {
		t.Fatal("creation confused with route or initial membership", e.created, e.failure)
	}
	if len(e.ccm.clpaGraph.NetGraph.VertexSet) != 0 {
		t.Fatal("Forest terminal reporting still maintains an unused CLPA graph")
	}
}

func TestForestEpochReplyBoundToExactRequestBytes(t *testing.T) {
	r := forestEpochRequest{Schema: forestEpochWorkerSchema, ID: 7, Op: "observe", Rows: []forestEpochObservation{}}
	raw, _ := json.Marshal(r)
	hash := sha256.Sum256(raw)
	out := forestEpochReply{Schema: forestEpochWorkerSchema, ID: 7, Status: "OK", RequestSHA256: hex.EncodeToString(hash[:])}
	response, _ := json.Marshal(out)
	if _, err := validateForestEpochReply(response, r, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := validateForestEpochReply(response, r, append(raw, ' ')); err == nil {
		t.Fatal("reply accepted for different request bytes")
	}
	out.Status = "ERROR"
	out.Error = "fixture failure"
	response, _ = json.Marshal(out)
	if _, err := validateForestEpochReply(response, r, raw); err == nil {
		t.Fatal("worker failure fell back to another planner")
	}
}

func TestForestEpochWorkerTimeoutPoisonsPipe(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	w := &forestEpochWorker{stdin: writer, replies: make(chan forestWorkerReply), abort: make(chan struct{}), timeout: 20 * time.Millisecond}
	// No reader consumes stdin: timeout covers writes as well as responses.
	started := time.Now()
	if _, err := w.request(forestEpochRequest{Op: "hello"}); err == nil {
		t.Fatal("blocked worker did not time out")
	}
	if time.Since(started) > time.Second {
		t.Fatal("worker timeout was unbounded")
	}
	if w.failure == nil {
		t.Fatal("failed worker remained reusable")
	}
	if _, err := w.request(forestEpochRequest{Op: "observe"}); err == nil {
		t.Fatal("request after worker failure accepted")
	}
}

func TestForestEpochObservationRunsOutsideControllerMutex(t *testing.T) {
	e := epochTestController(t)
	p := forestTestProvider(t)
	p.observed = 0
	e.dispatched = 0
	e.provider = p
	p.call = func(r forestEpochRequest) (forestEpochReply, error) {
		locked := make(chan struct{})
		go func() { e.mu.Lock(); e.mu.Unlock(); close(locked) }()
		select {
		case <-locked:
		case <-time.After(time.Second):
			return forestEpochReply{}, fmt.Errorf("controller mutex held over worker I/O")
		}
		return forestEpochReply{ObservedPrefix: 1, PrefixSHA256: p.prefixHash}, nil
	}
	if err := e.observeForestRows([]forestEpochObservation{{Sequence: 0, SourceTime: 1, Sender: forestTestA, Recipient: forestTestB}}); err != nil {
		t.Fatal(err)
	}
}
