package committee

import (
	"blockEmulator/message"
	"blockEmulator/params"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAsyncInitialOnlyPlanTransportsEmptyEligibleArray(t *testing.T) {
	oldShards := params.ShardNum
	params.ShardNum = 2
	defer func() { params.ShardNum = oldShards }()
	e := &clpaEpochController{counts: map[uint64]int{0: 1, 1: 1}, dispatched: 10}
	a := &forestAsyncPipeline{e: e, majorRounds: 3, nextStart: 1,
		intents: make(chan forestAsyncJob, 1), results: make(chan *forestAsyncResult, 1), notify: make(chan struct{}, 1)}
	a.observed.Store(10)
	a.tick()
	job := <-a.intents
	raw, err := json.Marshal(forestEpochRequest{Eligible: job.eligible})
	if err != nil || !strings.Contains(string(raw), `"eligible":[]`) {
		t.Fatalf("initial eligibility must be implicit with an empty array, not null: %s %v", raw, err)
	}
}

func TestAsyncInitialMappingHasCanonicalEmptyDigest(t *testing.T) {
	if clpaEpochMappingDigest(nil) != clpaEpochMappingDigest(map[string]uint64{}) {
		t.Fatal("initial sparse mapping nil and empty must bind the same layout")
	}
	e := &clpaEpochController{ccm: &CLPACommitteeModule{}, dispatched: 100}
	a := &forestAsyncPipeline{e: e, ready: &forestAsyncResult{
		job:   forestAsyncJob{overrides: map[string]uint64{}},
		reply: forestEpochReply{ObservedPrefix: 50, Delta: map[string]uint64{}}}}
	if _, err := a.proposalLocked(); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncAppliedWireRolesAndFollowerSubset(t *testing.T) {
	oldShards, oldNodes := params.ShardNum, params.NodesInShard
	params.ShardNum, params.NodesInShard = 2, 4
	defer func() { params.ShardNum, params.NodesInShard = oldShards, oldNodes }()
	hash := strings.Repeat("a", 64)
	e := &clpaEpochController{audit: io.Discard, applied: map[[2]uint64]message.CLPAEpochApplied{},
		plan: &message.CLPAEpochPlan{Epoch: 1, PlanHash: hash, InputCuts: map[uint64]uint64{0: 7}}}
	e.async = &forestAsyncPipeline{}
	leader := message.CLPAEpochApplied{Epoch: 1, PlanHash: hash, StateRoot: hash, AsyncPipeline: true,
		ReplicaRole: message.CLPAEpochFixedLeaderRole, InputCut: 7, LocalRounds: 3,
		Pending: 5, PendingOrdinary: 5, RetainedPending: 5, RetainedAvailable: 5,
		RetainedManifestHash: hash, IncomingManifestHash: hash, SourceManifestHash: hash,
		PendingLedgerHash: hash, InstallPayloadHash: hash}
	// Exercise the serialized native receipt, not an invented supervisor role.
	raw, _ := json.Marshal(leader)
	var wire message.CLPAEpochApplied
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	if err := e.acceptAppliedLocked(wire); err != nil {
		t.Fatal(err)
	}
	follower := leader
	follower.Node, follower.ReplicaRole = 1, message.CLPAEpochFollowerRole
	follower.Pending, follower.PendingOrdinary, follower.RetainedAvailable = 0, 0, 0
	if err := e.acceptAppliedLocked(follower); err != nil {
		t.Fatal(err)
	}
	follower.Node, follower.ReplicaRole = 2, "leader"
	if err := e.acceptAppliedLocked(follower); err == nil {
		t.Fatal("accepted mismatching replica wire role")
	}
}

func TestAsyncLateOldBlockIsAuditedWithoutAdvancingCadence(t *testing.T) {
	oldShards := params.ShardNum
	params.ShardNum = 2
	defer func() { params.ShardNum = oldShards }()
	var audit bytes.Buffer
	e := &clpaEpochController{audit: &audit, epoch: 1, counts: map[uint64]int{0: 2},
		blocks: map[string]bool{}, terminal: map[uint64]bool{}, provider: &forestEpochProvider{}}
	block := &message.BlockInfoMsg{Epoch: 0, SenderShardID: 0, CommittedBlockHash: strings.Repeat("a", 64)}
	e.handleBlock(block)
	e.handleBlock(block)
	if e.failure != nil || e.counts[0] != 2 || e.reportedRounds[0][0] != 1 {
		t.Fatalf("late report changed cadence or dedup: %v", e.failure)
	}
	if !strings.Contains(audit.String(), `"late_report":true`) || strings.Count(audit.String(), `"ordinary_block"`) != 1 {
		t.Fatalf("late cut evidence missing or duplicated: %s", audit.String())
	}
}

func TestAsyncObservationDoesNotWaitForProvider(t *testing.T) {
	oldShards := params.ShardNum
	params.ShardNum = 2
	defer func() { params.ShardNum = oldShards }()
	e := &clpaEpochController{audit: io.Discard, rounds: 3, ccm: &CLPACommitteeModule{}, counts: map[uint64]int{}, created: map[string]bool{}}
	entered, release := make(chan struct{}), make(chan struct{})
	p := &forestEpochProvider{call: func(r forestEpochRequest) (forestEpochReply, error) {
		if r.Op != "observe_group" || len(r.Batches) != 1 || len(r.Batches[0]) != 1 {
			return forestEpochReply{}, fmt.Errorf("unexpected observation group")
		}
		close(entered)
		<-release
		return forestEpochReply{ObservedPrefix: 1, PrefixSHA256: strings.Repeat("a", 64)}, nil
	}}
	a, err := newForestAsyncPipeline(e, p, filepath.Join(t.TempDir(), "observations.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	row := forestEpochObservation{Sequence: 0, SourceTime: 1, Sender: strings.Repeat("1", 40), Recipient: strings.Repeat("2", 40)}
	if err = a.append([]forestEpochObservation{row}); err != nil {
		t.Fatal(err)
	}
	if a.written.Load() != 1 || a.durable.Load() != 0 {
		t.Fatal("dispatch append acquired a durability barrier")
	}
	e.mu.Lock()
	e.dispatched = 1
	e.mu.Unlock()
	a.wake()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("observer did not start")
	}
	// The provider is blocked, but no controller lock or input writer is held.
	e.mu.Lock()
	e.counts[0] = 9
	e.mu.Unlock()
	if a.observed.Load() != 0 {
		close(release)
		t.Fatal("unacknowledged observation was published")
	}
	if a.durable.Load() != 1 {
		close(release)
		t.Fatal("observer used unpersisted evidence")
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for a.observed.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if a.observed.Load() != 1 {
		t.Fatal("observation did not advance")
	}
}

func TestAsyncReadyPlanUsesFrozenNotLatestPrefix(t *testing.T) {
	oldShards := params.ShardNum
	params.ShardNum = 2
	defer func() { params.ShardNum = oldShards }()
	account := strings.Repeat("1", 40)
	ledger := strings.Repeat("b", 64)
	prefix := strings.Repeat("c", 64)
	p := &forestEpochProvider{observed: 300, initialLedgerHash: ledger, initialRoutes: map[string]uint64{account: 0}}
	a := &forestAsyncPipeline{e: &clpaEpochController{ccm: &CLPACommitteeModule{}}, p: p}
	r := &forestAsyncResult{job: forestAsyncJob{generation: 1, overrides: map[string]uint64{}}, reply: forestEpochReply{ObservedPrefix: 100, PrefixSHA256: prefix}, started: time.Now()}
	reply := forestEpochReply{Epoch: 1, ObservedPrefix: 100, PrefixSHA256: prefix, InitialLedgerSHA256: ledger,
		CurrentMappingSHA256: clpaEpochMappingDigest(r.job.overrides), Delta: map[string]uint64{account: 1}, Report: []byte(`{"ok":true}`), Evidence: []byte(`{"ok":true}`)}
	if err := a.validateResult(r, reply); err != nil {
		t.Fatal(err)
	}
	reply.ObservedPrefix = 300
	if err := a.validateResult(r, reply); err == nil {
		t.Fatal("accepted latest-prefix substitution")
	}
}

func TestAsyncJournalGroupsDurabilityWithoutSkippingEvidence(t *testing.T) {
	var audit bytes.Buffer
	e := &clpaEpochController{audit: &audit, rounds: 3, ccm: &CLPACommitteeModule{}}
	p := &forestEpochProvider{call: func(r forestEpochRequest) (forestEpochReply, error) {
		last := r.Batches[len(r.Batches)-1]
		return forestEpochReply{ObservedPrefix: last[len(last)-1].Sequence + 1, PrefixSHA256: strings.Repeat("a", 64)}, nil
	}}
	a, err := newForestAsyncPipeline(e, p, filepath.Join(t.TempDir(), "journal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	for seq := uint64(0); seq < 2; seq++ {
		if err := a.append([]forestEpochObservation{{Sequence: seq, SourceTime: 1,
			Sender: strings.Repeat("1", 40), Recipient: strings.Repeat("2", 40)}}); err != nil {
			t.Fatal(err)
		}
	}
	e.mu.Lock()
	e.dispatched = 2
	e.mu.Unlock()
	a.wake()
	deadline := time.Now().Add(time.Second)
	for a.observed.Load() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if a.observed.Load() != 2 || a.durable.Load() != 2 || strings.Count(audit.String(), `"event":"observation_journal_sync"`) != 1 {
		t.Fatalf("grouped durability lost evidence or added per-batch barriers: %s", audit.String())
	}
}

func TestAsyncOrdinaryCadenceIsNotAnInstallBarrier(t *testing.T) {
	oldShards := params.ShardNum
	params.ShardNum = 2
	defer func() { params.ShardNum = oldShards }()
	e := &clpaEpochController{counts: map[uint64]int{0: 500, 1: 500}, rounds: 20}
	e.async = &forestAsyncPipeline{e: e, majorRounds: 400}
	if e.readyLocked() {
		t.Fatal("ordinary rounds without ready plan triggered install")
	}
	e.async.ready = &forestAsyncResult{}
	if !e.readyLocked() {
		t.Fatal("ready major update did not become installable")
	}
	e.counts[1] = 399
	if e.readyLocked() {
		t.Fatal("installed before declared major cadence")
	}
	e.async.inputDone = true
	if e.readyLocked() {
		t.Fatal("input EOF bypassed the declared major cadence")
	}
	e.counts[1] = 400
	if !e.readyLocked() {
		t.Fatal("input EOF cancelled the normal periodic update")
	}
	e.nativeCompleteAt = time.Now()
	if e.readyLocked() {
		t.Fatal("new installation started after all native transactions completed")
	}
}
