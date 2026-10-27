package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"reflect"
	"testing"
)

func asyncEpochTestModule(t *testing.T) (*CLPAPbftInsideExtraHandleMod, *CLPARelayOutsideModule) {
	t.Helper()
	in, out := epochTestModule(t)
	in.epoch.asyncPipeline = true
	return in, out
}

func epochTestManifest(tx *core.Transaction, destination uint64) message.CLPAEpochPendingLeg {
	return message.CLPAEpochPendingLeg{AdmissionID: tx.AdmissionID, Relayed: tx.Relayed, PayloadHash: epochFingerprint(tx), OwnerAccount: epochOwnerAccount(tx), Destination: destination}
}

func TestAsyncEpochCeilingIsDisabledUntilSealedPlan(t *testing.T) {
	in, out := asyncEpochTestModule(t)
	e := in.epoch
	e.started = true
	e.rounds = 17
	if !e.ordinaryAllowedLocked() {
		t.Fatal("planning delay imposed the synchronous round ceiling")
	}
	plan := message.CLPAEpochPlan{Epoch: 1, BaseEpoch: 0, Mapping: map[string]uint64{}, InputCuts: map[uint64]uint64{0: 0, 1: 0}}
	plan.PlanHash = plan.Digest()
	if err := out.handleEpochMessage(message.CCLPAEpochPlan, epochJSON(plan)); err != nil {
		t.Fatal(err)
	}
	if e.ordinaryAllowedLocked() {
		t.Fatal("sealed plan allowed another ordinary proposal")
	}
	if err := out.handleEpochMessage(message.CCLPAEpochDrain, epochJSON(message.CLPAEpochDrain{Epoch: 0})); err != nil {
		t.Fatal(err)
	}
	if e.ordinaryAllowedLocked() {
		t.Fatal("EOF drain bypassed an accepted plan")
	}
	e.plan = nil
	if !e.ordinaryAllowedLocked() {
		t.Fatal("async drain restored a round ceiling")
	}
	e.resumed = false
	if e.ordinaryAllowedLocked() {
		t.Fatal("async mode bypassed the global install barrier")
	}
}

func TestAsyncEpochActualVariableCutsAndZeroRelayCut(t *testing.T) {
	in, out := asyncEpochTestModule(t)
	e := in.epoch
	plan := message.CLPAEpochPlan{Epoch: 1, BaseEpoch: 0, Mapping: map[string]uint64{}, InputCuts: map[uint64]uint64{0: 0, 1: 0}}
	plan.PlanHash = plan.Digest()
	if err := out.handleEpochMessage(message.CCLPAEpochReady, epochJSON(message.CLPAEpochReady{Epoch: 1, PlanHash: plan.PlanHash, FromShard: 1, RelayCut: 0})); err != nil {
		t.Fatal(err)
	}
	if err := out.handleEpochMessage(message.CCLPAEpochPlan, epochJSON(plan)); err != nil {
		t.Fatal(err)
	}
	e.rounds = 7
	e.sentReady, e.sentTransfers = true, true
	e.ready[0] = message.CLPAEpochReady{Epoch: 1, PlanHash: plan.PlanHash, FromShard: 0, RelayCut: 7}
	e.transfers[0] = message.CLPAEpochTransfer{Epoch: 1, PlanHash: plan.PlanHash, FromShard: 0, ToShard: 0, SourceRounds: 7}
	e.transfers[1] = message.CLPAEpochTransfer{Epoch: 1, PlanHash: plan.PlanHash, FromShard: 1, ToShard: 0, SourceRounds: 0}
	ok, request := in.epochPropose()
	if !ok || request == nil || request.RequestType != message.CLPAEpochInstall {
		t.Fatal("different actual shard rounds or empty relay history prevented install")
	}
	local := e.transfers[0]
	local.SourceRounds = e.limit
	if err := in.validateEpochInstallLocked(clpaEpochInstallPayload{Plan: plan, Transfers: []message.CLPAEpochTransfer{local, e.transfers[1]}}); err == nil {
		t.Fatal("fixed configured limit substituted for the actual local commit cut")
	}
	bad := e.transfers[1]
	bad.SourceRounds = 1
	if err := out.handleEpochMessage(message.CCLPAEpochTransfer, epochJSON(bad)); err == nil {
		t.Fatal("transfer escaped its ready source cut")
	}
}

func TestAsyncEpochManifestRequiresCompleteLeaderAndChecksFollowerSubset(t *testing.T) {
	in, _ := asyncEpochTestModule(t)
	e := in.epoch
	e.rounds = 8
	keep := epochTestTx("keep", epochA, epochB, false)
	moveFirst := epochTestTx("move-first", epochC, epochA, false)
	moveSecond := epochTestTx("move-second", epochA, epochC, true)
	arrived := epochTestTx("arrived", epochB, epochA, false)
	plan := message.CLPAEpochPlan{Epoch: 1, BaseEpoch: 0, Mapping: map[string]uint64{epochC: 1, epochB: 0}, InputCuts: map[uint64]uint64{0: 0, 1: 0}}
	plan.PlanHash = plan.Digest()
	local := message.CLPAEpochTransfer{Epoch: 1, PlanHash: plan.PlanHash, FromShard: 0, ToShard: 0, SourceRounds: 8, PendingManifest: []message.CLPAEpochPendingLeg{epochTestManifest(keep, 0), epochTestManifest(moveFirst, 1), epochTestManifest(moveSecond, 1)}}
	remote := message.CLPAEpochTransfer{Epoch: 1, PlanHash: plan.PlanHash, FromShard: 1, ToShard: 0, SourceRounds: 11, Txs: []*core.Transaction{arrived}, PendingManifest: []message.CLPAEpochPendingLeg{epochTestManifest(arrived, 0)}}
	// Manifest checks do not need a trie; imported state is covered separately
	// by the existing strict account installation tests.
	payload := clpaEpochInstallPayload{Plan: plan, Transfers: []message.CLPAEpochTransfer{local, remote}}
	for _, tx := range []*core.Transaction{keep, moveFirst, moveSecond} {
		e.admissions[epochLegKey(tx.AdmissionID, tx.Relayed)] = epochFingerprint(tx)
	}
	in.pbftNode.CurChain.Txpool.AddTxs2Pool([]*core.Transaction{keep, moveFirst, moveSecond})
	source, retained, err := in.epochDeltaPendingLocked(payload)
	if err != nil || len(source) != 3 || len(retained) != 1 || retained[0] != keep {
		t.Fatalf("leader retained source incorrectly: %v", err)
	}
	if err := in.pbftNode.CurChain.Txpool.InstallCLPAEpochPendingDelta(source, retained, remote.Txs); err != nil {
		t.Fatal(err)
	}
	snapshot, err := in.pbftNode.CurChain.Txpool.SnapshotCLPAEpochPendingReferences()
	if err != nil || len(snapshot) != 2 || snapshot[0] != keep || snapshot[1].AdmissionID != arrived.AdmissionID {
		t.Fatalf("moved first/second legs or retained FIFO were wrong: %v", err)
	}
	// Hash metadata is not sufficient to recover missing leader payload.
	in.pbftNode.CurChain.Txpool.ReplaceCLPAEpochPending(nil)
	if _, _, err := in.epochDeltaPendingLocked(payload); err == nil {
		t.Fatal("missing fixed leader payload was silently accepted")
	}
	in.pbftNode.NodeID = 1
	if _, retained, err := in.epochDeltaPendingLocked(payload); err != nil || len(retained) != 0 {
		t.Fatalf("follower was assumed to have leader ordinary ingress: %v", err)
	}
	in.pbftNode.CurChain.Txpool.AddTx2Pool(keep)
	if _, retained, err := in.epochDeltaPendingLocked(payload); err != nil || len(retained) != 1 {
		t.Fatalf("follower's available retained subset was discarded: %v", err)
	}
	payload.Transfers[0].PendingManifest[0].PayloadHash = epochFingerprint("different")
	if _, _, err := in.epochDeltaPendingLocked(payload); err == nil {
		t.Fatal("changed retained admission hash passed the proof gate")
	}
}

func TestAsyncEpochMovedManifestBindsPayloadAndRejectsDuplicateLegs(t *testing.T) {
	in, _ := asyncEpochTestModule(t)
	tx := epochTestTx("moved", epochB, epochA, false)
	b := message.CLPAEpochTransfer{FromShard: 1, ToShard: 0, Txs: []*core.Transaction{tx}, PendingManifest: []message.CLPAEpochPendingLeg{epochTestManifest(tx, 0)}}
	owner := func(string) uint64 { return 0 }
	if err := in.validateEpochManifestLocked(b, owner, map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	tx.Value.SetInt64(2)
	if err := in.validateEpochManifestLocked(b, owner, map[string]bool{}); err == nil {
		t.Fatal("payload changed without changing its bound manifest")
	}
	tx.Value.SetInt64(1)
	b.Txs = append(b.Txs, tx)
	b.PendingManifest = append(b.PendingManifest, b.PendingManifest[0])
	if err := in.validateEpochManifestLocked(b, owner, map[string]bool{}); err == nil {
		t.Fatal("duplicate admission leg passed the async manifest gate")
	}
	if got := epochManifestHash(nil); got != epochFingerprint([]message.CLPAEpochPendingLeg{}) {
		t.Fatal("empty manifests did not hash the canonical empty array")
	}
	_, kept, arrived := epochPayloadManifests(clpaEpochInstallPayload{Transfers: []message.CLPAEpochTransfer{{FromShard: 0, PendingManifest: []message.CLPAEpochPendingLeg{epochTestManifest(tx, 0)}}}}, 0)
	if !reflect.DeepEqual(kept, []message.CLPAEpochPendingLeg{epochTestManifest(tx, 0)}) || len(arrived) != 0 {
		t.Fatal("manifest ledger extraction changed order or membership")
	}
}
