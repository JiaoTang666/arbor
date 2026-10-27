package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"fmt"
	"math/big"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestCLPAEpochPrefixReordersAndRejectsConflictingReplay(t *testing.T) {
	q := epochPrefix{}
	var applied []string
	apply := func(raw []byte) error { applied = append(applied, string(raw)); return nil }
	if err := q.accept(2, []byte("second"), apply); err != nil {
		t.Fatal(err)
	}
	if q.prefix != 0 || len(applied) != 0 {
		t.Fatal("gap was treated as a complete prefix")
	}
	if err := q.accept(1, []byte("first"), apply); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(applied, []string{"first", "second"}) || q.prefix != 2 {
		t.Fatalf("wrong ordered prefix: %v / %d", applied, q.prefix)
	}
	if err := q.accept(2, []byte("second"), apply); err != nil || len(applied) != 2 {
		t.Fatalf("duplicate reapplied: %v", err)
	}
	if err := q.accept(2, []byte("changed"), apply); err == nil {
		t.Fatal("conflicting duplicate accepted")
	}
}

func epochTestModule(t *testing.T) (*CLPAPbftInsideExtraHandleMod, *CLPARelayOutsideModule) {
	t.Helper()
	t.Setenv("BLOCK_EMULATOR_CLPA_EPOCH_V1", "1")
	t.Setenv("BLOCK_EMULATOR_CLPA_EPOCH_ROUNDS", "3")
	t.Setenv("BLOCK_EMULATOR_CLPA_ASYNC_PIPELINE", "")
	e := newCLPAEpochRuntime()
	config := &params.ChainConfig{ShardID: 0, ShardNums: 2, Nodes_perShard: 4}
	bc := &chain.BlockChain{ChainConfig: config, Txpool: core.NewTxPool(), PartitionMap: map[string]uint64{epochA: 0, epochB: 1, epochC: 0}}
	n := &PbftConsensusNode{ShardID: 0, CurChain: bc, pbftChainConfig: config}
	n.hasSeenWorkload.Store(true)
	d := dataSupport.NewCLPADataSupport()
	return &CLPAPbftInsideExtraHandleMod{pbftNode: n, cdm: d, epoch: e}, &CLPARelayOutsideModule{pbftNode: n, cdm: d, epoch: e}
}

const epochA = "0000000000000000000000000000000000000000"
const epochB = "0000000000000000000000000000000000000001"
const epochC = "0000000000000000000000000000000000000002"

func epochTestTx(id string, sender, recipient string, relay bool) *core.Transaction {
	tx := core.NewTransaction(sender, recipient, big.NewInt(1), 1, time.Unix(1, 0))
	tx.AdmissionID = id
	tx.Relayed = relay
	return tx
}

func TestCLPAEpochConcurrentInputAndPlanCloseExactPrefix(t *testing.T) {
	in, out := epochTestModule(t)
	plan := message.CLPAEpochPlan{Epoch: 1, BaseEpoch: 0, Mapping: map[string]uint64{}, InputCuts: map[uint64]uint64{0: 32, 1: 0}}
	plan.PlanHash = plan.Digest()
	if err := out.handleEpochMessage(message.CCLPAEpochPlan, epochJSON(plan)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errors := make(chan error, 32)
	for batch := uint64(32); batch > 0; batch-- {
		wg.Add(1)
		go func(batch uint64) {
			defer wg.Done()
			errors <- out.handleEpochMessage(message.CCLPAEpochInput, epochJSON(message.CLPAEpochInput{Epoch: 0, Batch: batch, ToShard: 0, Txs: []*core.Transaction{epochTestTx(fmt.Sprint(batch), epochA, epochB, false)}}))
		}(batch)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if in.epoch.input.prefix != 32 {
		t.Fatalf("prefix=%d", in.epoch.input.prefix)
	}
	queue, err := in.pbftNode.CurChain.Txpool.SnapshotCLPAEpochPending()
	if err != nil {
		t.Fatal(err)
	}
	for i, tx := range queue {
		if tx.AdmissionID != fmt.Sprint(i+1) {
			t.Fatalf("input order changed at %d", i)
		}
	}
	err = out.handleEpochMessage(message.CCLPAEpochInput, epochJSON(message.CLPAEpochInput{Epoch: 0, Batch: 33, ToShard: 0}))
	if err == nil {
		t.Fatal("input past closed cut accepted")
	}
}

func TestCLPAEpochPauseDoesNotBlockAndDrainReopensOnlyCeiling(t *testing.T) {
	in, out := epochTestModule(t)
	in.epoch.started = true
	in.epoch.rounds = 3
	if ok, r := in.epochPropose(); ok || r != nil {
		t.Fatal("ordinary proposal exceeded epoch limit")
	}
	if err := out.handleEpochMessage(message.CCLPAEpochDrain, epochJSON(message.CLPAEpochDrain{Epoch: 0})); err != nil {
		t.Fatal(err)
	}
	if !in.epoch.draining {
		t.Fatal("drain failed to remove round ceiling")
	}
	in.epoch.resumed = false
	if ok, r := in.epochPropose(); ok || r != nil {
		t.Fatal("drain bypassed global resume gate")
	}
}

func TestCLPAEpochInstallRoutesFirstAndSecondLegAndRequiresState(t *testing.T) {
	in, _ := epochTestModule(t)
	in.epoch.rounds = 3
	plan := message.CLPAEpochPlan{Epoch: 1, BaseEpoch: 0, Mapping: map[string]uint64{epochB: 0}, InputCuts: map[uint64]uint64{0: 0, 1: 0}}
	plan.PlanHash = plan.Digest()
	local := message.CLPAEpochTransfer{Epoch: 1, PlanHash: plan.PlanHash, FromShard: 0, ToShard: 0, Txs: []*core.Transaction{epochTestTx("local", epochA, epochB, false)}}
	remote := message.CLPAEpochTransfer{Epoch: 1, PlanHash: plan.PlanHash, FromShard: 1, ToShard: 0, Addrs: []string{epochB}, AccountState: []*core.AccountState{{Balance: big.NewInt(23), Nonce: 2}}, Txs: []*core.Transaction{epochTestTx("first", epochB, epochA, false), epochTestTx("second", epochA, epochB, true)}}
	payload := clpaEpochInstallPayload{Plan: plan, Transfers: []message.CLPAEpochTransfer{local, remote}}
	if err := in.validateEpochInstallLocked(payload); err != nil {
		t.Fatal(err)
	}
	payload.Transfers[1].Txs[1].Relayed = false
	if err := in.validateEpochInstallLocked(payload); err == nil {
		t.Fatal("relay second leg silently treated as first leg")
	}
	payload.Transfers[1].Txs[1].Relayed = true
	payload.Transfers[1].Addrs = nil
	payload.Transfers[1].AccountState = nil
	if err := in.validateEpochInstallLocked(payload); err == nil {
		t.Fatal("missing moved account state accepted")
	}
}

func TestCLPAEpochEarlyReadyIsBoundToNextPlan(t *testing.T) {
	in, out := epochTestModule(t)
	plan := message.CLPAEpochPlan{Epoch: 1, BaseEpoch: 0, Mapping: map[string]uint64{}, InputCuts: map[uint64]uint64{0: 0, 1: 0}}
	plan.PlanHash = plan.Digest()
	r := message.CLPAEpochReady{Epoch: 1, PlanHash: plan.PlanHash, FromShard: 1, RelayCut: 3}
	if err := out.handleEpochMessage(message.CCLPAEpochReady, epochJSON(r)); err != nil {
		t.Fatal(err)
	}
	if err := out.handleEpochMessage(message.CCLPAEpochPlan, epochJSON(plan)); err != nil {
		t.Fatal(err)
	}
	if len(in.epoch.ready) != 1 {
		t.Fatal("early ready lost")
	}
	r.PlanHash = "different"
	if err := out.handleEpochMessage(message.CCLPAEpochReady, epochJSON(r)); err == nil {
		t.Fatal("conflicting ready accepted")
	}
}

func TestCLPAEpochMissingOrGappedRelayDefersSnapshot(t *testing.T) {
	in, _ := epochTestModule(t)
	e := in.epoch
	plan := message.CLPAEpochPlan{Epoch: 1, BaseEpoch: 0, Mapping: map[string]uint64{}, InputCuts: map[uint64]uint64{0: 0, 1: 0}}
	plan.PlanHash = plan.Digest()
	e.plan = &plan
	e.rounds = 3
	e.sentReady = true
	for sid := uint64(0); sid < 2; sid++ {
		e.ready[sid] = message.CLPAEpochReady{Epoch: 1, PlanHash: plan.PlanHash, FromShard: sid, RelayCut: 3}
	}
	// A missing peer entry must not be interpreted as an already drained peer.
	if ok, r := in.epochPropose(); ok || r != nil || e.sentTransfers {
		t.Fatal("missing relay history bypassed closure")
	}
	q := &epochPrefix{}
	apply := func([]byte) error { return nil }
	if err := q.accept(1, []byte("one"), apply); err != nil {
		t.Fatal(err)
	}
	if err := q.accept(3, []byte("three"), apply); err != nil {
		t.Fatal(err)
	}
	e.relays[1] = q
	if ok, r := in.epochPropose(); ok || r != nil || e.sentTransfers {
		t.Fatal("max relay sequence substituted for contiguous prefix")
	}
}
