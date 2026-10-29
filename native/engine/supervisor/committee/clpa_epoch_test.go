package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/partition"
	"bytes"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"
	"time"
)

func epochTestController(t *testing.T) *clpaEpochController {
	t.Helper()
	oldShards, oldNodes, oldSpeed := params.ShardNum, params.NodesInShard, params.InjectSpeed
	params.ShardNum, params.NodesInShard, params.InjectSpeed = 2, 4, 100
	t.Cleanup(func() { params.ShardNum, params.NodesInShard, params.InjectSpeed = oldShards, oldNodes, oldSpeed })
	graph := new(partition.CLPAState)
	graph.Init_CLPAState(.5, 100, 2)
	return &clpaEpochController{ccm: &CLPACommitteeModule{dataTotalNum: 1000, clpaGraph: graph, modifiedMap: map[string]uint64{}},
		audit: &bytes.Buffer{}, rounds: 3, counts: map[uint64]int{}, blocks: map[string]bool{},
		cuts: map[uint64]uint64{}, applied: map[[2]uint64]message.CLPAEpochApplied{},
		hashes: map[uint64]string{}, terminal: map[uint64]bool{}, inputHash: strings.Repeat("a", 64)}
}

func TestCLPAEpochAllReplicaBarrier(t *testing.T) {
	e := epochTestController(t)
	e.plan = &message.CLPAEpochPlan{Epoch: 1, BaseEpoch: 0, Mapping: map[string]uint64{}, InputCuts: map[uint64]uint64{0: 0, 1: 0}}
	e.plan.PlanHash = e.plan.Digest()
	for shard := uint64(0); shard < 2; shard++ {
		for node := uint64(0); node < 4; node++ {
			a := message.CLPAEpochApplied{Epoch: 1, PlanHash: e.plan.PlanHash, Shard: shard, Node: node, Height: 3, StateRoot: strings.Repeat("a", 64)}
			if err := e.acceptAppliedLocked(a); err != nil {
				t.Fatal(err)
			}
			if err := e.acceptAppliedLocked(a); err != nil {
				t.Fatal("identical repeat", err)
			}
			if len(e.applied) != int(shard*4+node+1) {
				t.Fatal("duplicate counted as another replica")
			}
		}
	}
	bad := message.CLPAEpochApplied{Epoch: 1, PlanHash: e.plan.PlanHash, Shard: 0, Node: 0, Height: 4, StateRoot: strings.Repeat("a", 64)}
	if e.acceptAppliedLocked(bad) == nil {
		t.Fatal("conflicting replica evidence accepted")
	}
	bad.Epoch = 2
	if e.acceptAppliedLocked(bad) == nil {
		t.Fatal("future epoch accepted")
	}
}

func TestCLPAEpochSameShardRootRequired(t *testing.T) {
	e := epochTestController(t)
	e.plan = &message.CLPAEpochPlan{Epoch: 1, PlanHash: "plan"}
	a := message.CLPAEpochApplied{Epoch: 1, PlanHash: "plan", Shard: 0, Node: 0, Height: 3, StateRoot: strings.Repeat("a", 64)}
	if err := e.acceptAppliedLocked(a); err != nil {
		t.Fatal(err)
	}
	a.Node = 1
	a.StateRoot = strings.Repeat("b", 64)
	if e.acceptAppliedLocked(a) == nil {
		t.Fatal("divergent replica root accepted")
	}
}

func TestCLPAEpochLateAppliedDuplicates(t *testing.T) {
	e := epochTestController(t)
	a := message.CLPAEpochApplied{Epoch: 1, PlanHash: "old-plan", Shard: 0, Node: 0, Height: 3, StateRoot: strings.Repeat("a", 64)}
	e.epoch = 2
	e.appliedHistory = map[uint64]map[[2]uint64]message.CLPAEpochApplied{1: {[2]uint64{0, 0}: a}}
	if err := e.acceptAppliedLocked(a); err != nil {
		t.Fatal("exact historical duplicate rejected", err)
	}
	a.Height++
	if e.acceptAppliedLocked(a) == nil {
		t.Fatal("conflicting historical evidence accepted")
	}
	a.Epoch = 3
	if e.acceptAppliedLocked(a) == nil {
		t.Fatal("future unplanned apply accepted")
	}
}

func TestCLPAEpochRequiresAllShardRounds(t *testing.T) {
	e := epochTestController(t)
	e.counts[0] = 3
	if e.readyLocked() {
		t.Fatal("first shard alone released epoch")
	}
	e.counts[1] = 2
	if e.readyLocked() {
		t.Fatal("incomplete rounds released epoch")
	}
	e.counts[1] = 3
	if !e.readyLocked() {
		t.Fatal("complete rounds not recognized")
	}
	e.drain = true
	if e.readyLocked() {
		t.Fatal("new epoch after input drain")
	}
}

func TestCLPAEpochOfferedClockIncludesPause(t *testing.T) {
	e := epochTestController(t)
	e.start = time.Unix(1000, 0)
	if got := e.offeredDue(e.start.Add(5 * time.Second)); got != 501 {
		t.Fatal(got)
	}
	e.dispatched = 100
	if got := e.offeredDue(e.start.Add(9*time.Second)) - e.dispatched; got != 801 {
		t.Fatal("pause disappeared", got)
	}
	if got := e.offeredDue(e.start.Add(20 * time.Second)); got != 1000 {
		t.Fatal("not capped", got)
	}
}

func TestCLPAEpochTerminalOccurrenceAndBlockDedup(t *testing.T) {
	e := epochTestController(t)
	tx := core.NewTransaction(strings.Repeat("0", 39)+"1", strings.Repeat("0", 39)+"2", big.NewInt(1), 0, time.Now())
	tx.IngressSequenceAssigned = true
	tx.AdmissionID = "clpa/" + e.inputHash + "/0"
	e.hashes[0] = hex.EncodeToString(tx.TxHash)
	b := &message.BlockInfoMsg{SenderShardID: 0, Epoch: 0, CommittedBlockHash: "first", InnerShardTxs: []*core.Transaction{tx}, CommitTime: time.Now()}
	e.handleBlock(b)
	e.handleBlock(b)
	if e.failure != nil || len(e.terminal) != 1 || e.counts[0] != 1 {
		t.Fatal("block replay not idempotent", e.failure)
	}
	b.CommittedBlockHash = "second"
	e.handleBlock(b)
	if e.failure == nil {
		t.Fatal("second terminal commit of same occurrence accepted")
	}
}

func TestCLPAEpochFixtureOnlyMovesObservedGraph(t *testing.T) {
	e := epochTestController(t)
	a, b := strings.Repeat("0", 39)+"1", strings.Repeat("0", 39)+"2"
	e.ccm.clpaGraph.AddEdge(partition.Vertex{Addr: a}, partition.Vertex{Addr: b})
	current := map[string]uint64{a: 1, b: 0}
	result, err := (rotatingFixtureProvider{}).Plan(1, e.ccm.clpaGraph, current)
	if err != nil || result[a] != 0 || result[b] != 1 || current[a] != 1 {
		t.Fatal(result, err)
	}
	if clpaEpochMappingDigest(map[string]uint64{a: 0, b: 1}) != clpaEpochMappingDigest(result) {
		t.Fatal("unstable mapping digest")
	}
}

func TestCLPAEpochDeltaPreservesUnchangedPriorOwner(t *testing.T) {
	e := epochTestController(t)
	a, b := strings.Repeat("0", 39)+"1", strings.Repeat("0", 39)+"2"
	// Account a was already moved away from its hash owner in an earlier epoch.
	e.ccm.modifiedMap = map[string]uint64{a: 0, b: 0}
	desired := map[string]uint64{a: 0, b: 1}
	delta, moves := e.ccm.clpaEpochDelta(desired)
	if len(delta) != 1 || delta[b] != 1 || len(moves) != 1 || moves[b]["from"] != 0 {
		t.Fatal(delta, moves)
	}
	if _, ok := delta[a]; ok {
		t.Fatal("unchanged historical owner resent")
	}
	installed := copyEpochMapping(e.ccm.modifiedMap)
	for account, target := range delta {
		installed[account] = target
	}
	if installed[a] != 0 || installed[b] != 1 || desired[a] != 0 {
		t.Fatal("prior ownership lost", installed)
	}
}
