package committee

import (
	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/partition"
	"fmt"
	"strings"
	"testing"
)

func forestTestSetPlan(e *clpaEpochController, delta map[string]uint64) {
	e.plan = &message.CLPAEpochPlan{Epoch: e.epoch + 1, BaseEpoch: e.epoch, Mapping: delta, InputCuts: map[uint64]uint64{0: 0, 1: 0}}
	e.plan.PlanHash = e.plan.Digest()
	e.applied = map[[2]uint64]message.CLPAEpochApplied{}
}

func forestTestApplied(e *clpaEpochController, shard, node uint64) message.CLPAEpochApplied {
	return message.CLPAEpochApplied{Epoch: e.plan.Epoch, PlanHash: e.plan.PlanHash, Shard: shard, Node: node, Height: e.plan.Epoch + 3, StateRoot: strings.Repeat("a", 64)}
}

func TestForestEpochSparseProposalApplyKeepAndRemigration(t *testing.T) {
	e := epochTestController(t)
	p := forestTestProvider(t)
	e.provider, e.initialRoutes, e.dispatched = p, p.initialRoutes, 2
	e.created = map[string]bool{forestTestB: true}
	e.ccm.modifiedMap = map[string]uint64{forestTestA: 1, forestTestB: 1}
	// An unrelated sentinel makes accidental full-map provider scans fail.
	// The Forest runtime must not inspect or rebuild the complete routing map.
	e.ccm.modifiedMap["unrelated-full-map-sentinel"] = 0
	graph := e.ccm.clpaGraph
	for epoch, delta := range []map[string]uint64{
		{forestTestA: 0, forestTestB: 0}, // Initial non-hash owner and new hash owner move.
		{},                               // KEEP must preserve both committed overrides.
		{forestTestA: 1, forestTestB: 1}, // Return to initial/hash bases removes overrides.
		{forestTestA: 0},                 // Move the same initial account again.
	} {
		before := copyEpochMapping(e.ccm.modifiedMap)
		confirmed := clpaEpochMappingDigest(e.committedOverrides)
		p.call = func(r forestEpochRequest) (forestEpochReply, error) {
			if clpaEpochMappingDigest(r.CurrentMapping) != clpaEpochMappingDigest(copyEpochMapping(e.committedOverrides)) {
				t.Fatal("worker did not receive committed overrides")
			}
			return forestPlanReply(p, r, delta), nil
		}
		proposal, err := e.planMappingLocked()
		if err != nil {
			t.Fatal(err)
		}
		if proposal.mapping != nil || clpaEpochMappingDigest(e.ccm.modifiedMap) != clpaEpochMappingDigest(before) || confirmed != clpaEpochMappingDigest(e.committedOverrides) {
			t.Fatal("proposal published owners or materialized full mapping")
		}
		for account, target := range delta {
			if proposal.moves[account]["from"] != before[account] || proposal.moves[account]["to"] != target {
				t.Fatal("move audit does not use actual prior route")
			}
		}
		forestTestSetPlan(e, proposal.delta)
		if e.applyPlannedMappingLocked(nil) == nil {
			t.Fatal("applied before replica reports")
		}
		for shard := uint64(0); shard < 2; shard++ {
			for node := uint64(0); node < 4; node++ {
				if err = e.acceptAppliedLocked(forestTestApplied(e, shard, node)); err != nil {
					t.Fatal(err)
				}
				if len(e.applied) < 8 && e.applyPlannedMappingLocked(nil) == nil {
					t.Fatal("partial replica reports published mapping")
				}
			}
		}
		if clpaEpochMappingDigest(e.ccm.modifiedMap) != clpaEpochMappingDigest(before) || confirmed != clpaEpochMappingDigest(e.committedOverrides) {
			t.Fatal("reports alone published mapping")
		}
		if err = e.applyPlannedMappingLocked(nil); err != nil {
			t.Fatal(err)
		}
		for account, target := range delta {
			if e.ccm.modifiedMap[account] != target {
				t.Fatal("delta not installed")
			}
		}
		if e.ccm.clpaGraph != graph || len(graph.PartitionMap) != 0 {
			t.Fatal("Forest rebuilt CLPA graph")
		}
		wantOverrides := []int{2, 2, 0, 1}[epoch]
		if len(e.committedOverrides) != wantOverrides {
			t.Fatal("KEEP/return-to-base/remigration lost committed overrides", e.committedOverrides)
		}
		e.epoch = e.plan.Epoch
	}
}

func TestForestEpochSparseRejectsUncreatedAndFailedProposal(t *testing.T) {
	e := epochTestController(t)
	p := forestTestProvider(t)
	e.provider, e.initialRoutes, e.dispatched = p, p.initialRoutes, 2
	e.ccm.modifiedMap = map[string]uint64{forestTestA: 1, forestTestC: 0}
	p.call = func(r forestEpochRequest) (forestEpochReply, error) {
		return forestPlanReply(p, r, map[string]uint64{forestTestC: 1}), nil
	}
	if _, err := e.planMappingLocked(); err == nil {
		t.Fatal("routing-only account was considered created")
	}
	if p.lastEpoch != 0 || len(e.committedOverrides) != 0 || e.ccm.modifiedMap[forestTestC] != 0 {
		t.Fatal("invalid proposal changed committed state")
	}
	p.call = func(r forestEpochRequest) (forestEpochReply, error) {
		return forestEpochReply{}, fmt.Errorf("worker failed")
	}
	if _, err := e.planMappingLocked(); err == nil {
		t.Fatal("worker failure accepted")
	}
	if p.lastEpoch != 0 || len(e.committedOverrides) != 0 {
		t.Fatal("failed proposal changed committed state")
	}
}

func TestCLPAEpochFullMappingApplyCompatibility(t *testing.T) {
	for _, provider := range []CLPAEpochPlanProvider{nativeCLPAProvider{}, rotatingFixtureProvider{}} {
		t.Run(provider.Name(), func(t *testing.T) {
			e := epochTestController(t)
			e.provider = provider
			mapping := map[string]uint64{forestTestA: 1, forestTestB: 0}
			forestTestSetPlan(e, mapping)
			for shard := uint64(0); shard < 2; shard++ {
				for node := uint64(0); node < 4; node++ {
					if err := e.acceptAppliedLocked(forestTestApplied(e, shard, node)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := e.applyPlannedMappingLocked(mapping); err != nil {
				t.Fatal(err)
			}
			mapping[forestTestA] = 0
			if e.ccm.modifiedMap[forestTestA] != 1 || e.ccm.clpaGraph.PartitionMap[partition.Vertex{Addr: forestTestA}] != 1 || len(e.ccm.clpaGraph.PartitionMap) != 2 {
				t.Fatal("CLPA/fixture copy/reset behavior changed")
			}
		})
	}
}

// Small bounded benchmark: setup is excluded; application touches one moved
// account regardless of unrelated routing-table size. This is not a native run.
func BenchmarkForestEpochSparseMapApply(b *testing.B) {
	oldShards, oldNodes := params.ShardNum, params.NodesInShard
	params.ShardNum, params.NodesInShard = 2, 4
	defer func() { params.ShardNum, params.NodesInShard = oldShards, oldNodes }()
	for _, size := range []int{1, 4096} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			p := &forestEpochProvider{initialRoutes: map[string]uint64{forestTestA: 1}}
			e := &clpaEpochController{provider: p, ccm: &CLPACommitteeModule{modifiedMap: map[string]uint64{}}, committedOverrides: map[string]uint64{forestTestA: 0}}
			for i := 0; i < size; i++ {
				e.ccm.modifiedMap[fmt.Sprintf("%040x", i)] = 0
			}
			forestTestSetPlan(e, map[string]uint64{forestTestA: 0})
			for shard := uint64(0); shard < 2; shard++ {
				for node := uint64(0); node < 4; node++ {
					e.applied[[2]uint64{shard, node}] = forestTestApplied(e, shard, node)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := e.applyPlannedMappingLocked(nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
