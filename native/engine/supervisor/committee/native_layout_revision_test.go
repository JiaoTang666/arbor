package committee

import (
	"blockEmulator/core"
	"blockEmulator/initialledger"
	"blockEmulator/params"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestContinuousRevisionSingletonDynamicBindingAtSamePrefix(t *testing.T) {
	oldTotal := params.TotalDataSize
	params.TotalDataSize = 1000
	defer func() { params.TotalDataSize = oldTotal }()
	t.Setenv("SAA_SERIAL_GROUP_CONTROL", "true")
	dir := t.TempDir()
	for _, key := range []string{"SAA_GROUP_SOURCE_PREPARE_PATH", "SAA_GROUP_SOURCE_PREPARED_PATH", "SAA_GROUP_TARGET_INSTALL_PATH", "SAA_GROUP_TARGET_INSTALLED_PATH", "SAA_GROUP_TARGET_ACTIVATION_PATH", "SAA_GROUP_TARGET_ACTIVATED_PATH", "SAA_GROUP_SOURCE_RETIRED_PATH"} {
		t.Setenv(key, filepath.Join(dir, key))
	}
	account := "0000000000000000000000000000000000000002"
	dynamic := map[string]uint64{account: 2}
	s := &serialGroupControl{directory: dir, continuous: true, next: 2, version: 1, lastCutoff: 100, routes: map[string]uint64{}, history: map[string]nativeGroupSpec{}, used: map[string]bool{}, initial: map[string]uint64{}, dynamic: dynamic, shards: 4}
	g := &nativeGroupControl{serial: s, stage: -1}
	atomic.StoreInt64(&g.inputCount, 100)
	spec := nativeGroupSpec{Arm: "move", AfterOccurrences: 100, Command: core.GroupPrepareCommand{Members: []core.SourcePrepareCommand{{ProposalID: "revision-batch-2", Account: account, SourceShard: 2, TargetShard: 3, MappingVersion: 1, BindAtProposal: true}}}}
	p := serialGroupSelection{Schema: "saa_serial_selection_v2", Round: 2, BaseVersion: 1, ObservedOccurrences: 100, Decision: "MOVE", Spec: spec, ExpiresAt: time.Now().Add(time.Minute)}
	raw, _ := json.Marshal(p)
	if err := os.WriteFile(filepath.Join(dir, "round-002.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := g.poll(); err != nil {
		t.Fatal(err)
	}
	if g.stage != 1 || !s.loaded || s.version != 1 || g.active {
		t.Fatal("dynamic singleton was rejected or route activated before commitment")
	}
	if _, err := os.Stat(groupControlPath("SAA_GROUP_SOURCE_PREPARE_PATH", "revision-batch-2")); err != nil {
		t.Fatal("native preparation intent not emitted", err)
	}
}

func TestRevisionMemberOwnerIsNativeBindingNotHashOrDefaultZero(t *testing.T) {
	a := "0000000000000000000000000000000000000002"
	s := &serialGroupControl{initial: map[string]uint64{}, dynamic: map[string]uint64{a: 2}, routes: map[string]uint64{}}
	spec := nativeGroupSpec{Arm: "move", AfterOccurrences: 100, Command: core.GroupPrepareCommand{Members: []core.SourcePrepareCommand{{ProposalID: "owner", Account: a, SourceShard: 2, TargetShard: 3, BindAtProposal: true}}}}
	if _, err := validateDeferredGroupSpec(spec, "move", 100, s.memberRoutes(spec), 4); err != nil {
		t.Fatal(err)
	}
	spec.Command.Members[0].SourceShard = 0
	if _, err := validateDeferredGroupSpec(spec, "move", 100, s.memberRoutes(spec), 4); err == nil {
		t.Fatal("wrong source accepted")
	}
	delete(s.dynamic, a)
	if _, err := validateDeferredGroupSpec(spec, "move", 100, s.memberRoutes(spec), 4); err == nil {
		t.Fatal("unknown account interpreted as shard zero")
	}
}

func TestDynamicMigrationPopulationUsesActualOriginalShard(t *testing.T) {
	oldShards := params.ShardNum
	params.ShardNum = 4
	defer func() { params.ShardNum = oldShards }()
	a := "0000000000000000000000000000000000000002"
	dynamic := map[string]uint64{a: 2}
	s := &serialGroupControl{initial: map[string]uint64{}, dynamic: dynamic, routes: map[string]uint64{a: 3}, populationApplied: map[string]bool{}, version: 1}
	m := &RelayCommitteeModule{initialLedger: &initialledger.Ledger{Routes: s.initial}, nativeRoutes: dynamic, offeredLoadTPS: 2000, groupControl: &nativeGroupControl{serial: s}, newNewWork: &newNewWorkLedger{cumulative: true, population: []int64{0, 0, 1, 0}, work: make([]int64, 4)}}
	x, y := m.nativePlacement(a, a)
	if x != 3 || y != 3 || m.newNewWork.population[0] != 0 || m.newNewWork.population[2] != 0 || m.newNewWork.population[3] != 1 {
		t.Fatal("dynamic migration corrupted per-shard population", m.newNewWork.population)
	}
	m.nativePlacement(a, a)
	if m.newNewWork.population[3] != 1 {
		t.Fatal("migration population delta repeated")
	}
}
