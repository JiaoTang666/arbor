package committee

import (
	"blockEmulator/initialledger"
	"blockEmulator/message"
	"blockEmulator/params"
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Only test fixtures are synthesized here. The deployment artifact and its
// 16-shard training provenance are not rewritten for another topology.
func costScaleFixture(t *testing.T, shards int) (*RelayCommitteeModule, *bytes.Buffer) {
	t.Helper()
	model := testCostModel()
	model.Coef[0], model.Coef[19] = 1, .125
	data, e := json.Marshal(model)
	if e != nil { t.Fatal(e) }
	path := filepath.Join(t.TempDir(), "fixture-model.json")
	if e = os.WriteFile(path, data, 0600); e != nil { t.Fatal(e) }
	hash := sha256.Sum256(data)
	t.Setenv("SAA_NEWNEW_MODEL_PATH", path)
	t.Setenv("SAA_NEWNEW_MODEL_SHA256", hex.EncodeToString(hash[:]))
	share, e := newSharePlacement("cost_ranker_v1", shards)
	if e != nil { t.Fatal(e) }
	prior := &newNewPrior{Cutoff: 2, SourceTime: 1000, Rates: make([][]float64, shards), Persistence: make([]float64, shards), Arrivals: make([]float64, shards), Followup: make([]float64, shards), Mass: make([]float64, shards)}
	for s := range prior.Rates { prior.Rates[s] = make([]float64, 3) }
	source := make([]int64, 4000)
	for i := range source { source[i] = 1000 + int64(i) }
	population := make([]int64, shards)
	population[0], population[shards-1] = 1, 1
	var log bytes.Buffer
	m := &RelayCommitteeModule{
		initialLedger: &initialledger.Ledger{Routes: map[string]uint64{forestHookAddrV2(1): 0, forestHookAddrV2(5): uint64(shards-1)}},
		nativeRoutes: map[string]uint64{}, offeredLoadTPS: float64(125*shards), offeredAuditPath: "in-memory-lifecycle",
		newNewWork: &newNewWorkLedger{cumulative: true, work: make([]int64, shards), population: population},
		placementSignals: newPlacementSignals(shards), nativeAudit: &batchAudit{w: bufio.NewWriter(&log)},
		newNewPolicy: &newNewPlacementPolicy{mode: "cost_ranker_v1", source: source, base: 2, prior: prior, bindingSequence: map[string]int64{}, share: share},
	}
	m.placementSignals.enableHistory()
	m.forestFacts, e = NewForestFactsV2(map[string]string{forestHookAddrV2(1): "history:-2", forestHookAddrV2(5): "history:-3"}, "history", 2, m.placementSignals.clock.id, shards)
	if e != nil { t.Fatal(e) }
	m.forestJoint = &forestJointCacheV2{
		meta: forestSlowMetaV2{Schema: "forest_slow_facts_v2", Semantics: "source_activity_facts_v2.0", Version: 0, Cutoff: 2, SourceTime: 1000, Namespace: "history", SeedCutoff: 2, Shards: shards},
		joint: forestJointFileV2{Schema: "graph_forest_joint_v2", Graph: json.RawMessage(`{}`), Forest: json.RawMessage(`{"schema":"forest_slow_facts_v2"}`)},
		digest: "test-only-immutable-validated-cache", lightDayRates: make([]float64, shards),
	}
	return m, &log
}

func TestCostRankerScaleTopologies(t *testing.T) {
	old := params.ShardNum
	defer func() { params.ShardNum = old }()
	for _, shards := range []int{4, 8, 16, 32} {
		t.Run(fmt.Sprintf("shards%d", shards), func(t *testing.T) {
			params.ShardNum = shards
			m, log := costScaleFixture(t, shards)
			for _, v := range m.newNewPolicy.share.previous {
				if v != 1/float64(shards) { t.Fatal("initial previous pressure share is not 1/S") }
			}
			// A queue report is observation, not confirmation, at every topology.
			report := message.PlacementTelemetryV2{Schema: "placement_telemetry_v2", Shard: uint64(shards-1), Node: 0, View: 0, ClockID: "scale-node", Sequence: 1, CapturedMonoNS: 0, Queue: 9, ServiceMissing: true}
			if ok, _, e := m.placementSignals.receiveLightV2(report); e != nil || !ok || m.placementSignals.rows[shards-1].Confirmed != 0 { t.Fatal("dynamic telemetry failed", e) }
			var stages int64
			pairs := [][2]int{{1, 2}, {3, 1}, {8, 9}, {10, 10}, {8, 9}, {1, 5}, {8, 11}, {12, 13}}
			for seq, pair := range pairs {
				tx := forestHookTransactionV2(t, m, uint64(seq), pair[0], pair[1])
				sa, sb := tx.RoutedSenderShard, tx.RoutedRecipientShard
				if sa >= uint64(shards) || sb >= uint64(shards) { t.Fatal("route outside topology") }
				stages++
				if seq != 5 && sa != sb { t.Fatal("new-old/new-new association broken") }
				if sa == sb {
					block := forestHookBlockV2(tx, "inner", sa)
					m.auditBlock(block); m.auditBlock(block)
				} else {
					stages++
					second := forestHookBlockV2(tx, "relay2", sb)
					first := forestHookBlockV2(tx, "relay1", sa)
					m.auditBlock(second); m.auditBlock(second); m.auditBlock(first)
				}
			}
			audit, e := m.forestFacts.CompleteAudit(uint64(len(pairs)))
			if e != nil || len(audit.AssignedStages) != shards || !reflect.DeepEqual(audit.AssignedStages, audit.ConfirmedStages) { t.Fatal("dynamic work conservation", audit, e) }
			var actual int64
			for _, n := range audit.AssignedStages { actual += n }
			if actual != stages || stages != int64(len(pairs)+1) { t.Fatal("local/cross/duplicate stage mismatch", actual, stages) }
			m.nativeAudit.flush()
			previous := make([]float64, shards)
			for s := range previous { previous[s] = 1/float64(shards) }
			decisions := 0
			for _, line := range bytes.Split(bytes.TrimSpace(log.Bytes()), []byte("\n")) {
				var row struct {
					Event string `json:"event"`
					Sequence uint64 `json:"sequence"`
					Raw [][]float64 `json:"raw_features"`
					Previous []float64 `json:"previous_pressure_share"`
					Base []float64 `json:"base_proportions"`
					Candidates []uint64 `json:"candidate_ids"`
					Fast ForestFactsSnapshotV2 `json:"fast"`
				}
				if e = json.Unmarshal(line, &row); e != nil { t.Fatal(e) }
				if row.Event != "cost_entry_decision" { continue }
				decisions++
				if len(row.Raw) != shards || len(row.Base) != shards || len(row.Candidates) != 4 || !reflect.DeepEqual(row.Previous, previous) || row.Fast.BoundPrefix != row.Sequence || row.Fast.AssignedPrefix != row.Sequence { t.Fatal("dynamic causal policy contract") }
				if e = checkForestSnapshotWidthV1(row.Fast, shards); e != nil { t.Fatal(e) }
				for s, values := range row.Raw {
					if len(values) != 20 || values[19] != previous[s] { t.Fatal("20-dimensional/previous contract") }
				}
				previous = row.Base
			}
			if decisions != 3 || !reflect.DeepEqual(m.newNewPolicy.share.previous, previous) || !reflect.DeepEqual(m.newNewPolicy.share.prob, previous) { t.Fatal("not per-NN base pressure updates", decisions) }
			if m.newNewPolicy.share.costModel.Normalizer.FittedOn != "training points only; all 16 shards per point" { t.Fatal("deployment rewrote model provenance") }
		})
	}
}

func TestCostRankerScaleSupportAndNearTies(t *testing.T) {
	for _, shards := range []int{4, 8, 16, 32} {
		for _, support := range []int{1, 3} {
			p := make([]float64, shards)
			if support == 1 { p[shards-1] = 1 } else { p[0], p[1], p[shards-1] = .125, .375, .5 }
			ids, u, e := sampleCostCandidates(p, shareRandomContext, 7)
			if e != nil || len(ids) != support || len(u) != support { t.Fatal("positive-support slate", shards, support, e) }
			seen := map[uint64]bool{}
			for _, s := range ids {
				if s >= uint64(shards) || p[s] <= 0 || seen[s] { t.Fatal("zero/repeated candidate") }
				seen[s] = true
			}
			first, e := costPressureDraw(p, u[0])
			if e != nil || first != ids[0] { t.Fatal("candidate zero differs from P") }
			if support == 3 {
				chosen, e := chooseCostCandidate(ids, []float64{1e-7, 0, 2})
				if e != nil || chosen != ids[0] { t.Fatal("near tie changed candidate-order preference") }
				chosen, e = chooseCostCandidate(ids, []float64{2e-6, 0, 2})
				if e != nil || chosen != ids[1] { t.Fatal("physical cost minimum ignored") }
			}
		}
	}
	for _, u := range []float64{0, .125, .5, math.Nextafter(1, 0)} {
		if got, e := costPressureDraw([]float64{.125, .375, .5, 0}, u); e != nil || got >= 3 { t.Fatal("CDF selected zero tail", got, e) }
	}
	if got, e := costPressureDraw([]float64{.5, .5}, .5); e != nil || got != 1 { t.Fatal("right-open CDF boundary", got, e) }
	if _, e := costPressureDraw(nil, 0); e == nil { t.Fatal("empty topology accepted") }
}

func TestCostRankerScalePreserves16Arithmetic(t *testing.T) {
	for _, p := range [][]float64{
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{.125, .375, .5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{.0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625},
	} {
		for _, u := range []float64{0, .125, .5, .875, math.Nextafter(1, 0)} {
			old, oldErr := shareDraw(p, u)
			current, currentErr := costPressureDraw(p, u)
			if oldErr != nil || currentErr != nil || old != current { t.Fatal("16-shard CDF arithmetic changed", u, old, current, oldErr, currentErr) }
		}
	}
	if _, e := newSharePlacement("pressure_share", 4); e == nil { t.Fatal("legacy policy contract was widened") }
	if _, e := newSharePlacement("cost_ranker_v1", 0); e == nil { t.Fatal("zero shards accepted") }
}

func TestForestReplayTopologyAndWidths(t *testing.T) {
	for _, shards := range []int{4, 8, 16, 32} {
		data, _ := json.Marshal(forestReplayTopologyV1{Schema: "full_shard_scale_v1", Shards: shards, NodesPerShard: 4, InjectSpeed: 125*shards, ExpectedTransactions: 3})
		topology, e := parseForestReplayTopologyV1(fmt.Sprint(shards), data)
		if e != nil || topology.Shards != shards { t.Fatal("manifest topology not read", e) }
		if _, e = parseForestReplayTopologyV1(fmt.Sprint(shards+1), data); e == nil { t.Fatal("env/manifest mismatch accepted") }
		f, e := NewForestFactsV2(map[string]string{}, "test", 0, "clock", shards)
		if e != nil { t.Fatal(e) }
		snapshot, e := f.Snapshot(0, "clock")
		if e != nil || checkForestSnapshotWidthV1(snapshot, shards) != nil { t.Fatal("valid snapshot width rejected", e) }
		if e = checkForestSnapshotWidthV1(snapshot, shards+1); e == nil { t.Fatal("other-topology snapshot accepted") }
		snapshot.AssignedCategoryHalfStages["runtime_newnew"] = nil
		if e = checkForestSnapshotWidthV1(snapshot, shards); e == nil { t.Fatal("missing category width accepted") }
	}
	if _, e := parseForestReplayTopologyV1("", []byte(`{}`)); e == nil { t.Fatal("implicit topology accepted") }
}
