package committee

import "testing"

func newSAASoftTestManager(shards uint64) *RuntimeUnknownAdmissionManager {
	return &RuntimeUnknownAdmissionManager{
		shardNum:              shards,
		softDecisionMode:      "soft_decay",
		softDecay:             defaultSAASoftDecayConfig(),
		saaSoftStateByAccount: make(map[string]*saaSoftAccountState),
		saaSoftLoadByShard:    make(map[uint64]saaSoftLoadState),
		saaSoftLoadLastRound:  -1,
		livePressureByShard:   make(map[uint64]float64),
		liveQueueByShard:      make(map[uint64]float64),
		shardAdmissionCounts:  make(map[uint64]int),
		recentRoutePressure:   make(map[int]map[uint64]int),
		lastRouteRound:        -1,
	}
}

func TestSAASoftDecayAndScore(t *testing.T) {
	if saaDecayFactor(0, 4) != 0 {
		t.Fatal("zero delta must not decay")
	}
	if saaDecayFactor(4, 4) <= 0 || saaDecayFactor(4, 4) >= 1 {
		t.Fatal("decay factor out of range")
	}
	cfg := defaultSAASoftDecayConfig()
	low := saaScoreTarget(saaSoftTargetFeatures{AnchorConfidence: .9, LocalityConfidence: .9, Persistence: .8, CrossCost: .1, LoadPressure: .1, MigrationRisk: .1}, cfg)
	highLoad := saaScoreTarget(saaSoftTargetFeatures{AnchorConfidence: .9, LocalityConfidence: .9, Persistence: .8, CrossCost: .1, LoadPressure: 1, MigrationRisk: .9}, cfg)
	if low <= highLoad {
		t.Fatalf("load/risk penalty not applied: %v <= %v", low, highLoad)
	}
	if saaMigrationIntensity(0, cfg) >= .5 {
		t.Fatal("zero delta should retain current target")
	}
}

func TestSAASoftPolicyObservesWithoutEvidence(t *testing.T) {
	m := newSAASoftTestManager(4)
	decision := m.decideSAASoftTargetLocked("0x0000000000000000000000000000000000000001", nil, 1, 1, true)
	if decision.Action != "observe" {
		t.Fatalf("expected observe, got %s", decision.Action)
	}
}

func TestSAASoftAccountStateUsesIncrementalEvidence(t *testing.T) {
	m := newSAASoftTestManager(2)
	obs := &runtimeUnknownObservation{TotalTxCount: 1, ShardEvidence: map[uint64]int{0: 1}, StableShardEvidence: map[uint64]int{0: 1}}
	state := m.updateSAASoftAccountStateLocked("a", obs, 1)
	first := state.LocalityByShard[0]
	obs.TotalTxCount = 2
	obs.ShardEvidence[1] = 1
	obs.StableShardEvidence[1] = 1
	state = m.updateSAASoftAccountStateLocked("a", obs, 2)
	if state.LocalityByShard[1] <= 0 {
		t.Fatal("new shard evidence was not incorporated")
	}
	if state.LocalityByShard[0] >= first {
		t.Fatal("old locality evidence did not decay")
	}
}

func TestSAASoftStateDoesNotApplyEpochDecayPerTransaction(t *testing.T) {
	m := newSAASoftTestManager(2)
	obs := &runtimeUnknownObservation{ShardEvidence: map[uint64]int{}, StableShardEvidence: map[uint64]int{}}
	for count := 1; count <= 100; count++ {
		obs.TotalTxCount = count
		obs.ShardEvidence[0] = count
		obs.StableShardEvidence[0] = count
		m.updateSAASoftAccountStateLocked("a", obs, 7)
	}
	state := m.saaSoftStateByAccount["a"]
	expected := saaDecayFactor(1, m.softDecay.ObservationTau)
	if state.LocalityByShard[0] != expected {
		t.Fatalf("same-epoch transactions changed decay constant: got=%f expected=%f", state.LocalityByShard[0], expected)
	}
}

func TestSAASoftPolicyRespondsToLoad(t *testing.T) {
	m := newSAASoftTestManager(2)
	m.softDecay.LoadWeight = 0.35
	m.livePressureByShard = map[uint64]float64{0: 10, 1: 1}
	m.liveQueueByShard = map[uint64]float64{0: 10, 1: 1}
	obs := &runtimeUnknownObservation{TotalTxCount: 4, ShardEvidence: map[uint64]int{0: 2, 1: 2}, StableShardEvidence: map[uint64]int{0: 2, 1: 2}}
	decision := m.decideSAASoftTargetLocked("a", obs, 1, 0, false)
	if decision.SelectedShard != 1 || decision.Action != "migrate" {
		t.Fatalf("expected continuous load penalty to select shard 1, got shard=%d action=%s delta=%f", decision.SelectedShard, decision.Action, decision.ScoreDelta)
	}
}

func TestSAASoftRetainAuditUsesCurrentScore(t *testing.T) {
	m := newSAASoftTestManager(2)
	obs := &runtimeUnknownObservation{TotalTxCount: 1, ShardEvidence: map[uint64]int{0: 1}, StableShardEvidence: map[uint64]int{0: 1}}
	decision := m.decideSAASoftTargetLocked("a", obs, 1, 0, false)
	if decision.Action != "retain" {
		t.Fatalf("expected retain, got %s", decision.Action)
	}
	if decision.SelectedShard != decision.CurrentShard || decision.SelectedScore != decision.CurrentScore {
		t.Fatalf("retained decision has inconsistent selected/current fields: %+v", decision)
	}
}

func TestParseSAADecisionModeDefaultsToLegacy(t *testing.T) {
	if got := parseSAADecisionMode(""); got != "legacy" {
		t.Fatalf("got %q", got)
	}
	if got := parseSAADecisionMode("soft_decay"); got != "soft_decay" {
		t.Fatalf("got %q", got)
	}
}
