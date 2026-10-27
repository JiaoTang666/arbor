package committee

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"blockEmulator/utils"
)

func integratedTestAddress(value uint64) string {
	return fmt.Sprintf("0x%040x", value)
}

func newIntegratedTestManager(shardNum uint64) *RuntimeUnknownAdmissionManager {
	return &RuntimeUnknownAdmissionManager{
		enabled:                  true,
		dynamicAdmissionEnabled:  true,
		shardNum:                 shardNum,
		softDecisionMode:         saaIntegratedDecisionMode,
		stableShardByAccount:     make(map[string]uint64),
		stableClusterByAccount:   make(map[string]int),
		stableShardByCluster:     make(map[int]uint64),
		externalShardByAccount:   make(map[string]uint64),
		runtimeShardByAccount:    make(map[string]uint64),
		pendingRuntimeShard:      make(map[string]uint64),
		pendingRuntimeRouteRound: make(map[string]int),
		pendingRuntimeUpdate:     make(map[string]runtimeUnknownAdmissionUpdate),
		appliedExternalUpdates:   make(map[string]bool),
		shardAdmissionCounts:     make(map[uint64]int),
		observations:             make(map[string]*runtimeUnknownObservation),
		lastRuntimeRefreshRound:  make(map[string]int),
		liveQueueByShard:         make(map[uint64]float64),
		livePressureByShard:      make(map[uint64]float64),
	}
}

func TestSAAIntegratedRuntimeProposalIsFutureEffective(t *testing.T) {
	m := newIntegratedTestManager(4)
	account := integratedTestAddress(101)
	peer := integratedTestAddress(202)
	m.stableShardByAccount[peer] = 2
	m.stableClusterByAccount[peer] = 7
	m.stableShardByCluster[7] = 2
	for shardID := uint64(0); shardID < m.shardNum; shardID++ {
		m.liveQueueByShard[shardID] = 0
	}
	m.pressureHintEpoch = 5
	obs := &runtimeUnknownObservation{
		TotalTxCount:          10,
		FirstObservedRound:    5,
		LastObservedRound:     5,
		ShardEvidence:         map[uint64]int{2: 10},
		StableShardEvidence:   map[uint64]int{2: 10},
		StableClusterEvidence: map[int]int{7: 10},
	}
	m.decideSAAIntegratedLocked(account, obs, 5, true)
	if _, ok := m.lookupAssignedShardLocked(account); ok {
		t.Fatal("proposal became visible before its effective epoch")
	}
	m.applySAAIntegratedDueLocked(5)
	if _, ok := m.lookupAssignedShardLocked(account); ok {
		t.Fatal("route became visible in the decision epoch")
	}
	m.applySAAIntegratedDueLocked(6)
	shardID, ok := m.lookupAssignedShardLocked(account)
	if !ok || shardID != 2 {
		t.Fatalf("effective route = (%d, %v), want (2, true)", shardID, ok)
	}
	if got := m.CurrentRouteVersion(); got != 1 {
		t.Fatalf("route version = %d, want 1", got)
	}
	if len(m.runtimeShardByAccount) != 0 || len(m.externalShardByAccount) != 0 {
		t.Fatal("integrated mode wrote a legacy route map")
	}
}

func TestSAAIntegratedRuntimeMissingQueueKeepsButAdmits(t *testing.T) {
	m := newIntegratedTestManager(4)
	account := integratedTestAddress(303)
	current := uint64(0)
	for candidate := uint64(0); candidate < m.shardNum; candidate++ {
		if candidate == uint64(utilsAddr2ShardForTest(account)) {
			current = candidate
			break
		}
	}
	obs := &runtimeUnknownObservation{
		TotalTxCount:          5,
		FirstObservedRound:    8,
		LastObservedRound:     8,
		ShardEvidence:         map[uint64]int{(current + 1) % 4: 5},
		StableShardEvidence:   map[uint64]int{(current + 1) % 4: 5},
		StableClusterEvidence: map[int]int{},
	}
	m.decideSAAIntegratedLocked(account, obs, 8, true)
	m.applySAAIntegratedDueLocked(9)
	shardID, ok := m.lookupAssignedShardLocked(account)
	if !ok || shardID != current {
		t.Fatalf("missing queue signal route = (%d, %v), want hash shard %d", shardID, ok, current)
	}
}

func utilsAddr2ShardForTest(account string) int {
	return utils.Addr2Shard(account)
}
func TestSAAIntegratedExternalUpdateUsesAuthority(t *testing.T) {
	m := newIntegratedTestManager(4)
	m.externalActionEnabled = true
	account := integratedTestAddress(404)
	path := filepath.Join(t.TempDir(), "external.jsonl")
	payload := fmt.Sprintf("{\"proposal_id\":\"external-1\",\"effective_epoch\":1,\"source_shard\":0,\"target_shard\":3,\"addresses\":[\"%s\"]}\n", account)
	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(runtimeExternalMappingUpdatePathEnv, path)

	m.refreshExternalMappingUpdatesLocked(1)
	if len(m.externalShardByAccount) != 0 {
		t.Fatal("external scheduler bypassed the integrated mapping authority")
	}
	if _, ok := m.lookupAssignedShardLocked(account); ok {
		t.Fatal("external proposal became visible before the authority effective epoch")
	}
	m.applySAAIntegratedDueLocked(2)
	shardID, ok := m.lookupAssignedShardLocked(account)
	if !ok || shardID != 3 {
		t.Fatalf("external authoritative route = (%d, %v), want (3, true)", shardID, ok)
	}
}
