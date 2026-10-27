package committee

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMappingBoundaryIsIndependentOfUnknownAdmission(t *testing.T) {
	t.Setenv(runtimeUnknownAdmissionEnabledEnv, "0")
	t.Setenv(runtimeUnknownDynamicEnabledEnv, "1")
	t.Setenv(runtimeMappingBoundaryEnabledEnv, "1")
	t.Setenv(runtimeExternalActionEnabledEnv, "0")
	t.Setenv(runtimeEffectiveConfigDirEnv, t.TempDir())

	manager := NewRuntimeUnknownAdmissionManager(4, nil)
	if !manager.Enabled() {
		t.Fatal("mapping boundary should keep the manager alive when admission is disabled")
	}
	if manager.enabled {
		t.Fatal("unknown admission unexpectedly enabled")
	}
	if manager.dynamicAdmissionEnabled {
		t.Fatal("dynamic admission must remain subordinate to disabled first-touch admission")
	}
	config := manager.effectiveConfiguration()
	if config["mapping_boundary_enabled"] != true {
		t.Fatalf("effective mapping boundary=%v, want true", config["mapping_boundary_enabled"])
	}
	if config["admission_enabled"] != false {
		t.Fatalf("effective admission=%v, want false", config["admission_enabled"])
	}
}

func TestSAASoftEffectiveConfigurationMatchesEnvironment(t *testing.T) {
	t.Setenv(runtimeUnknownDecisionModeEnv, "soft_decay")
	t.Setenv(runtimeUnknownSoftAnchorWeightEnv, "0.31")
	t.Setenv(runtimeUnknownSoftLocalityWeightEnv, "0.29")
	t.Setenv(runtimeUnknownSoftLoadWeightEnv, "0.17")
	t.Setenv(runtimeUnknownMigrationMarginEnv, "0.03")
	manager := NewRuntimeUnknownAdmissionManager(4, nil)
	config := manager.effectiveConfiguration()
	if config["saa_decision_mode"] != "soft_decay" {
		t.Fatalf("mode=%v", config["saa_decision_mode"])
	}
	if config["saa_anchor_weight"] != 0.31 {
		t.Fatalf("anchor=%v", config["saa_anchor_weight"])
	}
	if config["saa_locality_weight"] != 0.29 {
		t.Fatalf("locality=%v", config["saa_locality_weight"])
	}
	if config["saa_load_weight"] != 0.17 {
		t.Fatalf("load=%v", config["saa_load_weight"])
	}
	if config["saa_migration_margin"] != 0.03 {
		t.Fatalf("margin=%v", config["saa_migration_margin"])
	}
}

func TestStableInitialRoutingIsIndependentOfMappingBoundary(t *testing.T) {
	root := t.TempDir()
	accountMap := filepath.Join(root, "account_cluster_map.csv")
	clusterMap := filepath.Join(root, "cluster_shard_map.csv")
	account := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := os.WriteFile(
		accountMap,
		[]byte("account,cluster_id,is_projected_unknown\n"+account+",7,false\n"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		clusterMap,
		[]byte("cluster_id,shard_id,cluster_type\n7,3,stable\n"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	t.Setenv(runtimeUnknownAdmissionEnabledEnv, "0")
	t.Setenv(runtimeUnknownDynamicEnabledEnv, "0")
	t.Setenv(runtimeMappingBoundaryEnabledEnv, "0")
	t.Setenv(runtimeExternalActionEnabledEnv, "0")
	t.Setenv(runtimeAccountClusterMapPathEnv, accountMap)
	t.Setenv(runtimeClusterShardMapPathEnv, clusterMap)
	t.Setenv(runtimeMappingDecisionRequestPathEnv, filepath.Join(root, "request.json"))

	manager := NewRuntimeUnknownAdmissionManager(4, nil)
	if !manager.Enabled() {
		t.Fatal("stable initial router must remain enabled when boundary is disabled")
	}
	shard, ok := manager.CurrentShard(account, 5)
	if !ok || shard != 3 {
		t.Fatalf("stable route=(%d,%v), want (3,true)", shard, ok)
	}
	if _, err := os.Stat(filepath.Join(root, "request.json")); !os.IsNotExist(err) {
		t.Fatalf("boundary-disabled stable routing unexpectedly published a request: %v", err)
	}
}

func TestExternalActionDisabledDoesNotApplyMappingRecord(t *testing.T) {
	root := t.TempDir()
	updatePath := filepath.Join(root, "mapping_updates.jsonl")
	record := map[string]any{
		"proposal_id":     "proposal-disabled-action",
		"effective_epoch": 1,
		"source_shard":    0,
		"target_shard":    2,
		"addresses":       []string{"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(updatePath, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(runtimeUnknownAdmissionEnabledEnv, "0")
	t.Setenv(runtimeUnknownDynamicEnabledEnv, "0")
	t.Setenv(runtimeMappingBoundaryEnabledEnv, "1")
	t.Setenv(runtimeExternalActionEnabledEnv, "0")
	t.Setenv(runtimeExternalMappingUpdatePathEnv, updatePath)

	manager := NewRuntimeUnknownAdmissionManager(4, nil)
	manager.refreshExternalMappingUpdatesLocked(1)
	if manager.routeVersion != 0 {
		t.Fatalf("route version=%d after disabled external action, want 0", manager.routeVersion)
	}
	if len(manager.externalShardByAccount) != 0 {
		t.Fatalf("external route map changed with action disabled: %#v", manager.externalShardByAccount)
	}
}

func TestAnchorRefreshIndependentAndStage3ProposalPaths(t *testing.T) {
	account := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	obs := &runtimeUnknownObservation{
		TotalTxCount:          6,
		ShardEvidence:         map[uint64]int{2: 2, 3: 4},
		StableShardEvidence:   map[uint64]int{2: 2, 3: 4},
		StableClusterEvidence: map[int]int{7: 2, 8: 4},
	}

	for _, tc := range []struct {
		name            string
		mode            string
		expectMapping   bool
		expectProposal  bool
		expectedChannel string
		expectedTarget  string
	}{
		{
			name:            "independent_preserves_existing_path",
			mode:            "independent",
			expectMapping:   true,
			expectProposal:  false,
			expectedChannel: "anchor_independent",
			expectedTarget:  `"target_shard":2`,
		},
		{
			name:            "stage3_emits_proposal_only",
			mode:            "stage3_proposal",
			expectMapping:   false,
			expectProposal:  true,
			expectedChannel: "stage3_anchor_proposal",
			expectedTarget:  `"target_shard":3`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			mappingPath := filepath.Join(root, "mapping.jsonl")
			proposalPath := filepath.Join(root, "proposal.jsonl")
			manager := &RuntimeUnknownAdmissionManager{
				enabled:                  true,
				dynamicAdmissionEnabled:  true,
				shardNum:                 4,
				mappingUpdatePath:        mappingPath,
				minAnchorTx:              1,
				minScoreShare:            0.6,
				localityWeight:           0.45,
				anchorWeight:             0.35,
				crossWeight:              0.15,
				loadWeight:               0.05,
				refreshMaxPerEpoch:       16,
				anchorRealignmentMode:    tc.mode,
				anchorProposalPath:       proposalPath,
				stableShardByCluster:     map[int]uint64{7: 2, 8: 3},
				runtimeShardByAccount:    map[string]uint64{account: 2},
				pendingRuntimeShard:      map[string]uint64{},
				lastRuntimeRefreshRound:  map[string]int{},
				runtimeRefreshCountRound: map[int]int{},
			}

			manager.maybeEmitRuntimeObservationRefreshLocked(account, 3, obs)

			mappingBytes, _ := os.ReadFile(mappingPath)
			proposalBytes, _ := os.ReadFile(proposalPath)
			if got := len(mappingBytes) > 0; got != tc.expectMapping {
				t.Fatalf("mapping path written=%v, want %v", got, tc.expectMapping)
			}
			if got := len(proposalBytes) > 0; got != tc.expectProposal {
				t.Fatalf("proposal path written=%v, want %v", got, tc.expectProposal)
			}
			payload := string(mappingBytes) + string(proposalBytes)
			if !strings.Contains(payload, tc.expectedChannel) {
				t.Fatalf("missing expected control channel %q in %q", tc.expectedChannel, payload)
			}
			if !strings.Contains(payload, tc.expectedTarget) {
				t.Fatalf("missing expected target %q in %q", tc.expectedTarget, payload)
			}
		})
	}
}

func TestQPSASelectsLowerMarginalQueuePotential(t *testing.T) {
	manager := &RuntimeUnknownAdmissionManager{
		shardNum:            2,
		localityWeight:      0.45,
		anchorWeight:        0.35,
		crossWeight:         0.15,
		qpsaQueueWeight:     0.50,
		qpsaDemandCap:       64,
		liveQueueByShard:    map[uint64]float64{0: 100, 1: 10},
		qpsaProjectedDemand: map[int]map[uint64]float64{},
	}
	obs := &runtimeUnknownObservation{
		TotalTxCount:        10,
		ShardEvidence:       map[uint64]int{0: 5, 1: 5},
		StableShardEvidence: map[uint64]int{},
	}

	target, _, evaluations := manager.selectQPSATargetLocked(obs, 4, 0)
	if target != 1 {
		t.Fatalf("QPSA target=%d, want lower-queue shard 1", target)
	}
	if len(evaluations) != 2 {
		t.Fatalf("candidate evaluations=%d, want 2", len(evaluations))
	}
}

func TestQPSAShadowPreservesLegacyTargetAndWritesAudit(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "qpsa.csv")
	manager := &RuntimeUnknownAdmissionManager{
		shardNum:            2,
		localityWeight:      0.45,
		anchorWeight:        0.35,
		crossWeight:         0.15,
		qpsaMode:            "shadow",
		qpsaQueueWeight:     0.50,
		qpsaDemandCap:       64,
		qpsaAuditPath:       auditPath,
		liveQueueByShard:    map[uint64]float64{0: 100, 1: 10},
		qpsaProjectedDemand: map[int]map[uint64]float64{},
	}
	obs := &runtimeUnknownObservation{
		TotalTxCount:        10,
		ShardEvidence:       map[uint64]int{0: 5, 1: 5},
		StableShardEvidence: map[uint64]int{},
	}

	target, mode, _, _ := manager.applyQPSADecisionLocked(
		"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		4,
		obs,
		0,
		"legacy_mode",
		"legacy_reason",
		0.5,
		true,
	)
	if target != 0 || mode != "legacy_mode" {
		t.Fatalf("shadow applied target=%d mode=%q, want legacy target=0 mode=legacy_mode", target, mode)
	}

	handle, err := os.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	records, err := csv.NewReader(handle).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("audit records=%d, want header plus one decision", len(records))
	}
	if records[1][7] != "true" {
		t.Fatalf("shadow disagreement not recorded: %#v", records)
	}
}

func TestQPSAActivePreservesStrongAnchorWhenOverrideDisabled(t *testing.T) {
	manager := &RuntimeUnknownAdmissionManager{
		shardNum:            2,
		localityWeight:      0.45,
		anchorWeight:        0.35,
		crossWeight:         0.15,
		qpsaMode:            "active",
		qpsaQueueWeight:     1.0,
		qpsaDemandCap:       64,
		liveQueueByShard:    map[uint64]float64{0: 1000, 1: 0},
		qpsaProjectedDemand: map[int]map[uint64]float64{},
	}
	obs := &runtimeUnknownObservation{
		TotalTxCount:        10,
		ShardEvidence:       map[uint64]int{0: 10},
		StableShardEvidence: map[uint64]int{0: 10},
	}
	target, mode, _, _ := manager.applyQPSADecisionLocked(
		"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		5,
		obs,
		0,
		"runtime_strong_stable_anchor",
		"stable_anchor_confident",
		1.0,
		false,
	)
	if target != 0 || mode != "runtime_strong_stable_anchor" {
		t.Fatalf("strong anchor was overridden: target=%d mode=%q", target, mode)
	}
}

func TestCloseMappingDecisionsDoesNotDuplicateAuditedRound(t *testing.T) {
	dir := t.TempDir()
	requestPath := filepath.Join(dir, "mapping_decision_request.json")
	auditPath := filepath.Join(dir, "supervisor_routing_boundary_audit.jsonl")
	manager := &RuntimeUnknownAdmissionManager{
		enabled:                         true,
		mappingBoundaryEnabled:          true,
		mappingDecisionMode:             "async_pipeline_lag1",
		mappingDecisionRequestPath:      requestPath,
		mappingDecisionReadyPath:        filepath.Join(dir, "mapping_decision_ready.json"),
		mappingDecisionBarrierAuditPath: auditPath,
		lastMappingDecisionBarrierRound: 119,
		acceptedMappingDecisionRounds:   map[int]bool{119: false},
	}

	manager.CloseMappingDecisions(119)

	if !manager.mappingDecisionsClosed {
		t.Fatal("mapping decision channel was not closed")
	}
	if _, err := os.Stat(auditPath); !os.IsNotExist(err) {
		t.Fatalf("terminal close duplicated routing audit: err=%v", err)
	}
	request, err := os.ReadFile(requestPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(request), `"status":"CLOSED"`) {
		t.Fatalf("terminal request is not CLOSED: %s", request)
	}
}

func TestCloseMappingDecisionsAcceptsMatchingFinalReadyOnce(t *testing.T) {
	dir := t.TempDir()
	requestPath := filepath.Join(dir, "mapping_decision_request.json")
	readyPath := filepath.Join(dir, "mapping_decision_ready.json")
	auditPath := filepath.Join(dir, "supervisor_routing_boundary_audit.jsonl")
	ready := `{"status":"READY","request_id":"mapping-request-d118-e119","decision_epoch":118,"effective_epoch":119,` +
		`"emitted_update_count":1,"mapping_authority":"supervisor_frozen_route"}`
	if err := os.WriteFile(readyPath, []byte(ready), 0o644); err != nil {
		t.Fatal(err)
	}
	manager := &RuntimeUnknownAdmissionManager{
		enabled:                         true,
		mappingBoundaryEnabled:          true,
		mappingDecisionMode:             "async_pipeline_lag1",
		mappingDecisionRequestPath:      requestPath,
		mappingDecisionReadyPath:        readyPath,
		mappingDecisionBarrierAuditPath: auditPath,
		lastMappingDecisionBarrierRound: 118,
		acceptedMappingDecisionRounds:   map[int]bool{},
	}

	manager.CloseMappingDecisions(119)

	if !manager.acceptedMappingDecisionRounds[119] {
		t.Fatal("matching final READY was not accepted")
	}
	audit, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(audit), "\n") != 1 ||
		!strings.Contains(string(audit), `"status":"ACCEPTED_PIPELINE_FINAL_BATCH"`) {
		t.Fatalf("unexpected final routing audit: %s", audit)
	}
}

func TestPublishMappingDecisionRequestCarriesIdentityAndAudit(t *testing.T) {
	dir := t.TempDir()
	requestPath := filepath.Join(dir, "mapping_decision_request.json")
	if !publishMappingDecisionRequest(requestPath, 3, 4) {
		t.Fatal("request publication failed")
	}
	request, err := os.ReadFile(requestPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(request), `"request_id":"mapping-request-d3-e4"`) {
		t.Fatalf("request identity missing: %s", request)
	}
	audit, err := os.ReadFile(filepath.Join(dir, "mapping_decision_request_audit.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(audit), "\n") != 1 || !strings.Contains(string(audit), `"decision_epoch":3`) {
		t.Fatalf("unexpected request audit: %s", audit)
	}
}
