package committee

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"blockEmulator/utils"
)

const saaIntegratedDecisionMode = "integrated_v1"
const forecastControllerMode = "forecast"
const integratedControllerEnv = "SAA_INTEGRATED_CONTROLLER"
const nativeInstallationReceiptGateEnv = "SAA_REQUIRE_NATIVE_INSTALLATION_RECEIPT"

type saaIntegratedRuntimeState struct {
	forecastObservedSequence uint64
	forecastStreamPath       string
	forecastStreamID         string
	forecastAnchorPath       string
	forecastCheckpointHash   string
	forecastAnchorOffset     int64
	forecastContextPath      string
	forecastContextVersion   uint64
	forecastContextCount     uint64
	forecastCommits          map[uint64]forecastCommitObservation
	forecastService          map[uint64]forecastServiceObservation
	forecastStatePayloads    map[uint64]map[string]forecastStatePayloadObservation
	forecastDecisionPath     string
	forecastDecisionOffset   int64
	nativeReceiptPath        string
	nativeSourcePreparedPath string
	nativeReceiptOffset      int64
	controllerMode           string
	modelAnchor              *forecastAnchorState
	forecastBridge           *forecastAuthorityBridge
	forecastAnchorVersion    uint64
	authority                *mappingAuthority
	config                   saaIntegratedConfig
	lastDecisionEpoch        map[string]int
	stableInitializationHash string
}

var saaIntegratedRuntimeStates sync.Map

func (m *RuntimeUnknownAdmissionManager) integratedStateLocked() *saaIntegratedRuntimeState {
	if m == nil {
		return nil
	}
	if state, ok := saaIntegratedRuntimeStates.Load(m); ok {
		return state.(*saaIntegratedRuntimeState)
	}
	initial := make(map[string]uint64, len(m.stableShardByAccount))
	for account, shardID := range m.stableShardByAccount {
		initial[account] = shardID
	}
	state := &saaIntegratedRuntimeState{
		authority:         newMappingAuthority(m.shardNum, initial),
		config:            defaultSAAIntegratedConfig(),
		lastDecisionEpoch: make(map[string]int),
	}
	state.controllerMode = strings.TrimSpace(os.Getenv(integratedControllerEnv))
	if state.controllerMode == "" {
		state.controllerMode = "legacy"
	}
	if state.controllerMode != "legacy" && state.controllerMode != forecastControllerMode {
		panic("invalid SAA_INTEGRATED_CONTROLLER: expected legacy or forecast")
	}
	if state.controllerMode == forecastControllerMode {
		if strings.EqualFold(strings.TrimSpace(os.Getenv(nativeInstallationReceiptGateEnv)), "1") || strings.EqualFold(strings.TrimSpace(os.Getenv(nativeInstallationReceiptGateEnv)), "true") {
			state.authority.enableInstallationReceiptGate()
			state.nativeReceiptPath = strings.TrimSpace(os.Getenv("SAA_NATIVE_INSTALLATION_RECEIPT_PATH"))
			state.nativeSourcePreparedPath = strings.TrimSpace(os.Getenv("SAA_NATIVE_SOURCE_PREPARED_PATH"))
		}
		state.forecastStreamPath = strings.TrimSpace(os.Getenv("SAA_FORECAST_STREAM_PATH"))
		state.forecastStreamID = strings.TrimSpace(os.Getenv("SAA_FORECAST_STREAM_ID"))
		state.forecastAnchorPath = strings.TrimSpace(os.Getenv("SAA_FORECAST_ANCHOR_PATH"))
		state.forecastCheckpointHash = strings.TrimSpace(os.Getenv("SAA_FORECAST_CHECKPOINT_SHA256"))
		state.forecastContextPath = strings.TrimSpace(os.Getenv("SAA_FORECAST_CONTEXT_PATH"))
		state.forecastDecisionPath = strings.TrimSpace(os.Getenv("SAA_FORECAST_DECISION_PATH"))
	}
	state.stableInitializationHash = state.authority.stateDigest()
	state.forecastBridge = newForecastAuthorityBridge(state.authority)
	actual, _ := saaIntegratedRuntimeStates.LoadOrStore(m, state)
	return actual.(*saaIntegratedRuntimeState)
}

// submitForecastLocked is the controller ingress into the existing authority.
// forecastAnchorVersion remains zero until a model Anchor state is installed;
// this prevents interpreting static initialization as the supplied model state.
// Caller must hold the RuntimeUnknownAdmissionManager lock.
func (m *RuntimeUnknownAdmissionManager) submitForecastLocked(envelope forecastAuthorityEnvelope, logicalRound int, maxQueueAge int) mappingAuthorityDecision {
	state := m.integratedStateLocked()
	if state == nil || m.softDecisionMode != saaIntegratedDecisionMode {
		return mappingAuthorityDecision{ProposalID: envelope.DecisionID, Status: mappingRejected, Reason: "integrated_runtime_unavailable"}
	}
	var decision mappingAuthorityDecision
	_, duplicate := state.forecastBridge.seen[envelope.DecisionID]
	_, sourceKnown := m.liveQueueByShard[envelope.SourceShard]
	_, targetKnown := m.liveQueueByShard[envelope.TargetShard]
	if state.controllerMode != forecastControllerMode {
		decision = mappingAuthorityDecision{ProposalID: envelope.DecisionID, Status: mappingRejected, Reason: "forecast_controller_not_selected"}
	} else if !duplicate && !envelope.Keep && (!sourceKnown || !targetKnown || m.pressureHintEpoch <= 0 || envelope.QueueEpoch != m.pressureHintEpoch) {
		decision = mappingAuthorityDecision{ProposalID: envelope.DecisionID, Status: mappingRejected, Reason: "native_queue_evidence_missing_or_mismatched"}
	} else {
		decision = state.forecastBridge.submit(envelope, logicalRound, state.forecastAnchorVersion, maxQueueAge)
	}
	m.appendSAAIntegratedAuditLocked(map[string]interface{}{
		"record_type": "forecast_authority_receipt", "logical_round": logicalRound,
		"controller_envelope": envelope, "authority_decision": decision,
		"anchor_version":     state.forecastAnchorVersion,
		"native_queue_epoch": m.pressureHintEpoch,
	})
	return decision
}

func (m *RuntimeUnknownAdmissionManager) applySAAIntegratedDueLocked(logicalRound int) {
	if m == nil || m.softDecisionMode != saaIntegratedDecisionMode {
		return
	}
	state := m.integratedStateLocked()
	if err := m.consumeForecastAnchorsLocked(); err != nil {
		panic(fmt.Sprintf("forecast Anchor ingress failed: %v", err))
	}
	if state.authority.requireInstallationReceipt {
		if err := m.consumeNativeSourcePreparedLocked(state.nativeSourcePreparedPath); err != nil {
			panic(fmt.Sprintf("native source preparation ingress failed: %v", err))
		}
	}
	if state.authority.requireInstallationReceipt && state.nativeReceiptPath != "" {
		if _, err := m.consumeNativeInstallationReceiptsLocked(state.nativeReceiptPath, &state.nativeReceiptOffset); err != nil {
			panic(fmt.Sprintf("native installation receipt ingress failed: %v", err))
		}
	}
	decisions := state.authority.applyDue(logicalRound)
	m.routeVersion = state.authority.routeVersion
	if err := m.publishDueTargetActivationLocked(); err != nil {
		panic(fmt.Sprintf("target activation publication failed: %v", err))
	}
	if err := m.consumeForecastDecisionsLocked(logicalRound); err != nil {
		panic(fmt.Sprintf("forecast decision ingress failed: %v", err))
	}
	if state.authority.requireInstallationReceipt {
		if err := m.publishSourceCandidateIfEnabledLocked(); err != nil {
			panic(fmt.Sprintf("source candidate publication failed: %v", err))
		}
	}
	if err := m.exportForecastContextLocked(logicalRound); err != nil {
		panic(fmt.Sprintf("forecast context export failed: %v", err))
	}
	if len(decisions) == 0 {
		return
	}
	stateDigest := state.authority.stateDigest()
	for _, decision := range decisions {
		m.appendSAAIntegratedAuditLocked(map[string]interface{}{
			"record_type":                "mapping_authority_decision",
			"activation_scope":           "supervisor_route_map_only",
			"native_handoff_status":      "not_connected_to_this_authority",
			"native_effect_delay_status": "missing",
			"native_effect_delay":        nil,
			"handoff_bytes":              nil,
			"logical_round":              logicalRound,
			"decision":                   decision,
			"route_version":              state.authority.routeVersion,
			"state_digest":               stateDigest,
		})
	}
}

func (m *RuntimeUnknownAdmissionManager) lookupSAAIntegratedLocked(account string) (uint64, bool) {
	state := m.integratedStateLocked()
	if state == nil {
		return 0, false
	}
	return state.authority.resolve(account)
}

func (m *RuntimeUnknownAdmissionManager) isSAAIntegratedObservedLocked(account string) bool {
	state := m.integratedStateLocked()
	if state == nil {
		return false
	}
	account = normalizeMappingAuthorityAccount(account)
	if account == "" {
		return false
	}
	if _, ok := state.authority.active[account]; ok {
		_, stable := m.stableShardByAccount[account]
		return !stable
	}
	for _, proposal := range state.authority.pending {
		for _, pendingAccount := range proposal.Addresses {
			if pendingAccount == account {
				return true
			}
		}
	}
	return false
}

func (m *RuntimeUnknownAdmissionManager) decideSAAIntegratedLocked(account string, obs *runtimeUnknownObservation, logicalRound int, initial bool) {
	state := m.integratedStateLocked()
	if state == nil || state.controllerMode == forecastControllerMode || state.lastDecisionEpoch[account] == logicalRound {
		return
	}
	state.lastDecisionEpoch[account] = logicalRound
	currentShard, known := state.authority.resolve(account)
	if !known {
		currentShard = uint64(utils.Addr2Shard(account))
	}
	inputs := m.buildSAAIntegratedInputsLocked(obs, logicalRound)
	decision := decideSAAIntegrated(currentShard, inputs, state.config)
	selected := decision.SelectedShard
	if initial && decision.Action != "migrate" {
		selected = currentShard
	}
	if !initial && decision.Action != "migrate" {
		m.appendSAAIntegratedPolicyAuditLocked(account, logicalRound, initial, decision, nil)
		return
	}
	proposal := mappingAuthorityProposal{
		ProposalID: fmt.Sprintf("saa-integrated:%s:%d", account, logicalRound),
		Source:     "saa_integrated_policy", Addresses: []string{account},
		SourceShard: currentShard, TargetShard: selected,
		BaseRouteVersion: state.authority.routeVersion,
		EffectiveEpoch:   logicalRound + 1,
	}
	authorityDecision := state.authority.submit(proposal, logicalRound)
	m.appendSAAIntegratedPolicyAuditLocked(account, logicalRound, initial, decision, &authorityDecision)
}

func (m *RuntimeUnknownAdmissionManager) buildSAAIntegratedInputsLocked(obs *runtimeUnknownObservation, logicalRound int) []saaIntegratedTargetInput {
	total := 0
	if obs != nil {
		total = obs.TotalTxCount
	}
	inputs := make([]saaIntegratedTargetInput, 0, m.shardNum)
	for shardID := uint64(0); shardID < m.shardNum; shardID++ {
		queueLength, queueKnown := m.liveQueueByShard[shardID]
		signalAge := maxInt(logicalRound-m.pressureHintEpoch, 0)
		queueKnown = queueKnown && m.pressureHintEpoch > 0
		anchorCount, localityCount := 0, 0
		if obs != nil {
			anchorCount = obs.StableShardEvidence[shardID]
			localityCount = obs.ShardEvidence[shardID]
		}
		anchor := m.explainSAAIntegratedAnchorLocked(obs, shardID)
		projected := 0.0
		if epochCounts := m.recentRoutePressure[logicalRound]; epochCounts != nil {
			projected = float64(epochCounts[shardID])
		}
		inputs = append(inputs, saaIntegratedTargetInput{
			ShardID:                 shardID,
			AnchorAffinity:          share(anchorCount, total),
			RecentLocality:          share(localityCount, total),
			ProjectedIncomingWork:   projected,
			ProjectedCrossShardWork: 0,
			PredictedEffectEpochs:   1,
			Queue: saaQueueSnapshot{
				QueueLength:      queueLength,
				EstimatedService: 0,
				SnapshotEpoch:    logicalRound,
				SignalAge:        signalAge,
				SignalMissing:    !queueKnown,
			},
			Anchor: anchor,
		})
	}
	return inputs
}

func (m *RuntimeUnknownAdmissionManager) explainSAAIntegratedAnchorLocked(obs *runtimeUnknownObservation, shardID uint64) saaAnchorExplanation {
	explanation := saaAnchorExplanation{
		AnchorID:                 fmt.Sprintf("stable-shard:%d", shardID),
		AnchorVersion:            1,
		TargetShardDistribution:  make(map[uint64]float64),
		StructureStatus:          "frozen_initialization",
		InitializationSourceHash: m.integratedStateLocked().stableInitializationHash,
	}
	if clusterID, ok := m.dominantStableClusterForShardLocked(obs, shardID); ok {
		explanation.AnchorID = fmt.Sprintf("stable:%d", clusterID)
	}
	if obs == nil || obs.TotalTxCount <= 0 {
		return explanation
	}
	explanation.TotalInteractionCount = obs.TotalTxCount
	explanation.AnchorInteractionCount = obs.StableShardEvidence[shardID]
	explanation.AnchorCoverage = share(explanation.AnchorInteractionCount, obs.TotalTxCount)
	resolved := 0
	shards := make([]uint64, 0, len(obs.StableShardEvidence))
	for candidate := range obs.StableShardEvidence {
		shards = append(shards, candidate)
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	for _, candidate := range shards {
		count := obs.StableShardEvidence[candidate]
		resolved += count
		explanation.TargetShardDistribution[candidate] = share(count, obs.TotalTxCount)
	}
	explanation.UnresolvedInteractionMass = share(obs.TotalTxCount-resolved, obs.TotalTxCount)
	explanation.EvidenceSequenceStart = uint64(maxInt(obs.FirstObservedRound, 0))
	explanation.EvidenceSequenceEnd = uint64(maxInt(obs.LastObservedRound, 0))
	return explanation
}

func (m *RuntimeUnknownAdmissionManager) appendSAAIntegratedPolicyAuditLocked(account string, logicalRound int, initial bool, decision saaIntegratedDecision, authorityDecision *mappingAuthorityDecision) {
	record := map[string]interface{}{
		"record_type":       "saa_integrated_policy_decision",
		"account":           account,
		"logical_round":     logicalRound,
		"initial_admission": initial,
		"decision_summary": map[string]interface{}{
			"current_shard":  decision.CurrentShard,
			"selected_shard": decision.SelectedShard,
			"action":         decision.Action,
			"reason":         decision.Reason,
			"keep_utility":   decision.KeepUtility,
			"best_utility":   decision.BestUtility,
			"net_gain":       decision.NetGain,
		},
	}
	if m.saaSoftAuditFull {
		record["policy_decision"] = decision
	} else {
		for _, candidate := range decision.Candidates {
			if candidate.ShardID == decision.SelectedShard {
				record["selected_candidate"] = candidate
				break
			}
		}
	}
	if authorityDecision != nil {
		record["authority_decision"] = authorityDecision
	}
	m.appendSAAIntegratedAuditLocked(record)
}

func (m *RuntimeUnknownAdmissionManager) appendSAAIntegratedAuditLocked(record map[string]interface{}) {
	path := m.saaSoftAuditPath
	if path == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	record["recorded_at_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
	record["mapping_authority"] = "supervisor_single_writer"
	payload, err := json.Marshal(record)
	if err != nil {
		return
	}
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer handle.Close()
	_, _ = handle.Write(append(payload, '\n'))
}
