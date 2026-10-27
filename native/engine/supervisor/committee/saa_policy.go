package committee

import (
	"strconv"
	"strings"
)

type saaSoftDecision struct {
	Account            string               `json:"account"`
	LogicalRound       int                  `json:"logical_round"`
	CurrentShard       uint64               `json:"current_shard"`
	SelectedShard      uint64               `json:"selected_shard"`
	BestShard          uint64               `json:"best_shard"`
	CurrentScore       float64              `json:"current_score"`
	SelectedScore      float64              `json:"selected_score"`
	BestScore          float64              `json:"best_score"`
	ScoreDelta         float64              `json:"score_delta"`
	MigrationIntensity float64              `json:"migration_intensity"`
	Action             string               `json:"action"`
	Reason             string               `json:"reason"`
	LoadSignalSource   string               `json:"load_signal_source"`
	Candidates         []saaSoftTargetScore `json:"candidates"`
}

func (m *RuntimeUnknownAdmissionManager) decideSAASoftTargetLocked(account string, obs *runtimeUnknownObservation, logicalRound int, currentShard uint64, initial bool) saaSoftDecision {
	state := m.updateSAASoftAccountStateLocked(account, obs, logicalRound)
	m.updateSAASoftLoadStateLocked(logicalRound)
	scores := m.scoreSAASoftTargetsLocked(state, currentShard)
	decision := saaSoftDecision{Account: account, LogicalRound: logicalRound, CurrentShard: currentShard, SelectedShard: currentShard, Action: "retain", Reason: "soft_decay_score_retains_current", Candidates: scores}
	if obs == nil || obs.TotalTxCount <= 0 || len(obs.ShardEvidence) == 0 {
		decision.Action = "observe"
		decision.Reason = "soft_decay_waiting_for_observed_peer_evidence"
		m.appendSAASoftDecisionAuditLocked(decision)
		return decision
	}
	best := saaSoftTargetScore{ShardID: currentShard, Score: -1e18}
	current := saaSoftTargetScore{ShardID: currentShard, Score: -1e18}
	for _, candidate := range scores {
		if candidate.ShardID == currentShard {
			current = candidate
		}
		if candidate.Score > best.Score || (candidate.Score == best.Score && candidate.ShardID < best.ShardID) {
			best = candidate
		}
	}
	decision.CurrentScore = current.Score
	decision.SelectedScore = current.Score
	decision.BestShard = best.ShardID
	decision.BestScore = best.Score
	decision.ScoreDelta = best.Score - current.Score
	decision.MigrationIntensity = saaMigrationIntensity(decision.ScoreDelta, m.softDecay)
	if best.ShardID != currentShard && decision.MigrationIntensity > 0.5 {
		decision.SelectedShard = best.ShardID
		decision.SelectedScore = best.Score
		decision.Action = "migrate"
		if initial {
			decision.Action = "assign"
		}
		decision.Reason = "soft_decay_positive_utility_gain"
	} else if initial {
		decision.Action = "assign"
		decision.Reason = "soft_decay_initial_target"
	}
	for _, load := range m.saaSoftLoadByShard {
		if load.SignalSource != "" {
			decision.LoadSignalSource = load.SignalSource
			break
		}
	}
	m.appendSAASoftDecisionAuditLocked(decision)
	return decision
}

func (m *RuntimeUnknownAdmissionManager) scheduleSAASoftRemapLocked(account string, logicalRound int, currentShard uint64, decision saaSoftDecision, obs *runtimeUnknownObservation) {
	if decision.Action != "migrate" || decision.SelectedShard == currentShard {
		return
	}
	if _, pending := m.pendingRuntimeShard[account]; pending {
		return
	}
	anchorCount := 0
	anchorShare := 0.0
	if obs != nil {
		anchorCount = obs.StableShardEvidence[decision.SelectedShard]
		anchorShare = share(anchorCount, obs.TotalTxCount)
	}
	update := runtimeUnknownAdmissionUpdate{
		ProposalID: "saa_soft_remap:" + account + ":" + strconv.Itoa(logicalRound),
		EpochID:    logicalRound, EffectiveEpoch: logicalRound + 1,
		ObjectType: "runtime_unknown_account", ObjectID: account,
		SourceShard: currentShard, TargetShard: decision.SelectedShard,
		Addresses: []string{account}, ResolvedAccountCount: 1,
		ApplyMode:           "runtime_unknown_soft_decay_remap",
		PlacementMode:       "soft_decay_unified_policy",
		PlacementReason:     decision.Reason,
		PlacementConfidence: decision.MigrationIntensity,
		ObservedTxCount:     obs.TotalTxCount, AnchorTxCount: anchorCount, AnchorScoreShare: anchorShare,
		ControlChannel: "saa_soft_decay", ProposalKind: "continuous_policy_remap",
	}
	m.pendingRuntimeShard[account] = decision.SelectedShard
	m.pendingRuntimeRouteRound[account] = logicalRound + 1
	m.pendingRuntimeUpdate[account] = update
	if m.shardAdmissionCounts[currentShard] > 0 {
		m.shardAdmissionCounts[currentShard]--
	}
	m.shardAdmissionCounts[decision.SelectedShard]++
	state := m.saaSoftStateByAccount[account]
	state.recordSwitch(m.softDecay.PersistenceTau)
	state.LastTarget = decision.SelectedShard
	state.HasTarget = true
	mustAppendAdmissionUpdateJSONL(m.mappingUpdatePath, update)
}

func (m *RuntimeUnknownAdmissionManager) appendSAASoftDecisionAuditLocked(decision saaSoftDecision) {
	path := strings.TrimSpace(m.saaSoftAuditPath)
	if path == "" {
		return
	}
	if decision.Action == "retain" || decision.Action == "observe" {
		if m.saaSoftAuditLastRound[decision.Account] == decision.LogicalRound {
			return
		}
	}
	m.saaSoftAuditLastRound[decision.Account] = decision.LogicalRound
	if !m.saaSoftAuditFull {
		decision.Candidates = nil
	}
	if err := appendJSONLRecord(path, decision); err != nil {
		panic("append SAA soft decision audit: " + err.Error())
	}
}
