package committee

// This file contains the frozen QPSA extensions used only by the legacy SAA
// policy. The unified soft-decay policy never calls these functions.

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type qpsaCandidateEvaluation struct {
	ShardID           uint64
	LocalityShare     float64
	AnchorShare       float64
	CrossShardRisk    float64
	QueueLength       float64
	ProjectedDemand   float64
	NormalizedQueue   float64
	NormalizedDemand  float64
	MarginalPotential float64
	Score             float64
}

type qpsaV2QueueState struct {
	Epoch             int     `json:"epoch"`
	ShardID           uint64  `json:"shard_id"`
	PendingTxCount    float64 `json:"pending_tx_count"`
	EWMAArrivalRate   float64 `json:"ewma_arrival_rate"`
	EWMAServiceRate   float64 `json:"ewma_service_rate"`
	ObservationStatus string  `json:"observation_status"`
}

type qpsaV2CandidateEvaluation struct {
	ShardID           uint64
	LocalityShare     float64
	AnchorShare       float64
	CrossShardRisk    float64
	QueueLength       float64
	ProjectedDemand   float64
	ArrivalRate       float64
	ServiceRate       float64
	PositiveDrift     float64
	MarginalPotential float64
	Score             float64
	StateEpoch        int
}

// applyQPSADecisionLocked evaluates the same locality-feasible candidates as
// legacy SAA. Shadow mode records a sequential counterfactual trajectory while
// preserving the legacy mapping; active mode applies the minimum-risk choice.
func (m *RuntimeUnknownAdmissionManager) applyQPSADecisionLocked(
	account string,
	logicalRound int,
	obs *runtimeUnknownObservation,
	legacyTarget uint64,
	legacyMode string,
	legacyReason string,
	legacyConfidence float64,
	allowOverride bool,
) (uint64, string, string, float64) {
	if (m.qpsaMode != "shadow" && m.qpsaMode != "active") || obs == nil || len(obs.ShardEvidence) == 0 {
		return legacyTarget, legacyMode, legacyReason, legacyConfidence
	}

	qpsaTarget, qpsaScore, evaluations := m.selectQPSATargetLocked(obs, logicalRound, legacyTarget)
	appliedTarget := legacyTarget
	appliedMode := legacyMode
	appliedReason := legacyReason
	appliedConfidence := legacyConfidence
	if m.qpsaMode == "active" && allowOverride {
		appliedTarget = qpsaTarget
		appliedMode = "qpsa_queue_potential_admission"
		appliedReason = "locality_constrained_minimum_marginal_queue_potential"
		appliedConfidence = qpsaScore
	}
	projectionTarget := qpsaTarget
	if m.qpsaMode == "active" {
		projectionTarget = appliedTarget
	}
	m.recordQPSAProjectionLocked(logicalRound, projectionTarget, obs)
	m.appendQPSAAuditLocked(
		logicalRound, account, legacyTarget, qpsaTarget, appliedTarget, allowOverride, evaluations,
	)
	m.evaluateQPSAV2ShadowLocked(
		logicalRound, account, obs, appliedTarget,
	)
	return appliedTarget, appliedMode, appliedReason, appliedConfidence
}

func (m *RuntimeUnknownAdmissionManager) selectQPSATargetLocked(
	obs *runtimeUnknownObservation,
	logicalRound int,
	legacyTarget uint64,
) (uint64, float64, []qpsaCandidateEvaluation) {
	evaluations := make([]qpsaCandidateEvaluation, 0, m.shardNum)
	for shardID := uint64(0); shardID < m.shardNum; shardID++ {
		evaluation := m.qpsaCandidateLocked(obs, shardID, logicalRound)
		evaluations = append(evaluations, evaluation)
	}
	legacyPotential := 1e18
	for _, evaluation := range evaluations {
		if evaluation.ShardID == legacyTarget {
			legacyPotential = evaluation.MarginalPotential
			break
		}
	}
	bestShard := legacyTarget
	bestScore := -1e18
	for _, evaluation := range evaluations {
		if evaluation.ShardID == legacyTarget {
			bestScore = evaluation.Score
			break
		}
	}
	requiredGain := m.qpsaMinRelativePotentialGain * maxFloat(legacyPotential, 1e-9)
	for _, evaluation := range evaluations {
		// QPSA may replace the established SAA choice only with an action that
		// does not increase the estimated marginal queue potential. This
		// monotone feasible set keeps locality optimization subordinate to the
		// queue-stability objective without introducing another tuned gate.
		if evaluation.ShardID == legacyTarget ||
			evaluation.MarginalPotential > legacyPotential-requiredGain-1e-12 {
			continue
		}
		if evaluation.Score > bestScore || (evaluation.Score == bestScore && evaluation.ShardID < bestShard) {
			bestScore = evaluation.Score
			bestShard = evaluation.ShardID
		}
	}
	return bestShard, bestScore, evaluations
}

func (m *RuntimeUnknownAdmissionManager) qpsaCandidateLocked(
	obs *runtimeUnknownObservation,
	shardID uint64,
	logicalRound int,
) qpsaCandidateEvaluation {
	localityShare := share(obs.ShardEvidence[shardID], obs.TotalTxCount)
	anchorShare := share(obs.StableShardEvidence[shardID], obs.TotalTxCount)
	crossShardRisk := 1.0 - localityShare
	queueLength := maxFloat(0.0, m.liveQueueByShard[shardID])
	projectedDemand := maxFloat(0.0, m.qpsaProjectedDemand[logicalRound][shardID])
	meanQueue := m.meanLiveQueueLocked()
	demand := minFloat(maxFloat(float64(obs.TotalTxCount), 1.0), m.qpsaDemandCap)
	normalizedQueue := (queueLength + projectedDemand) / meanQueue
	normalizedDemand := demand / meanQueue
	marginalPotential := normalizedQueue*normalizedDemand + 0.5*normalizedDemand*normalizedDemand
	score := m.localityWeight*localityShare +
		m.anchorWeight*anchorShare -
		m.crossWeight*crossShardRisk -
		m.qpsaQueueWeight*marginalPotential
	return qpsaCandidateEvaluation{
		ShardID:           shardID,
		LocalityShare:     localityShare,
		AnchorShare:       anchorShare,
		CrossShardRisk:    crossShardRisk,
		QueueLength:       queueLength,
		ProjectedDemand:   projectedDemand,
		NormalizedQueue:   normalizedQueue,
		NormalizedDemand:  normalizedDemand,
		MarginalPotential: marginalPotential,
		Score:             score,
	}
}

// evaluateQPSAV2ShadowLocked compares the executed QPSA-v1 choice with a
// drift-aware quadratic queue potential. The v2 path is audit-only until a
// validation run establishes that its queue-state contract is complete.
func (m *RuntimeUnknownAdmissionManager) evaluateQPSAV2ShadowLocked(
	logicalRound int,
	account string,
	obs *runtimeUnknownObservation,
	baseTarget uint64,
) {
	if m.qpsaV2Mode != "shadow" || obs == nil || len(obs.ShardEvidence) == 0 {
		return
	}
	m.refreshQPSAV2QueueStatesLocked(logicalRound)
	if len(m.qpsaV2StateByShard) != int(m.shardNum) {
		m.appendQPSAV2AuditLocked(logicalRound, account, baseTarget, baseTarget, nil, "INCOMPLETE_QUEUE_STATE")
		return
	}
	evaluations := make([]qpsaV2CandidateEvaluation, 0, m.shardNum)
	meanQueue := m.meanQPSAV2QueueLocked()
	demand := minFloat(maxFloat(float64(obs.TotalTxCount), 1.0), m.qpsaDemandCap)
	normalizedDemand := demand / meanQueue
	for shardID := uint64(0); shardID < m.shardNum; shardID++ {
		state := m.qpsaV2StateByShard[shardID]
		queueLength := maxFloat(0.0, state.PendingTxCount)
		projectedDemand := maxFloat(0.0, m.qpsaV2ProjectedDemand[logicalRound][shardID])
		normalizedQueue := (queueLength + projectedDemand) / meanQueue
		positiveDrift := 0.0
		if state.EWMAServiceRate > 1e-9 {
			positiveDrift = maxFloat(0.0, state.EWMAArrivalRate-state.EWMAServiceRate) / state.EWMAServiceRate
		} else if state.EWMAArrivalRate > 1e-9 {
			positiveDrift = 1.0
		}
		basePotential := normalizedQueue*normalizedDemand + 0.5*normalizedDemand*normalizedDemand
		marginalPotential := basePotential * (1.0 + m.qpsaV2DriftWeight*positiveDrift)
		localityShare := share(obs.ShardEvidence[shardID], obs.TotalTxCount)
		anchorShare := share(obs.StableShardEvidence[shardID], obs.TotalTxCount)
		crossRisk := 1.0 - localityShare
		score := m.localityWeight*localityShare +
			m.anchorWeight*anchorShare -
			m.crossWeight*crossRisk -
			m.qpsaQueueWeight*marginalPotential
		evaluations = append(evaluations, qpsaV2CandidateEvaluation{
			ShardID:           shardID,
			LocalityShare:     localityShare,
			AnchorShare:       anchorShare,
			CrossShardRisk:    crossRisk,
			QueueLength:       queueLength,
			ProjectedDemand:   projectedDemand,
			ArrivalRate:       maxFloat(0.0, state.EWMAArrivalRate),
			ServiceRate:       maxFloat(0.0, state.EWMAServiceRate),
			PositiveDrift:     positiveDrift,
			MarginalPotential: marginalPotential,
			Score:             score,
			StateEpoch:        state.Epoch,
		})
	}
	selected := baseTarget
	base := findQPSAV2Evaluation(evaluations, baseTarget)
	best := base
	if base != nil {
		requiredGain := m.qpsaMinRelativePotentialGain * maxFloat(base.MarginalPotential, 1e-9)
		for index := range evaluations {
			candidate := &evaluations[index]
			if candidate.ShardID == baseTarget ||
				candidate.MarginalPotential > base.MarginalPotential-requiredGain-1e-12 {
				continue
			}
			if best == nil || candidate.Score > best.Score ||
				(candidate.Score == best.Score && candidate.ShardID < best.ShardID) {
				best = candidate
				selected = candidate.ShardID
			}
		}
	}
	m.appendQPSAV2AuditLocked(logicalRound, account, baseTarget, selected, evaluations, "OK")
	m.recordQPSAV2ProjectionLocked(logicalRound, selected, demand)
}

func (m *RuntimeUnknownAdmissionManager) refreshQPSAV2QueueStatesLocked(logicalRound int) {
	if logicalRound == m.qpsaV2LastRefreshRound || m.qpsaV2QueueStatePath == "" {
		return
	}
	m.qpsaV2LastRefreshRound = logicalRound
	payload, err := os.ReadFile(m.qpsaV2QueueStatePath)
	if err != nil {
		return
	}
	states := make([]qpsaV2QueueState, 0, m.shardNum)
	if err := json.Unmarshal(payload, &states); err != nil {
		return
	}
	parsed := make(map[uint64]qpsaV2QueueState, len(states))
	for _, state := range states {
		if state.ShardID >= m.shardNum || state.ObservationStatus == "WARMUP_NO_PREVIOUS_COMMIT" {
			continue
		}
		parsed[state.ShardID] = state
	}
	if len(parsed) == int(m.shardNum) {
		m.qpsaV2StateByShard = parsed
	}
}

func (m *RuntimeUnknownAdmissionManager) meanQPSAV2QueueLocked() float64 {
	total := 0.0
	for shardID := uint64(0); shardID < m.shardNum; shardID++ {
		total += maxFloat(0.0, m.qpsaV2StateByShard[shardID].PendingTxCount)
	}
	return maxFloat(1.0, total/float64(maxInt(int(m.shardNum), 1)))
}

// recordQPSAV2ProjectionLocked maintains an independent counterfactual batch
// ledger for the shadow policy. Without this ledger, repeated admissions in
// one epoch would all observe the same cold target and could create a new hot
// shard before the next native queue snapshot becomes visible.
func (m *RuntimeUnknownAdmissionManager) recordQPSAV2ProjectionLocked(
	logicalRound int,
	targetShard uint64,
	demand float64,
) {
	if _, ok := m.qpsaV2ProjectedDemand[logicalRound]; !ok {
		m.qpsaV2ProjectedDemand[logicalRound] = make(map[uint64]float64)
	}
	m.qpsaV2ProjectedDemand[logicalRound][targetShard] += maxFloat(0.0, demand)
	for round := range m.qpsaV2ProjectedDemand {
		if round < logicalRound-1 {
			delete(m.qpsaV2ProjectedDemand, round)
		}
	}
}

func findQPSAV2Evaluation(
	evaluations []qpsaV2CandidateEvaluation,
	shardID uint64,
) *qpsaV2CandidateEvaluation {
	for index := range evaluations {
		if evaluations[index].ShardID == shardID {
			return &evaluations[index]
		}
	}
	return nil
}

func (m *RuntimeUnknownAdmissionManager) appendQPSAV2AuditLocked(
	logicalRound int,
	account string,
	baseTarget uint64,
	selectedTarget uint64,
	evaluations []qpsaV2CandidateEvaluation,
	status string,
) {
	if m.qpsaV2AuditPath == "" {
		return
	}
	base := findQPSAV2Evaluation(evaluations, baseTarget)
	selected := findQPSAV2Evaluation(evaluations, selectedTarget)
	row := []string{
		strconv.Itoa(logicalRound), account, status,
		strconv.FormatUint(baseTarget, 10), strconv.FormatUint(selectedTarget, 10),
		strconv.FormatBool(baseTarget != selectedTarget),
	}
	for _, evaluation := range []*qpsaV2CandidateEvaluation{base, selected} {
		if evaluation == nil {
			row = append(row, "", "", "", "", "", "", "")
			continue
		}
		row = append(row,
			strconv.Itoa(evaluation.StateEpoch),
			strconv.FormatFloat(evaluation.QueueLength, 'f', 4, 64),
			strconv.FormatFloat(evaluation.ProjectedDemand, 'f', 4, 64),
			strconv.FormatFloat(evaluation.ArrivalRate, 'f', 8, 64),
			strconv.FormatFloat(evaluation.ServiceRate, 'f', 8, 64),
			strconv.FormatFloat(evaluation.PositiveDrift, 'f', 8, 64),
			strconv.FormatFloat(evaluation.MarginalPotential, 'f', 8, 64),
		)
	}
	appendQPSAV2AuditCSV(m.qpsaV2AuditPath, row)
}

func (m *RuntimeUnknownAdmissionManager) meanLiveQueueLocked() float64 {
	if m.shardNum == 0 {
		return 1.0
	}
	total := 0.0
	for shardID := uint64(0); shardID < m.shardNum; shardID++ {
		total += maxFloat(0.0, m.liveQueueByShard[shardID])
	}
	return maxFloat(1.0, total/float64(m.shardNum))
}

func (m *RuntimeUnknownAdmissionManager) recordQPSAProjectionLocked(
	logicalRound int,
	targetShard uint64,
	obs *runtimeUnknownObservation,
) {
	if _, ok := m.qpsaProjectedDemand[logicalRound]; !ok {
		m.qpsaProjectedDemand[logicalRound] = make(map[uint64]float64)
	}
	demand := 1.0
	if obs != nil {
		demand = minFloat(maxFloat(float64(obs.TotalTxCount), 1.0), m.qpsaDemandCap)
	}
	m.qpsaProjectedDemand[logicalRound][targetShard] += demand
	for round := range m.qpsaProjectedDemand {
		if round < logicalRound-1 {
			delete(m.qpsaProjectedDemand, round)
		}
	}
}

func (m *RuntimeUnknownAdmissionManager) appendQPSAAuditLocked(
	logicalRound int,
	account string,
	legacyTarget uint64,
	qpsaTarget uint64,
	appliedTarget uint64,
	allowOverride bool,
	evaluations []qpsaCandidateEvaluation,
) {
	if m.qpsaAuditPath == "" {
		return
	}
	byShard := make(map[uint64]qpsaCandidateEvaluation, len(evaluations))
	for _, evaluation := range evaluations {
		byShard[evaluation.ShardID] = evaluation
	}
	legacy := byShard[legacyTarget]
	qpsa := byShard[qpsaTarget]
	relativePotentialGain := 0.0
	if legacy.MarginalPotential > 1e-12 {
		relativePotentialGain = (legacy.MarginalPotential - qpsa.MarginalPotential) / legacy.MarginalPotential
	}
	appendQPSAAuditCSV(m.qpsaAuditPath, []string{
		strconv.Itoa(logicalRound), account, m.qpsaMode,
		strconv.Itoa(len(evaluations)),
		strconv.FormatUint(legacyTarget, 10),
		strconv.FormatUint(qpsaTarget, 10),
		strconv.FormatUint(appliedTarget, 10),
		strconv.FormatBool(legacyTarget != qpsaTarget),
		strconv.FormatBool(allowOverride),
		strconv.FormatFloat(relativePotentialGain, 'f', 8, 64),
		strconv.FormatFloat(legacy.LocalityShare, 'f', 8, 64),
		strconv.FormatFloat(qpsa.LocalityShare, 'f', 8, 64),
		strconv.FormatFloat(legacy.QueueLength, 'f', 4, 64),
		strconv.FormatFloat(qpsa.QueueLength, 'f', 4, 64),
		strconv.FormatFloat(legacy.ProjectedDemand, 'f', 4, 64),
		strconv.FormatFloat(qpsa.ProjectedDemand, 'f', 4, 64),
		strconv.FormatFloat(legacy.MarginalPotential, 'f', 8, 64),
		strconv.FormatFloat(qpsa.MarginalPotential, 'f', 8, 64),
		strconv.FormatFloat(legacy.Score, 'f', 8, 64),
		strconv.FormatFloat(qpsa.Score, 'f', 8, 64),
	})
}

func appendQPSAAuditCSV(path string, row []string) {
	if strings.TrimSpace(path) == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer handle.Close()
	stat, err := handle.Stat()
	if err != nil {
		return
	}
	writer := csv.NewWriter(handle)
	if stat.Size() == 0 {
		_ = writer.Write([]string{
			"epoch", "account", "qpsa_mode", "candidate_count",
			"legacy_target", "qpsa_target", "applied_target", "shadow_disagreement",
			"override_allowed", "relative_potential_gain",
			"legacy_locality_share", "qpsa_locality_share",
			"legacy_queue_length", "qpsa_queue_length",
			"legacy_projected_demand", "qpsa_projected_demand",
			"legacy_marginal_queue_potential", "qpsa_marginal_queue_potential",
			"legacy_qpsa_score", "qpsa_score",
		})
	}
	_ = writer.Write(row)
	writer.Flush()
}

func appendQPSAV2AuditCSV(path string, row []string) {
	if strings.TrimSpace(path) == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer handle.Close()
	stat, err := handle.Stat()
	if err != nil {
		return
	}
	writer := csv.NewWriter(handle)
	if stat.Size() == 0 {
		_ = writer.Write([]string{
			"epoch", "account", "status", "qpsa_v1_target", "qpsa_v2_target", "shadow_disagreement",
			"v1_state_epoch", "v1_pending", "v1_projected_demand", "v1_arrival_rate", "v1_service_rate", "v1_positive_drift", "v1_marginal_potential",
			"v2_state_epoch", "v2_pending", "v2_projected_demand", "v2_arrival_rate", "v2_service_rate", "v2_positive_drift", "v2_marginal_potential",
		})
	}
	_ = writer.Write(row)
	writer.Flush()
}
