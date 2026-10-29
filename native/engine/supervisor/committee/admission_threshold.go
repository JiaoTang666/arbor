package committee

import (
	"blockEmulator/utils"
	"strconv"
)

// This file contains the frozen threshold-based SAA policy. It is retained as
// an experiment baseline and is never called by soft_decay mode.
func (m *RuntimeUnknownAdmissionManager) maybeAssignLegacyLocked(accountAddr string, obs *runtimeUnknownObservation, logicalRound int) {
	if obs == nil || len(obs.ShardEvidence) == 0 {
		return
	}
	topShard, topCount, topShare := topShardEvidence(obs.ShardEvidence, obs.TotalTxCount)
	stableTopCount := obs.StableShardEvidence[topShard]
	stableTopShare := share(stableTopCount, obs.TotalTxCount)
	if stableTopCount >= m.minAnchorTx && stableTopShare >= m.minScoreShare {
		legacyTarget := topShard
		legacyMode := "runtime_strong_stable_anchor"
		legacyReason := "stable_anchor_confident"
		if m.shouldDivertOverflowLocked(topShard, stableTopCount, stableTopShare) {
			target := m.chooseLocalityPreservingDiversionShardLocked(accountAddr, obs, topShard)
			if target != topShard {
				legacyTarget, legacyMode, legacyReason = target, "stage1_hot_anchor_diversion", "new_account_anchor_points_to_hot_shard"
			}
		}
		target, mode, reason, confidence := m.applyQPSADecisionLocked(accountAddr, logicalRound, obs, legacyTarget, legacyMode, legacyReason, stableTopShare, false)
		m.assignLocked(accountAddr, target, logicalRound, mode, reason, confidence, obs)
		return
	}
	if topCount >= m.minAnchorTx && topShare >= maxFloat(0.45, m.minScoreShare*0.75) {
		legacyTarget, bestScore := topShard, -1e18
		for shardID := range obs.ShardEvidence {
			score := m.localityScoreLocked(obs, shardID)
			if score > bestScore || (score == bestScore && shardID < legacyTarget) {
				bestScore, legacyTarget = score, shardID
			}
		}
		legacyMode, legacyReason := "runtime_candidate_locality_first", "locality_first_candidate"
		if m.shouldDivertOverflowLocked(legacyTarget, topCount, topShare) {
			target := m.chooseLocalityPreservingDiversionShardLocked(accountAddr, obs, legacyTarget)
			if target != legacyTarget {
				legacyTarget, legacyMode, legacyReason = target, "stage1_hot_locality_diversion", "new_account_locality_points_to_hot_shard"
			}
		}
		target, mode, reason, confidence := m.applyQPSADecisionLocked(accountAddr, logicalRound, obs, legacyTarget, legacyMode, legacyReason, bestScore, true)
		m.assignLocked(accountAddr, target, logicalRound, mode, reason, confidence, obs)
		return
	}
	if m.globalFallbackEnabled && obs.TotalTxCount >= m.minAnchorTx {
		target := m.chooseLoadFallbackShardLocked(accountAddr)
		m.assignLocked(accountAddr, target, logicalRound, "runtime_global_load_fallback", "locality_evidence_insufficient", topShare, obs)
	}
}

func (m *RuntimeUnknownAdmissionManager) maybeEmitLegacyObservationRefreshLocked(account string, logicalRound int, obs *runtimeUnknownObservation) {
	if obs.TotalTxCount < m.minAnchorTx {
		return
	}
	if m.refreshMaxPerEpoch <= 0 || m.runtimeRefreshCountRound[logicalRound] >= m.refreshMaxPerEpoch {
		return
	}
	if lastRound, ok := m.lastRuntimeRefreshRound[account]; ok && lastRound >= logicalRound {
		return
	}
	currentShard, ok := m.pendingRuntimeShard[account]
	if !ok {
		currentShard, ok = m.runtimeShardByAccount[account]
	}
	if !ok {
		return
	}
	targetShard := currentShard
	if m.anchorRealignmentMode == "stage3_proposal" {
		targetShard = m.chooseAnchorRealignmentTargetLocked(obs, currentShard)
		if targetShard == currentShard {
			return
		}
	}
	anchorTxCount := obs.StableShardEvidence[targetShard]
	anchorScoreShare := share(anchorTxCount, obs.TotalTxCount)
	mode, reason := "runtime_candidate_locality_first", "runtime_observation_refresh"
	confidence := m.localityScoreLocked(obs, targetShard)
	if anchorTxCount >= m.minAnchorTx && anchorScoreShare >= m.minScoreShare {
		mode, reason, confidence = "runtime_strong_stable_anchor", "runtime_stable_anchor_refresh", anchorScoreShare
	}
	parentStableObjectID := ""
	var stableClusterIDPtr *int
	if clusterID, hasCluster := m.dominantStableClusterForShardLocked(obs, targetShard); hasCluster {
		clusterIDCopy := clusterID
		stableClusterIDPtr = &clusterIDCopy
		parentStableObjectID = "stable:" + strconv.Itoa(clusterID)
	}
	update := runtimeUnknownAdmissionUpdate{
		ProposalID: "runtime_unknown_refresh:" + account + ":" + strconv.Itoa(logicalRound),
		EpochID:    logicalRound, EffectiveEpoch: logicalRound + 1,
		ObjectType: "runtime_unknown_account", ObjectID: account,
		SourceShard: currentShard, TargetShard: targetShard,
		Addresses: []string{account}, ResolvedAccountCount: 1,
		ApplyMode: "runtime_unknown_observation_refresh", PlacementMode: mode,
		PlacementReason: reason, PlacementConfidence: confidence,
		ObservedTxCount: obs.TotalTxCount, AnchorTxCount: anchorTxCount, AnchorScoreShare: anchorScoreShare,
		ParentStableObjectID: parentStableObjectID, ProjectionClusterID: stableClusterIDPtr,
		AnchorClusterID: stableClusterIDPtr, StableClusterID: stableClusterIDPtr,
		ControlChannel: "anchor_independent", ProposalKind: "anchor_realignment",
	}
	if m.anchorRealignmentMode == "stage3_proposal" && m.anchorProposalPath != "" {
		update.ApplyMode = "runtime_anchor_realignment_proposal"
		update.ControlChannel = "stage3_anchor_proposal"
		mustAppendAdmissionUpdateJSONL(m.anchorProposalPath, update)
	} else {
		mustAppendAdmissionUpdateJSONL(m.mappingUpdatePath, update)
	}
	m.lastRuntimeRefreshRound[account] = logicalRound
	m.runtimeRefreshCountRound[logicalRound]++
}

func (m *RuntimeUnknownAdmissionManager) localityScoreLocked(obs *runtimeUnknownObservation, shardID uint64) float64 {
	total := obs.TotalTxCount
	if total <= 0 {
		return -1.0
	}
	localityScore := share(obs.ShardEvidence[shardID], total)
	anchorConfidence := share(obs.StableShardEvidence[shardID], total)
	crossShardRisk := 1.0 - localityScore
	loadPenalty := m.normalizedLoadPenaltyLocked(shardID)
	return (m.localityWeight * localityScore) +
		(m.anchorWeight * anchorConfidence) -
		(m.crossWeight * crossShardRisk) -
		(m.loadWeight * loadPenalty)
}

func (m *RuntimeUnknownAdmissionManager) normalizedLoadPenaltyLocked(shardID uint64) float64 {
	if activePressure, _, _ := m.activePressureByShardLocked(); len(activePressure) > 0 {
		maxPressure := 1e-9
		for _, pressure := range activePressure {
			if pressure > maxPressure {
				maxPressure = pressure
			}
		}
		return activePressure[shardID] / maxPressure
	}
	maxLoad := 1
	for _, count := range m.shardAdmissionCounts {
		if count > maxLoad {
			maxLoad = count
		}
	}
	return float64(m.shardAdmissionCounts[shardID]) / float64(maxLoad)
}

func (m *RuntimeUnknownAdmissionManager) shouldDivertOverflowLocked(shardID uint64, anchorTxCount int, anchorShare float64) bool {
	if !m.overflowDiversionEnabled || m.shardNum == 0 {
		return false
	}
	// Do not split highly certain locality: a frequently repeated, very
	// concentrated stable anchor is more expensive to place remotely than to
	// retain. The diversion path targets weak-to-medium incoming attachment.
	if anchorTxCount >= maxInt(2, m.minAnchorTx+1) && anchorShare >= m.strongAnchorProtectShare {
		return false
	}
	if activePressure, _, _ := m.activePressureByShardLocked(); len(activePressure) > 0 {
		totalPressure := 0.0
		minPressure := 1e18
		for shardIDIter := uint64(0); shardIDIter < m.shardNum; shardIDIter++ {
			pressure := activePressure[shardIDIter]
			totalPressure += pressure
			if pressure < minPressure {
				minPressure = pressure
			}
		}
		avgPressure := totalPressure / float64(m.shardNum)
		targetPressure := activePressure[shardID]
		if avgPressure > 0.0 &&
			targetPressure > avgPressure*m.overflowRatio &&
			targetPressure-minPressure > 0.20 {
			return true
		}
	}
	// Weak locality evidence can form a hotspot before execution-pressure
	// feedback is visible. Retain an admission-volume structural brake rather
	// than requiring the hotspot to become hot first.
	totalAdmissions := 0
	minAdmissions := int(^uint(0) >> 1)
	for shardIDIter := uint64(0); shardIDIter < m.shardNum; shardIDIter++ {
		count := m.shardAdmissionCounts[shardIDIter]
		totalAdmissions += count
		if count < minAdmissions {
			minAdmissions = count
		}
	}
	if totalAdmissions < m.overflowMinAdmissions {
		return false
	}
	avgAdmissions := float64(totalAdmissions) / float64(m.shardNum)
	targetAdmissions := float64(m.shardAdmissionCounts[shardID])
	minGap := maxFloat(64.0, avgAdmissions*0.25)
	return targetAdmissions > avgAdmissions*m.overflowRatio &&
		targetAdmissions-float64(minAdmissions) > minGap
}

func (m *RuntimeUnknownAdmissionManager) chooseLoadFallbackShardLocked(account string) uint64 {
	bestShard := uint64(utils.Addr2Shard(account))
	if activePressure, _, _ := m.activePressureByShardLocked(); len(activePressure) > 0 {
		bestScore := 1e18
		maxAdmissions := 1
		for _, count := range m.shardAdmissionCounts {
			if count > maxAdmissions {
				maxAdmissions = count
			}
		}
		for shardID := uint64(0); shardID < m.shardNum; shardID++ {
			admissionPenalty := float64(m.shardAdmissionCounts[shardID]) / float64(maxAdmissions)
			// Actual committed pressure dominates; admission count only avoids
			// repeatedly filling the same currently-cold target.
			score := activePressure[shardID] + 0.25*admissionPenalty
			if score < bestScore || (score == bestScore && shardID < bestShard) {
				bestScore = score
				bestShard = shardID
			}
		}
		return bestShard
	}
	bestLoad := int(^uint(0) >> 1)
	preferredShard := uint64(utils.Addr2Shard(account))
	for shardID := uint64(0); shardID < m.shardNum; shardID++ {
		load := m.shardAdmissionCounts[shardID]
		if load < bestLoad {
			bestLoad = load
			bestShard = shardID
			continue
		}
		if load == bestLoad {
			if shardID == preferredShard {
				bestShard = shardID
				continue
			}
			if bestShard != preferredShard && shardID < bestShard {
				bestShard = shardID
			}
		}
	}
	return bestShard
}

func (m *RuntimeUnknownAdmissionManager) chooseLocalityPreservingDiversionShardLocked(account string, obs *runtimeUnknownObservation, excludedHotShard uint64) uint64 {
	activePressure, _, _ := m.activePressureByShardLocked()
	if obs != nil && len(activePressure) == int(m.shardNum) {
		totalPressure := 0.0
		for shardID := uint64(0); shardID < m.shardNum; shardID++ {
			totalPressure += activePressure[shardID]
		}
		avgPressure := totalPressure / maxFloat(1.0, float64(m.shardNum))
		bestShard := excludedHotShard
		bestScore := -1e18
		for shardID, evidenceCount := range obs.ShardEvidence {
			if shardID == excludedHotShard || evidenceCount <= 0 || activePressure[shardID] > avgPressure {
				continue
			}
			// Preserve observed locality whenever a non-hot interacted shard
			// exists; load relief remains a secondary preference.
			score := m.localityScoreLocked(obs, shardID) + 0.12*(avgPressure-activePressure[shardID])
			if score > bestScore || (score == bestScore && shardID < bestShard) {
				bestScore = score
				bestShard = shardID
			}
		}
		if bestShard != excludedHotShard {
			return bestShard
		}
	}
	return m.chooseLoadFallbackShardLocked(account)
}

func (m *RuntimeUnknownAdmissionManager) chooseAnchorRealignmentTargetLocked(obs *runtimeUnknownObservation, currentShard uint64) uint64 {
	if obs == nil || obs.TotalTxCount <= 0 {
		return currentShard
	}
	bestShard := currentShard
	bestScore := -1e18
	for shardID, evidenceCount := range obs.ShardEvidence {
		if shardID == currentShard || evidenceCount <= 0 {
			continue
		}
		score := m.localityScoreLocked(obs, shardID)
		// Stable-anchor evidence is the primary reason for proposing a
		// post-admission remap. Load remains secondary through localityScore.
		score += 0.20 * share(obs.StableShardEvidence[shardID], obs.TotalTxCount)
		if score > bestScore || (score == bestScore && shardID < bestShard) {
			bestScore = score
			bestShard = shardID
		}
	}
	return bestShard
}
