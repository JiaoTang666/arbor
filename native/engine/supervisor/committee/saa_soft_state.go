package committee

// saaSoftAccountState is the sufficient online state for one runtime account.
// Raw cumulative observations remain outside the policy; only decayed values
// enter target scoring.
type saaSoftAccountState struct {
	CurrentRound             int
	LastTotalTx              int
	LastShardEvidence        map[uint64]int
	LastAnchorEvidence       map[uint64]int
	EpochStartTotalTx        int
	EpochStartShardEvidence  map[uint64]int
	EpochStartAnchorEvidence map[uint64]int
	EpochBaseLocality        map[uint64]float64
	EpochBaseAnchor          map[uint64]float64
	EpochBaseAnchorSupport   map[uint64]float64
	EpochBasePersistence     float64
	EpochBaseSwitchRate      float64
	LocalityByShard          map[uint64]float64
	AnchorByShard            map[uint64]float64
	AnchorSupportByShard     map[uint64]float64
	Persistence              float64
	RecentSwitchRate         float64
	SwitchObservedThisRound  bool
	LastTarget               uint64
	HasTarget                bool
}

type saaSoftLoadState struct {
	Value          float64
	LastValidRound int
	SignalMissing  bool
	SignalSource   string
}

func newSAASoftAccountState() *saaSoftAccountState {
	return &saaSoftAccountState{
		CurrentRound:             -1,
		LastShardEvidence:        make(map[uint64]int),
		LastAnchorEvidence:       make(map[uint64]int),
		EpochStartShardEvidence:  make(map[uint64]int),
		EpochStartAnchorEvidence: make(map[uint64]int),
		EpochBaseLocality:        make(map[uint64]float64),
		EpochBaseAnchor:          make(map[uint64]float64),
		EpochBaseAnchorSupport:   make(map[uint64]float64),
		LocalityByShard:          make(map[uint64]float64),
		AnchorByShard:            make(map[uint64]float64),
		AnchorSupportByShard:     make(map[uint64]float64),
	}
}

func (m *RuntimeUnknownAdmissionManager) beginSAASoftEpochLocked(state *saaSoftAccountState, logicalRound int) int {
	if state.CurrentRound == logicalRound {
		return 1
	}
	gap := 1
	if state.CurrentRound >= 0 {
		gap = maxInt(logicalRound-state.CurrentRound, 1)
	}
	missingEpochs := maxInt(gap-1, 0)
	for shardID := uint64(0); shardID < m.shardNum; shardID++ {
		state.EpochBaseLocality[shardID] = state.LocalityByShard[shardID] * saaRetentionFactor(missingEpochs, m.softDecay.ObservationTau)
		state.EpochBaseAnchor[shardID] = state.AnchorByShard[shardID] * saaRetentionFactor(missingEpochs, m.softDecay.AnchorTau)
		state.EpochBaseAnchorSupport[shardID] = state.AnchorSupportByShard[shardID] * saaRetentionFactor(gap, m.softDecay.AnchorTau)
		state.EpochStartShardEvidence[shardID] = state.LastShardEvidence[shardID]
		state.EpochStartAnchorEvidence[shardID] = state.LastAnchorEvidence[shardID]
	}
	state.EpochBasePersistence = state.Persistence * saaRetentionFactor(missingEpochs, m.softDecay.PersistenceTau)
	state.EpochBaseSwitchRate = state.RecentSwitchRate * saaRetentionFactor(missingEpochs, m.softDecay.PersistenceTau)
	state.EpochStartTotalTx = state.LastTotalTx
	state.SwitchObservedThisRound = false
	state.CurrentRound = logicalRound
	return gap
}

func (m *RuntimeUnknownAdmissionManager) updateSAASoftAccountStateLocked(account string, obs *runtimeUnknownObservation, logicalRound int) *saaSoftAccountState {
	state := m.saaSoftStateByAccount[account]
	if state == nil {
		state = newSAASoftAccountState()
		m.saaSoftStateByAccount[account] = state
	}
	gap := m.beginSAASoftEpochLocked(state, logicalRound)
	if obs == nil {
		state.Persistence = state.EpochBasePersistence * saaRetentionFactor(1, m.softDecay.PersistenceTau)
		state.RecentSwitchRate = state.EpochBaseSwitchRate * saaRetentionFactor(1, m.softDecay.PersistenceTau)
		return state
	}
	epochTotal := obs.TotalTxCount - state.EpochStartTotalTx
	if epochTotal < 0 {
		epochTotal = obs.TotalTxCount
	}
	denominator := maxInt(epochTotal, 1)
	observationAlpha := saaDecayFactor(1, m.softDecay.ObservationTau)
	anchorAlpha := saaDecayFactor(1, m.softDecay.AnchorTau)
	for shardID := uint64(0); shardID < m.shardNum; shardID++ {
		epochLocality := obs.ShardEvidence[shardID] - state.EpochStartShardEvidence[shardID]
		epochAnchor := obs.StableShardEvidence[shardID] - state.EpochStartAnchorEvidence[shardID]
		if epochLocality < 0 {
			epochLocality = obs.ShardEvidence[shardID]
		}
		if epochAnchor < 0 {
			epochAnchor = obs.StableShardEvidence[shardID]
		}
		localityShare := float64(epochLocality) / float64(denominator)
		anchorShare := float64(epochAnchor) / float64(denominator)
		state.LocalityByShard[shardID] = saaClamp01(state.EpochBaseLocality[shardID]*(1-observationAlpha) + localityShare*observationAlpha)
		state.AnchorByShard[shardID] = saaClamp01(state.EpochBaseAnchor[shardID]*(1-anchorAlpha) + anchorShare*anchorAlpha)
		state.AnchorSupportByShard[shardID] = state.EpochBaseAnchorSupport[shardID] + float64(maxInt(epochAnchor, 0))
		state.LastShardEvidence[shardID] = obs.ShardEvidence[shardID]
		state.LastAnchorEvidence[shardID] = obs.StableShardEvidence[shardID]
	}
	recurrence := 0.0
	if epochTotal > 0 {
		recurrence = 1 / float64(gap)
	}
	persistenceAlpha := saaDecayFactor(1, m.softDecay.PersistenceTau)
	state.Persistence = saaClamp01(state.EpochBasePersistence*(1-persistenceAlpha) + recurrence*persistenceAlpha)
	switchObserved := 0.0
	if state.SwitchObservedThisRound {
		switchObserved = 1
	}
	state.RecentSwitchRate = saaClamp01(state.EpochBaseSwitchRate*(1-persistenceAlpha) + switchObserved*persistenceAlpha)
	state.LastTotalTx = obs.TotalTxCount
	return state
}

func (state *saaSoftAccountState) recordSwitch(tau float64) {
	state.SwitchObservedThisRound = true
	alpha := saaDecayFactor(1, tau)
	state.RecentSwitchRate = saaClamp01(state.EpochBaseSwitchRate*(1-alpha) + alpha)
}

func (m *RuntimeUnknownAdmissionManager) updateSAASoftLoadStateLocked(logicalRound int) {
	if m.saaSoftLoadLastRound == logicalRound {
		return
	}
	deltaRound := 1
	if m.saaSoftLoadLastRound >= 0 {
		deltaRound = maxInt(logicalRound-m.saaSoftLoadLastRound, 1)
	}
	activePressure, pressureSource, pressureEpoch := m.activePressureByShardLocked()
	hasPressure := len(activePressure) == int(m.shardNum)
	hasQueue := len(m.liveQueueByShard) == int(m.shardNum)
	maxPressure, maxQueue, maxAdmissions := 1e-9, 1e-9, 0
	for shardID := uint64(0); shardID < m.shardNum; shardID++ {
		if activePressure[shardID] > maxPressure {
			maxPressure = activePressure[shardID]
		}
		if m.liveQueueByShard[shardID] > maxQueue {
			maxQueue = m.liveQueueByShard[shardID]
		}
		if m.shardAdmissionCounts[shardID] > maxAdmissions {
			maxAdmissions = m.shardAdmissionCounts[shardID]
		}
	}
	for shardID := uint64(0); shardID < m.shardNum; shardID++ {
		state := m.saaSoftLoadByShard[shardID]
		fallback := state.Value
		if state.LastValidRound == 0 && m.saaSoftLoadLastRound < 0 {
			fallback = 0.5
		}
		queue := fallback
		pressure := fallback
		if hasQueue {
			queue = saaNormalize(m.liveQueueByShard[shardID], maxQueue)
		}
		if hasPressure {
			pressure = saaNormalize(activePressure[shardID], maxPressure)
		}
		projected := 0.5
		if maxAdmissions > 0 {
			projected = saaNormalize(float64(m.shardAdmissionCounts[shardID]), float64(maxAdmissions))
		}
		observed := 0.5*queue + 0.3*pressure + 0.2*projected
		state.Value = saaEma(state.Value, observed, deltaRound, m.softDecay.LoadTau)
		state.SignalMissing = !hasQueue || !hasPressure
		state.SignalSource = pressureSource
		if state.SignalMissing && state.SignalSource == "" {
			state.SignalSource = "decayed_last_valid_plus_projected_demand"
		}
		if hasQueue || hasPressure {
			state.LastValidRound = maxInt(pressureEpoch, logicalRound)
		}
		m.saaSoftLoadByShard[shardID] = state
	}
	m.saaSoftLoadLastRound = logicalRound
}
