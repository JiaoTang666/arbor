package committee

import "math"

type saaSoftTargetFeatures struct {
	AnchorConfidence   float64 `json:"anchor_confidence"`
	LocalityConfidence float64 `json:"locality_confidence"`
	Persistence        float64 `json:"temporal_persistence"`
	CrossCost          float64 `json:"cross_cost"`
	LoadPressure       float64 `json:"load_pressure"`
	MigrationRisk      float64 `json:"migration_risk"`
	LoadSignalMissing  bool    `json:"load_signal_missing"`
}

type saaSoftTargetScore struct {
	ShardID  uint64                `json:"shard_id"`
	Features saaSoftTargetFeatures `json:"features"`
	Score    float64               `json:"score"`
}

func saaScoreTarget(features saaSoftTargetFeatures, cfg saaSoftDecayConfig) float64 {
	return cfg.AnchorWeight*features.AnchorConfidence +
		cfg.LocalityWeight*features.LocalityConfidence +
		cfg.PersistenceWeight*features.Persistence -
		cfg.CrossWeight*features.CrossCost -
		cfg.LoadWeight*features.LoadPressure -
		cfg.RiskWeight*features.MigrationRisk
}

func (m *RuntimeUnknownAdmissionManager) scoreSAASoftTargetsLocked(state *saaSoftAccountState, currentShard uint64) []saaSoftTargetScore {
	result := make([]saaSoftTargetScore, 0, m.shardNum)
	for shardID := uint64(0); shardID < m.shardNum; shardID++ {
		locality := saaClamp01(state.LocalityByShard[shardID])
		anchorShare := saaClamp01(state.AnchorByShard[shardID])
		entropyPenalty := 1 - anchorShare
		anchorConfidence := saaSigmoid(4*anchorShare + 0.25*math.Log1p(state.AnchorSupportByShard[shardID]) + state.Persistence - 0.8*entropyPenalty)
		load := m.saaSoftLoadByShard[shardID]
		switchCost := 0.0
		if shardID != currentShard {
			switchCost = 1
		}
		anchorCutDamage := 0.0
		if shardID != currentShard {
			anchorCutDamage = state.AnchorByShard[currentShard] * (1 - anchorShare)
		}
		migrationRisk := saaClamp01(0.35*load.Value + 0.25*switchCost + 0.20*anchorCutDamage + 0.20*state.RecentSwitchRate)
		features := saaSoftTargetFeatures{
			AnchorConfidence:   anchorConfidence,
			LocalityConfidence: locality,
			Persistence:        state.Persistence,
			CrossCost:          1 - locality,
			LoadPressure:       load.Value,
			MigrationRisk:      migrationRisk,
			LoadSignalMissing:  load.SignalMissing,
		}
		result = append(result, saaSoftTargetScore{ShardID: shardID, Features: features, Score: saaScoreTarget(features, m.softDecay)})
	}
	return result
}
