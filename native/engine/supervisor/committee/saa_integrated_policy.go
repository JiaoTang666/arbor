package committee

import (
	"math"
	"sort"
)

type saaIntegratedConfig struct {
	AnchorWeight        float64
	LocalityWeight      float64
	QueueWeight         float64
	HandoffWeight       float64
	QueueCapacity       float64
	StateBytesScale     float64
	EffectHorizonEpochs float64
	BaseHandoffCost     float64
	RecentSwitchCost    float64
}

func defaultSAAIntegratedConfig() saaIntegratedConfig {
	return saaIntegratedConfig{
		AnchorWeight: 0.35, LocalityWeight: 0.35,
		QueueWeight: 0.20, HandoffWeight: 0.10,
		QueueCapacity: 1, StateBytesScale: 1, EffectHorizonEpochs: 1,
		BaseHandoffCost: 0.05, RecentSwitchCost: 0.10,
	}
}

type saaQueueSnapshot struct {
	QueueLength        float64 `json:"queue_length"`
	AcceptedWork       float64 `json:"accepted_work"`
	CommittedWork      float64 `json:"committed_work"`
	EstimatedService   float64 `json:"estimated_service"`
	PendingHandoffWork float64 `json:"pending_handoff_work"`
	SnapshotEpoch      int     `json:"snapshot_epoch"`
	SignalAge          int     `json:"signal_age"`
	SignalMissing      bool    `json:"signal_missing"`
}

type saaAnchorExplanation struct {
	AnchorID                  string             `json:"anchor_id"`
	AnchorVersion             uint64             `json:"anchor_version"`
	EvidenceSequenceStart     uint64             `json:"evidence_sequence_start"`
	EvidenceSequenceEnd       uint64             `json:"evidence_sequence_end"`
	AnchorInteractionCount    int                `json:"anchor_interaction_count"`
	TotalInteractionCount     int                `json:"total_interaction_count"`
	AnchorCoverage            float64            `json:"anchor_coverage"`
	TargetShardDistribution   map[uint64]float64 `json:"target_shard_distribution"`
	UnresolvedInteractionMass float64            `json:"unresolved_interaction_mass"`
	InitializationSourceHash  string             `json:"initialization_source_hash"`
	StructureStatus           string             `json:"structure_status"`
}

type saaIntegratedTargetInput struct {
	ShardID                 uint64
	AnchorAffinity          float64
	RecentLocality          float64
	ProjectedIncomingWork   float64
	ProjectedCrossShardWork float64
	StateBytes              float64
	PredictedEffectEpochs   float64
	RecentSwitchRate        float64
	Queue                   saaQueueSnapshot
	Anchor                  saaAnchorExplanation
}

type saaIntegratedCostComponents struct {
	AnchorBenefit   float64 `json:"anchor_benefit"`
	LocalityBenefit float64 `json:"locality_benefit"`
	QueueCost       float64 `json:"queue_cost"`
	HandoffCost     float64 `json:"handoff_cost"`
	Utility         float64 `json:"utility"`
}

type saaIntegratedCandidate struct {
	ShardID    uint64                      `json:"shard_id"`
	Feasible   bool                        `json:"feasible"`
	Reason     string                      `json:"reason"`
	Components saaIntegratedCostComponents `json:"components"`
	Anchor     saaAnchorExplanation        `json:"anchor"`
}

type saaIntegratedDecision struct {
	CurrentShard  uint64                   `json:"current_shard"`
	SelectedShard uint64                   `json:"selected_shard"`
	Action        string                   `json:"action"`
	Reason        string                   `json:"reason"`
	KeepUtility   float64                  `json:"keep_utility"`
	BestUtility   float64                  `json:"best_utility"`
	NetGain       float64                  `json:"net_gain"`
	Candidates    []saaIntegratedCandidate `json:"candidates"`
}

func positiveOrZero(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0
	}
	return value
}

func saaProjectedQueueArea(input saaIntegratedTargetInput, cfg saaIntegratedConfig) float64 {
	queue := positiveOrZero(input.Queue.QueueLength)
	arrivals := positiveOrZero(input.Queue.AcceptedWork-input.Queue.CommittedWork) +
		positiveOrZero(input.ProjectedIncomingWork) +
		positiveOrZero(input.ProjectedCrossShardWork) +
		positiveOrZero(input.Queue.PendingHandoffWork)
	service := positiveOrZero(input.Queue.EstimatedService)
	next := math.Max(0, queue+arrivals-service)
	capacity := math.Max(1, positiveOrZero(cfg.QueueCapacity))
	return (queue + next) / (2 * capacity)
}

func saaExplicitHandoffCost(input saaIntegratedTargetInput, currentShard uint64, cfg saaIntegratedConfig) float64 {
	if input.ShardID == currentShard {
		return 0
	}
	stateScale := math.Max(1, positiveOrZero(cfg.StateBytesScale))
	horizon := math.Max(1, positiveOrZero(cfg.EffectHorizonEpochs))
	return positiveOrZero(cfg.BaseHandoffCost) +
		positiveOrZero(cfg.RecentSwitchCost)*positiveOrZero(input.RecentSwitchRate) +
		positiveOrZero(input.StateBytes)/stateScale +
		positiveOrZero(input.PredictedEffectEpochs)/horizon
}

func scoreSAAIntegratedTarget(input saaIntegratedTargetInput, currentShard uint64, cfg saaIntegratedConfig) saaIntegratedCandidate {
	candidate := saaIntegratedCandidate{ShardID: input.ShardID, Feasible: true, Reason: "feasible", Anchor: input.Anchor}
	if input.Queue.SignalMissing || input.Queue.SignalAge > 2 {
		candidate.Feasible = input.ShardID == currentShard
		candidate.Reason = "queue_signal_missing_or_stale"
	}
	anchor := positiveOrZero(input.AnchorAffinity)
	locality := positiveOrZero(input.RecentLocality)
	queueCost := saaProjectedQueueArea(input, cfg)
	handoffCost := saaExplicitHandoffCost(input, currentShard, cfg)
	candidate.Components = saaIntegratedCostComponents{
		AnchorBenefit:   cfg.AnchorWeight * anchor,
		LocalityBenefit: cfg.LocalityWeight * locality,
		QueueCost:       cfg.QueueWeight * queueCost,
		HandoffCost:     cfg.HandoffWeight * handoffCost,
	}
	candidate.Components.Utility = candidate.Components.AnchorBenefit +
		candidate.Components.LocalityBenefit - candidate.Components.QueueCost - candidate.Components.HandoffCost
	return candidate
}

func decideSAAIntegrated(currentShard uint64, inputs []saaIntegratedTargetInput, cfg saaIntegratedConfig) saaIntegratedDecision {
	candidates := make([]saaIntegratedCandidate, 0, len(inputs))
	for _, input := range inputs {
		candidates = append(candidates, scoreSAAIntegratedTarget(input, currentShard, cfg))
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ShardID < candidates[j].ShardID })
	decision := saaIntegratedDecision{
		CurrentShard: currentShard, SelectedShard: currentShard,
		Action: "retain", Reason: "keep_is_best_feasible_action",
		KeepUtility: math.Inf(-1), BestUtility: math.Inf(-1), Candidates: candidates,
	}
	bestShard := currentShard
	for _, candidate := range candidates {
		if candidate.ShardID == currentShard {
			decision.KeepUtility = candidate.Components.Utility
		}
		if !candidate.Feasible {
			continue
		}
		if candidate.Components.Utility > decision.BestUtility ||
			(candidate.Components.Utility == decision.BestUtility && candidate.ShardID < bestShard) {
			decision.BestUtility = candidate.Components.Utility
			bestShard = candidate.ShardID
		}
	}
	if math.IsInf(decision.KeepUtility, -1) {
		decision.Action = "reject"
		decision.Reason = "keep_candidate_missing"
		decision.NetGain = math.Inf(-1)
		return decision
	}
	decision.NetGain = decision.BestUtility - decision.KeepUtility
	if bestShard != currentShard && decision.NetGain > 0 {
		decision.SelectedShard = bestShard
		decision.Action = "migrate"
		decision.Reason = "positive_net_gain"
	}
	return decision
}
