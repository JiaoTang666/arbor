package committee

import (
	"math"
	"testing"
)

func queueAt(length float64) saaQueueSnapshot {
	return saaQueueSnapshot{QueueLength: length, EstimatedService: 1, SnapshotEpoch: 3}
}

func TestIntegratedQueueCostIsAbsoluteAndMonotonic(t *testing.T) {
	cfg := defaultSAAIntegratedConfig()
	cfg.QueueCapacity = 10
	low := saaIntegratedTargetInput{ShardID: 1, Queue: queueAt(2)}
	high := saaIntegratedTargetInput{ShardID: 1, Queue: queueAt(20)}
	if saaProjectedQueueArea(high, cfg) <= saaProjectedQueueArea(low, cfg) {
		t.Fatal("higher absolute queue did not increase queue area")
	}
	beforeBatch := saaProjectedQueueArea(high, cfg)
	high.ProjectedIncomingWork = 10
	if saaProjectedQueueArea(high, cfg) <= beforeBatch {
		t.Fatal("projected batch work was not charged to the target queue")
	}
}

func TestIntegratedPolicyKeepsWhenTargetQueueMissing(t *testing.T) {
	cfg := defaultSAAIntegratedConfig()
	decision := decideSAAIntegrated(0, []saaIntegratedTargetInput{
		{ShardID: 0, AnchorAffinity: 0.1, RecentLocality: 0.1, Queue: queueAt(1)},
		{ShardID: 1, AnchorAffinity: 1, RecentLocality: 1, Queue: saaQueueSnapshot{SignalMissing: true}},
	}, cfg)
	if decision.Action != "retain" || decision.SelectedShard != 0 {
		t.Fatalf("decision=%+v", decision)
	}
}

func TestIntegratedPolicyUsesOnePositiveNetGainRule(t *testing.T) {
	cfg := defaultSAAIntegratedConfig()
	cfg.BaseHandoffCost = 0
	decision := decideSAAIntegrated(0, []saaIntegratedTargetInput{
		{ShardID: 0, AnchorAffinity: 0.1, RecentLocality: 0.1, Queue: queueAt(4)},
		{ShardID: 1, AnchorAffinity: 0.9, RecentLocality: 0.9, Queue: queueAt(1)},
	}, cfg)
	if decision.Action != "migrate" || decision.SelectedShard != 1 || decision.NetGain <= 0 {
		t.Fatalf("decision=%+v", decision)
	}
}

func TestIntegratedComponentsReconstructUtility(t *testing.T) {
	candidate := scoreSAAIntegratedTarget(saaIntegratedTargetInput{
		ShardID: 1, AnchorAffinity: 0.7, RecentLocality: 0.6,
		Queue: queueAt(3), StateBytes: 2, PredictedEffectEpochs: 1,
	}, 0, defaultSAAIntegratedConfig())
	c := candidate.Components
	recomputed := c.AnchorBenefit + c.LocalityBenefit - c.QueueCost - c.HandoffCost
	if math.Abs(recomputed-c.Utility) > 1e-12 {
		t.Fatalf("utility=%f recomputed=%f", c.Utility, recomputed)
	}
}

func TestIntegratedPolicyTieBreakIsDeterministic(t *testing.T) {
	cfg := defaultSAAIntegratedConfig()
	cfg.BaseHandoffCost = 0
	inputs := []saaIntegratedTargetInput{
		{ShardID: 2, AnchorAffinity: 1, RecentLocality: 1, Queue: queueAt(0)},
		{ShardID: 1, AnchorAffinity: 1, RecentLocality: 1, Queue: queueAt(0)},
		{ShardID: 0, Queue: queueAt(10)},
	}
	decision := decideSAAIntegrated(0, inputs, cfg)
	if decision.SelectedShard != 1 {
		t.Fatalf("selected shard=%d, want deterministic lowest tied shard 1", decision.SelectedShard)
	}
}
