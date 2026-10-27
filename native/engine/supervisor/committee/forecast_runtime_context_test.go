package committee

import "testing"

func TestForecastContextUsesAuthorityAndRetainsMissing(t *testing.T) {
	t.Setenv(integratedControllerEnv, forecastControllerMode)
	account := authorityAccount('a')
	m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2, stableShardByAccount: map[string]uint64{account: 1}, liveQueueByShard: map[uint64]float64{1: 7}, pressureHintEpoch: 3, stableShardByCluster: map[int]uint64{2: 1}}
	defer saaIntegratedRuntimeStates.Delete(m)
	s := m.integratedStateLocked()
	s.modelAnchor = &forecastAnchorState{Accounts: map[string]forecastAnchorAccount{account: {}}}
	s.forecastAnchorVersion = 1
	snapshot := m.forecastContextLocked(4)
	if snapshot["routes"].(map[string]uint64)[account] != 1 || snapshot["queue_age_epochs"] != 1 {
		t.Fatal(snapshot)
	}
	for _, key := range []string{"baseline_arrivals", "seconds", "state_bytes"} {
		if snapshot[key] != nil {
			t.Fatalf("missing %s fabricated", key)
		}
	}
	if len(snapshot["service_rates"].(map[uint64]float64)) != 0 {
		t.Fatal("service observations fabricated")
	}
	m.liveQueueByShard[1] = 999
	if snapshot["queues"].(map[uint64]float64)[1] != 7 {
		t.Fatal("snapshot aliases mutable queue")
	}
}
