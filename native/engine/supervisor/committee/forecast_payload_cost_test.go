package committee

import "testing"

func TestForecastPayloadCostIsExplicitAndFresh(t *testing.T) {
	t.Setenv(integratedControllerEnv, forecastControllerMode)
	t.Setenv("SAA_FORECAST_STATE_COST_SCOPE", "committed_payload_v1")
	a := authorityAccount('a')
	m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2, stableShardByAccount: map[string]uint64{a: 0}}
	defer saaIntegratedRuntimeStates.Delete(m)
	s := m.integratedStateLocked()
	s.modelAnchor = &forecastAnchorState{Accounts: map[string]forecastAnchorAccount{a: {}}}
	s.forecastStatePayloads = map[uint64]map[string]forecastStatePayloadObservation{0: {a: {Bytes: 200, Epoch: 2, Shard: 0}}}
	if m.forecastContextLocked(3)["state_bytes"].(map[string]uint64)[a] != 200 {
		t.Fatal("observed payload lost")
	}
	if len(m.forecastContextLocked(5)["state_bytes"].(map[string]uint64)) != 0 {
		t.Fatal("stale payload accepted")
	}
	s.authority.active[a] = 1
	if len(m.forecastContextLocked(3)["state_bytes"].(map[string]uint64)) != 0 {
		t.Fatal("wrong shard evidence accepted")
	}
}
