package committee

import (
	"os"
	"path/filepath"
	"testing"
)

func TestForecastControllerExcludesLegacyIngress(t *testing.T) {
	t.Setenv(integratedControllerEnv, forecastControllerMode)
	account := authorityAccount('a')
	m := &RuntimeUnknownAdmissionManager{enabled: true, externalActionEnabled: true, softDecisionMode: saaIntegratedDecisionMode, shardNum: 2, stableShardByAccount: map[string]uint64{account: 0}, appliedExternalUpdates: map[string]bool{}}
	defer saaIntegratedRuntimeStates.Delete(m)
	state := m.integratedStateLocked()
	before := state.authority.stateDigest()
	path := filepath.Join(t.TempDir(), "legacy.jsonl")
	if err := os.WriteFile(path, []byte(`{"proposal_id":"legacy-attempt","addresses":["`+account+`"],"source_shard":0,"target_shard":1,"effective_epoch":1}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(runtimeExternalMappingUpdatePathEnv, path)
	m.refreshExternalMappingUpdatesLocked(2)
	m.decideSAAIntegratedLocked(authorityAccount('b'), nil, 2, true)
	if len(state.authority.pending) != 0 || len(state.lastDecisionEpoch) != 0 || len(m.appliedExternalUpdates) != 0 {
		t.Fatal("legacy ingress acted in forecast mode")
	}
	if before != state.authority.stateDigest() {
		t.Fatal("legacy ingress changed routing")
	}
	// Ownership is selected once, not switched by a mid-run environment edit.
	t.Setenv(integratedControllerEnv, "legacy")
	if m.integratedStateLocked().controllerMode != forecastControllerMode {
		t.Fatal("controller changed mid-run")
	}
}

func TestLegacyControllerRejectsForecastIngress(t *testing.T) {
	t.Setenv(integratedControllerEnv, "")
	m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2}
	defer saaIntegratedRuntimeStates.Delete(m)
	d := m.submitForecastLocked(forecastAuthorityEnvelope{DecisionID: "forecast-attempt", Keep: true}, 1, 1)
	if d.Status != mappingRejected || d.Reason != "forecast_controller_not_selected" {
		t.Fatalf("unexpected receipt: %+v", d)
	}
	if len(m.integratedStateLocked().authority.pending) != 0 {
		t.Fatal("forecast queued in legacy mode")
	}
}
