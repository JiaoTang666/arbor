package committee

import (
	"strings"
	"testing"
)

func TestForecastAnchorInstallationIsolation(t *testing.T) {
	m := &RuntimeUnknownAdmissionManager{softDecisionMode: saaIntegratedDecisionMode, shardNum: 2}
	defer saaIntegratedRuntimeStates.Delete(m)
	account := authorityAccount('a')
	next := forecastAnchorState{AnchorVersion: 1, EvidenceStart: 0, EvidenceEnd: 8000, CheckpointHash: strings.Repeat("1", 64), Accounts: map[string]forecastAnchorAccount{
		account: {SoftMembership: map[string]float64{"stable:1": .75}, KnownRelationMass: 4, UnresolvedPartnerMass: .25},
	}}
	state := m.integratedStateLocked()
	before := state.authority.stateDigest()
	if err := m.installForecastAnchorLocked(next, 7999); err == nil {
		t.Fatal("future evidence accepted")
	}
	if state.forecastAnchorVersion != 0 {
		t.Fatal("failed install advanced version")
	}
	if err := m.installForecastAnchorLocked(next, 8000); err != nil {
		t.Fatal(err)
	}
	if state.authority.stateDigest() != before {
		t.Fatal("anchor installation changed mapping")
	}
	next.Accounts[account].SoftMembership["stable:1"] = 0
	if state.modelAnchor.Accounts[account].SoftMembership["stable:1"] != .75 {
		t.Fatal("installed state aliases caller memory")
	}
	if err := m.installForecastAnchorLocked(next, 8000); err == nil {
		t.Fatal("duplicate version accepted")
	}
	next.AnchorVersion = 2
	next.EvidenceEnd = 10000
	if err := m.installForecastAnchorLocked(next, 10000); err == nil {
		t.Fatal("nonconserved weights accepted")
	}
	if state.forecastAnchorVersion != 1 {
		t.Fatal("invalid weights advanced version")
	}
}
