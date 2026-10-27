package committee

import (
	"strings"
	"testing"
)

func TestForecastBridgeUsesExistingAuthority(t *testing.T) {
	account := authorityAccount('a')
	a := newMappingAuthority(2, map[string]uint64{account: 0})
	b := newForecastAuthorityBridge(a)
	e := forecastAuthorityEnvelope{DecisionID: "forecast", InputHash: strings.Repeat("1", 64), AnchorVersion: 7, Account: account, TargetShard: 1, EffectiveEpoch: 2, QueueAvailable: true, QueueEpoch: 1}
	if d := b.submit(e, 1, 7, 1); d.Status != mappingAccepted {
		t.Fatal(d)
	}
	if s, _ := a.resolve(account); s != 0 {
		t.Fatal("premature route change")
	}
	a.applyDue(2)
	if d := b.submit(e, 2, 7, 1); d.Status != mappingApplied {
		t.Fatal(d)
	}
	if s, _ := a.resolve(account); s != 1 {
		t.Fatal("authority route unchanged")
	}
	e.TargetShard = 0
	if d := b.submit(e, 2, 7, 1); d.Reason != "decision_id_payload_conflict" {
		t.Fatal(d)
	}
}

func TestForecastBridgeQueueVetoAndAnchorVersion(t *testing.T) {
	account := authorityAccount('b')
	b := newForecastAuthorityBridge(newMappingAuthority(2, map[string]uint64{account: 0}))
	e := forecastAuthorityEnvelope{DecisionID: "p", InputHash: strings.Repeat("1", 64), AnchorVersion: 2, Account: account, TargetShard: 1, EffectiveEpoch: 2}
	if d := b.submit(e, 1, 3, 1); d.Reason != "anchor_version_mismatch" {
		t.Fatal(d)
	}
	if d := b.submit(e, 1, 2, 1); d.Reason != "queue_missing_or_stale" {
		t.Fatal(d)
	}
	e.Keep = true
	e.TargetShard = 0
	if d := b.submit(e, 1, 2, 1); d.Status != mappingNoop {
		t.Fatal(d)
	}
	if b.authority.routeVersion != 0 || len(b.authority.pending) != 0 {
		t.Fatal("keep mutated route")
	}
}
