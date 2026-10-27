package committee

import "testing"

func authorityAccount(ch byte) string {
	buf := make([]byte, 42)
	buf[0], buf[1] = '0', 'x'
	for i := 2; i < len(buf); i++ {
		buf[i] = ch
	}
	return string(buf)
}

func TestMappingAuthorityAppliesDisjointSharedBaseProposals(t *testing.T) {
	a := newMappingAuthority(4, nil)
	left, right := authorityAccount('1'), authorityAccount('2')
	for _, proposal := range []mappingAuthorityProposal{
		{ProposalID: "b", Addresses: []string{right}, TargetShard: 2, BaseRouteVersion: 0, EffectiveEpoch: 2},
		{ProposalID: "a", Addresses: []string{left}, TargetShard: 1, BaseRouteVersion: 0, EffectiveEpoch: 2},
	} {
		if got := a.submit(proposal, 1); got.Status != mappingAccepted {
			t.Fatalf("submit status=%s reason=%s", got.Status, got.Reason)
		}
	}
	if got := a.applyDue(1); len(got) != 0 {
		t.Fatalf("applied before boundary: %v", got)
	}
	a.recordInstallationReceipt("a")
	a.recordInstallationReceipt("b")
	got := a.applyDue(2)
	if len(got) != 2 || got[0].ProposalID != "a" || got[1].ProposalID != "b" {
		t.Fatalf("non-deterministic apply order: %+v", got)
	}
	if a.routeVersion != 2 {
		t.Fatalf("route version=%d, want 2", a.routeVersion)
	}
	if shard, ok := a.resolve(left); !ok || shard != 1 {
		t.Fatalf("left route=(%d,%v)", shard, ok)
	}
	if shard, ok := a.resolve(right); !ok || shard != 2 {
		t.Fatalf("right route=(%d,%v)", shard, ok)
	}
}

func TestMappingAuthorityIsIdempotentAndRejectsConflicts(t *testing.T) {
	a := newMappingAuthority(4, nil)
	account := authorityAccount('a')
	p := mappingAuthorityProposal{ProposalID: "p1", Addresses: []string{account}, TargetShard: 1, EffectiveEpoch: 2}
	first := a.submit(p, 1)
	if duplicate := a.submit(p, 1); duplicate != first {
		t.Fatalf("duplicate decision changed: first=%+v duplicate=%+v", first, duplicate)
	}
	conflict := a.submit(mappingAuthorityProposal{
		ProposalID: "p2", Addresses: []string{account}, TargetShard: 2, EffectiveEpoch: 2,
	}, 1)
	if conflict.Status != mappingRejected || conflict.Reason != "account_has_pending_proposal" {
		t.Fatalf("conflict=%+v", conflict)
	}
	a.recordInstallationReceipt("p1")
	a.applyDue(2)
	stale := a.submit(mappingAuthorityProposal{
		ProposalID: "p3", Addresses: []string{account}, TargetShard: 3,
		BaseRouteVersion: 0, EffectiveEpoch: 4,
	}, 3)
	if stale.Status != mappingRejected || stale.Reason != "stale_account_version" {
		t.Fatalf("stale=%+v", stale)
	}
}

func TestMappingAuthorityRejectsInvalidProposal(t *testing.T) {
	a := newMappingAuthority(2, nil)
	tests := []mappingAuthorityProposal{
		{ProposalID: "", Addresses: []string{authorityAccount('1')}, TargetShard: 1, EffectiveEpoch: 2},
		{ProposalID: "bad-address", Addresses: []string{"0x1234"}, TargetShard: 1, EffectiveEpoch: 2},
		{ProposalID: "bad-shard", Addresses: []string{authorityAccount('1')}, TargetShard: 2, EffectiveEpoch: 2},
		{ProposalID: "bad-epoch", Addresses: []string{authorityAccount('1')}, TargetShard: 1, EffectiveEpoch: 1},
		{ProposalID: "future-base", Addresses: []string{authorityAccount('1')}, TargetShard: 1, BaseRouteVersion: 1, EffectiveEpoch: 2},
	}
	for _, proposal := range tests {
		if got := a.submit(proposal, 1); got.Status != mappingRejected {
			t.Fatalf("proposal %+v status=%s", proposal, got.Status)
		}
	}
}

func TestMappingAuthorityDigestIsInsertionOrderIndependent(t *testing.T) {
	left, right := authorityAccount('1'), authorityAccount('2')
	a := newMappingAuthority(4, map[string]uint64{left: 1, right: 2})
	b := newMappingAuthority(4, map[string]uint64{right: 2, left: 1})
	if a.stateDigest() != b.stateDigest() {
		t.Fatalf("digest differs: %s != %s", a.stateDigest(), b.stateDigest())
	}
}

func TestMappingAuthorityRequiresAllInstallationReceipts(t *testing.T) {
	a := newMappingAuthority(4, nil)
	a.enableInstallationReceiptGate()
	left, right := authorityAccount('1'), authorityAccount('2')
	p := mappingAuthorityProposal{ProposalID: "multi", Addresses: []string{left, right}, SourceShard: 0, TargetShard: 2, BaseRouteVersion: 0, EffectiveEpoch: 2}
	if a.submit(p, 1).Status != mappingAccepted {
		t.Fatal("submit failed")
	}
	leftReceipt := prepareReceiptForTest(t, a, "multi", left, 2)
	rightReceipt := prepareReceiptForTest(t, a, "multi", right, 2)
	a.recordInstallationReceiptMeta(leftReceipt)
	if got := a.applyDue(2); len(got) != 1 || got[0].Reason != "state_installation_receipt_incomplete" {
		t.Fatalf("partial receipt: %+v", got)
	}
	a.recordInstallationReceiptMeta(rightReceipt)
	got := a.applyDue(2)
	if len(got) != 1 || got[0].Status != mappingApplied {
		t.Fatalf("complete receipts: %+v", got)
	}
	if !a.canFinalizeMigration("multi", left, leftReceipt.StateHash) || !a.canFinalizeMigration("multi", right, rightReceipt.StateHash) {
		t.Fatal("per-account receipt lost after second account")
	}
}
