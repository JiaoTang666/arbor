package committee

import "testing"

func TestMappingAuthorityActivationTiming(t *testing.T) {
	a := newMappingAuthority(2, nil)
	p := mappingAuthorityProposal{ProposalID: "timing", Addresses: []string{authorityAccount('1')}, TargetShard: 1, EffectiveEpoch: 3}
	accepted := a.submit(p, 1)
	if accepted.SubmittedEpoch != 1 || accepted.AppliedEpoch != nil || accepted.RouteActivationDelayEpochs != nil {
		t.Fatalf("premature activation evidence: %+v", accepted)
	}
	decisions := a.applyDue(5)
	if len(decisions) != 1 {
		t.Fatal(decisions)
	}
	applied := decisions[0]
	if applied.AppliedEpoch == nil || *applied.AppliedEpoch != 5 || applied.RouteActivationDelayEpochs == nil || *applied.RouteActivationDelayEpochs != 4 {
		t.Fatalf("actual activation timing not recorded: %+v", applied)
	}
	duplicate := a.submit(p, 6)
	if duplicate.SubmittedEpoch != 1 || duplicate.AppliedEpoch == nil || *duplicate.AppliedEpoch != 5 || a.routeVersion != 1 {
		t.Fatalf("duplicate changed activation evidence: %+v", duplicate)
	}
}

func TestMappingAuthorityNoopHasNoActivationTiming(t *testing.T) {
	account := authorityAccount('2')
	a := newMappingAuthority(2, map[string]uint64{account: 1})
	a.submit(mappingAuthorityProposal{ProposalID: "keep", Addresses: []string{account}, TargetShard: 1, EffectiveEpoch: 2}, 1)
	d := a.applyDue(2)[0]
	if d.Status != mappingNoop || d.AppliedEpoch != nil || d.RouteActivationDelayEpochs != nil || a.routeVersion != 0 {
		t.Fatalf("noop misreported as activation: %+v", d)
	}
}
