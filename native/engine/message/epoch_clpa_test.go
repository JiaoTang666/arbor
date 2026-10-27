package message

import "testing"

func TestCLPAEpochPlanCanonicalBinding(t *testing.T) {
	p := CLPAEpochPlan{Epoch: 1, Mapping: map[string]uint64{"0000000000000000000000000000000000000001": 0}, InputCuts: map[uint64]uint64{0: 3, 1: 0}}
	p.PlanHash = p.Digest()
	if err := p.Validate(2); err != nil {
		t.Fatal(err)
	}
	p.InputCuts[0]++
	if p.Validate(2) == nil {
		t.Fatal("modified input boundary accepted")
	}
	p.PlanHash = p.Digest()
	p.Epoch = 3
	p.PlanHash = p.Digest()
	if p.Validate(2) == nil {
		t.Fatal("skipped generation accepted")
	}
	p.Epoch = 1
	delete(p.InputCuts, 1)
	p.InputCuts[2] = 0
	p.PlanHash = p.Digest()
	if p.Validate(2) == nil {
		t.Fatal("foreign shard replaced missing participant")
	}
}
