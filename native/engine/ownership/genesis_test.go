package ownership

import (
	"reflect"
	"sync"
	"testing"
)

func TestSharedGenesisIsPrivateAndDeltaOnly(t *testing.T) {
	f := newCommitteeFixture(t)
	base, err := NewGenesis(f.genesis, f.verifier.Context().Shards)
	if err != nil {
		t.Fatal(err)
	}
	view, err := newViewWithGenesis(f.verifier, base)
	if err != nil || view.genesis != base || len(view.owners) != 0 {
		t.Fatalf("base was not shared or genesis was copied into delta: %v", err)
	}
	delete(f.genesis, testAccount(1000))
	f.genesis[testAccount(1)] = 0
	if view.Check(testAccount(1000), 0) != nil || view.Check(testAccount(1), 0) == nil {
		t.Fatal("caller mutated the private genesis index")
	}
	cert := f.certificate(t, 0, "one-delta", Binding{testAccount(1), 1})
	checked, digest, err := f.verifier.Verify(cert)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := view.apply(checked, digest); err != nil || !changed {
		t.Fatalf("valid addition failed: %v", err)
	}
	if len(view.owners) != 1 || base.Len() != 1 || view.Check(testAccount(1), 1) != nil {
		t.Fatal("addition changed base or logical ownership")
	}
	if _, exists := base.Owner(testAccount(1)); exists {
		t.Fatal("new owner contaminated historical base")
	}
	if changed, err := view.apply(checked, digest); err != nil || changed {
		t.Fatalf("idempotence changed: %v", err)
	}
	conflict := f.certificate(t, 1, "rebind-genesis", Binding{testAccount(2), 0}, Binding{testAccount(1000), 1})
	checked, digest, err = f.verifier.Verify(conflict)
	if err != nil {
		t.Fatal(err)
	}
	before := view.ownerSnapshot()
	if changed, err := view.apply(checked, digest); err == nil || changed || !reflect.DeepEqual(before, view.ownerSnapshot()) || view.Version() != 1 {
		t.Fatal("genesis conflict did not reject the whole transition")
	}
	status := view.Status()
	if status.Owners != 2 || status.GenesisOwners != 1 || status.CertifiedOwners != 1 {
		t.Fatalf("logical status changed: %+v", status)
	}
	if local, total := view.PartitionCounts(0); local != 1 || total != 2 {
		t.Fatalf("logical partition counts differ: %d/%d", local, total)
	}
}

func TestSharedGenesisReadersAndIndependentViews(t *testing.T) {
	f := newCommitteeFixture(t)
	base, err := NewGenesis(f.genesis, 2)
	if err != nil {
		t.Fatal(err)
	}
	left, err := newViewWithGenesis(f.verifier, base)
	if err != nil {
		t.Fatal(err)
	}
	right, err := newViewWithGenesis(f.verifier, base)
	if err != nil {
		t.Fatal(err)
	}
	cert := f.certificate(t, 0, "left-only", Binding{testAccount(1), 1})
	checked, digest, err := f.verifier.Verify(cert)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if left.Check(testAccount(1000), 0) != nil || right.Check(testAccount(1000), 0) != nil {
					t.Error("immutable base changed while adding a different account")
				}
			}
		}()
	}
	left.mu.Lock()
	_, err = left.apply(checked, digest)
	left.mu.Unlock()
	wg.Wait()
	if err != nil || right.Check(testAccount(1), 1) == nil || right.Version() != 0 {
		t.Fatal("independent views share mutable additions")
	}
}

func TestSharedGenesisRejectsWrongIdentity(t *testing.T) {
	f := newCommitteeFixture(t)
	for _, routes := range []map[string]uint64{nil, {testAccount(1000): 1}} {
		base, err := NewGenesis(routes, 2)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := newViewWithGenesis(f.verifier, base); err == nil {
			t.Fatal("wrong genesis accepted")
		}
	}
	base, err := NewGenesis(f.genesis, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newViewWithGenesis(f.verifier, base); err == nil {
		t.Fatal("wrong topology accepted")
	}
	if _, err := newViewWithGenesis(f.verifier, nil); err == nil {
		t.Fatal("nil genesis accepted")
	}
}
