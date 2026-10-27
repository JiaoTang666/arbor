package ownership

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

func TestAdvanceVoteInstallsOnlyParentAndReplaysCanonicalQC(t *testing.T) {
	f := newCommitteeFixture(t)
	s, _ := openFixtureSigner(t, f, "member-0")
	parent := f.certificate(t, 0, "parent", Binding{testAccount(1), 1})
	next := f.certificate(t, 1, "next", Binding{testAccount(2), 0}).Batch
	sig, err := s.AdvanceVote([]Certificate{parent}, next)
	if err != nil {
		t.Fatal(err)
	}
	view := requireSignerSnapshot(t, s, f, 1)
	if view.Check(testAccount(1), 1) != nil {
		t.Fatal("parent owner absent")
	}
	if _, exists := view.Owner(testAccount(2)); exists {
		t.Fatal("uncommitted next vote published owner")
	}
	variant := f.sign(t, parent.Batch, "member-3", "member-2", "member-1")
	replayed, err := s.AdvanceVote([]Certificate{variant}, next)
	if err != nil || !bytes.Equal(sig.Value, replayed.Value) {
		t.Fatalf("canonical parent subset replay: %v", err)
	}
	conflict := f.certificate(t, 1, "conflict", Binding{testAccount(2), 1}).Batch
	if result, err := s.AdvanceVote([]Certificate{variant}, conflict); err == nil || len(result.Value) != 0 {
		t.Fatal("fused retry unlocked a conflicting slot")
	}
}

func TestAdvanceVoteRejectsWholeInvalidTransitionWithoutFencing(t *testing.T) {
	f := newCommitteeFixture(t)
	s, _ := openFixtureSigner(t, f, "member-0")
	parent := f.certificate(t, 0, "parent", Binding{testAccount(1), 1})
	invalid := f.certificate(t, 1, "invalid", Binding{testAccount(1), 0}).Batch
	if sig, err := s.AdvanceVote([]Certificate{parent}, invalid); err == nil || len(sig.Value) != 0 {
		t.Fatal("invalid next binding accepted")
	}
	if s.registry.fenced {
		t.Fatal("contract rejection unnecessarily fenced signer")
	}
	view := requireSignerSnapshot(t, s, f, 0)
	if _, exists := view.Owner(testAccount(1)); exists {
		t.Fatal("invalid transition partially installed parent")
	}
	valid := f.certificate(t, 1, "valid", Binding{testAccount(2), 0}).Batch
	if _, err := s.AdvanceVote([]Certificate{parent}, valid); err != nil {
		t.Fatal(err)
	}
}

func TestAdvanceVoteCatchupOrderedAndAtomic(t *testing.T) {
	f := newCommitteeFixture(t)
	s, _ := openFixtureSigner(t, f, "member-3")
	first := f.certificate(t, 0, "one", Binding{testAccount(1), 0})
	second := f.certificate(t, 1, "two", Binding{testAccount(2), 1})
	third := f.certificate(t, 2, "three", Binding{testAccount(3), 0}).Batch
	if _, err := s.AdvanceVote([]Certificate{second, first}, third); err == nil {
		t.Fatal("gap/reordered catch-up accepted")
	}
	requireSignerSnapshot(t, s, f, 0)
	if _, err := s.AdvanceVote([]Certificate{first, second}, third); err != nil {
		t.Fatal(err)
	}
	view := requireSignerSnapshot(t, s, f, 2)
	if view.Check(testAccount(1), 0) != nil || view.Check(testAccount(2), 1) != nil {
		t.Fatal("catch-up missing owners")
	}
}

func TestAdvanceVoteDurabilityFaultsRecoverTogether(t *testing.T) {
	for _, after := range []bool{false, true} {
		name := "before-commit"
		if after {
			name = "after-commit"
		}
		t.Run(name, func(t *testing.T) {
			f := newCommitteeFixture(t)
			s, path := openFixtureSigner(t, f, "member-0")
			parent := f.certificate(t, 0, "parent", Binding{testAccount(1), 1})
			next := f.certificate(t, 1, "next", Binding{testAccount(2), 0}).Batch
			fault := func() error { return errors.New("fused durability fault") }
			if after {
				s.afterVoteCommit = fault
			} else {
				s.beforeVoteCommit = fault
			}
			if sig, err := s.AdvanceVote([]Certificate{parent}, next); err == nil || len(sig.Value) != 0 {
				t.Fatal("fault disclosed signature")
			}
			if !s.registry.fenced {
				t.Fatal("uncertain transition not fenced")
			}
			if _, exists := s.registry.ownerCache[testAccount(1)]; exists {
				t.Fatal("uncertain transition published cache")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenSigner(path, f.verifier, f.genesis, "member-0", f.private["member-0"])
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			version := uint64(0)
			if after {
				version = 1
			}
			requireSignerSnapshot(t, reopened, f, version)
			if _, err := reopened.AdvanceVote([]Certificate{parent}, next); err != nil {
				t.Fatal(err)
			}
			conflict := f.certificate(t, 1, "conflict", Binding{testAccount(2), 1}).Batch
			requireRejectedSignerVote(t, reopened, conflict)
		})
	}
}

func TestAdvanceVoteStrictParentAndNextPlacementValidation(t *testing.T) {
	f := newCommitteeFixture(t)
	s, err := OpenSignerWithPolicy(filepath.Join(t.TempDir(), "member.db"), f.verifier, f.genesis, "member-0", f.private["member-0"], RegistryCertificateMode, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first := witnessedBatch(t, f, 0, "first", 1, []PlacementTransaction{
		{"first", 1, testAccount(1000), testAccount(1), 0, 0},
	}, Binding{testAccount(1), 0})
	parent := f.sign(t, first)
	next := witnessedBatch(t, f, 1, "next", 2, []PlacementTransaction{
		{"second", 2, testAccount(1), testAccount(2), 0, 0},
	}, Binding{testAccount(2), 0})
	bad := cloneCertificate(t, f.sign(t, next)).Batch
	bad.PlacementWitness.Transactions[0].RecipientShard = 1
	bad.Bindings[0].Shard = 1
	if _, err := s.AdvanceVote([]Certificate{parent}, bad); err == nil {
		t.Fatal("bad next NO follow accepted")
	}
	requireSignerSnapshot(t, s, f, 0)
	if _, err := s.AdvanceVote([]Certificate{parent}, next); err != nil {
		t.Fatal(err)
	}
}
