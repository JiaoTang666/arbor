package ownership

import (
	"path/filepath"
	"testing"
)

func witnessedBatch(t *testing.T, f *committeeFixture, base uint64, proposal string, start uint64, transactions []PlacementTransaction, bindings ...Binding) Batch {
	t.Helper()
	batch := f.certificate(t, base, proposal, bindings...).Batch
	batch.EvidenceVersion = 1
	batch.EvidenceDigest = digestBytes([]byte("historical evidence"))
	batch.EvidenceCutoff = start
	batch.AdmissionStart, batch.AdmissionEnd = start, start+uint64(len(transactions))
	batch.PlacementWitness = &PlacementWitness{PlacementWitnessSchema, PlacementInputDigest(transactions), transactions}
	return batch
}

func TestPlacementWitnessOrderedNOAndNNValidateIndependently(t *testing.T) {
	f := newCommitteeFixture(t)
	s, err := OpenSignerWithPolicy(filepath.Join(t.TempDir(), "member.db"), f.verifier, f.genesis, "member-0", f.private["member-0"], RegistryCertificateMode, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	batch := witnessedBatch(t, f, 0, "ordered", 0, []PlacementTransaction{
		{"nn", 0, testAccount(1), testAccount(2), 1, 1},
		{"no", 1, testAccount(2), testAccount(3), 1, 1},
		{"oo", 2, testAccount(1000), testAccount(1), 0, 1},
	}, Binding{testAccount(1), 1}, Binding{testAccount(2), 1}, Binding{testAccount(3), 1})
	if _, err := s.Vote(batch); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(f.sign(t, batch)); err != nil {
		t.Fatal(err)
	}
	view := requireSignerSnapshot(t, s, f, 1)
	if view.Check(testAccount(3), 1) != nil {
		t.Fatal("ordered in-batch old owner not followed")
	}
}

func TestPlacementWitnessRejectsInvalidContractWithoutConsumingSlot(t *testing.T) {
	for _, name := range []string{"missing", "NO-follow", "OO-rewrite", "NN-colocation", "binding-coverage", "input-digest", "sequence", "duplicate-occurrence"} {
		t.Run(name, func(t *testing.T) {
			f := newCommitteeFixture(t)
			s, err := OpenSignerWithPolicy(filepath.Join(t.TempDir(), "member.db"), f.verifier, f.genesis, "member-0", f.private["member-0"], RegistryCertificateMode, true)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			good := witnessedBatch(t, f, 0, "good", 1, []PlacementTransaction{
				{"one", 1, testAccount(1000), testAccount(1), 0, 0},
				{"two", 2, testAccount(1), testAccount(2), 0, 0},
			}, Binding{testAccount(1), 0}, Binding{testAccount(2), 0})
			bad := cloneCertificate(t, f.sign(t, good)).Batch
			switch name {
			case "missing":
				bad.PlacementWitness = nil
			case "NO-follow":
				bad.PlacementWitness.Transactions[0].RecipientShard = 1
				bad.Bindings[0].Shard = 1
			case "OO-rewrite":
				bad.PlacementWitness.Transactions[0].SenderShard = 1
			case "NN-colocation":
				bad.PlacementWitness.Transactions[0].Sender = testAccount(3)
				bad.PlacementWitness.Transactions[0].RecipientShard = 1
				bad.PlacementWitness.InputDigest = PlacementInputDigest(bad.PlacementWitness.Transactions)
			case "binding-coverage":
				bad.Bindings = append(bad.Bindings, Binding{testAccount(3), 0})
			case "input-digest":
				bad.PlacementWitness.InputDigest = digestBytes([]byte("wrong"))
			case "sequence":
				bad.PlacementWitness.Transactions[1].Sequence = 7
				bad.PlacementWitness.InputDigest = PlacementInputDigest(bad.PlacementWitness.Transactions)
			case "duplicate-occurrence":
				bad.PlacementWitness.Transactions[1].OccurrenceID = "one"
				bad.PlacementWitness.InputDigest = PlacementInputDigest(bad.PlacementWitness.Transactions)
			}
			requireRejectedSignerVote(t, s, bad)
			// Even a mathematically valid quorum made by fixture private keys is
			// not sufficient to bypass this member's independent placement rule.
			if changed, err := s.Commit(f.sign(t, bad)); err == nil || changed {
				t.Fatal("invalid witness installed via catch-up")
			}
			requireSignerVote(t, f, s, good, "member-0")
		})
	}
}

func TestPlacementPolicyPinnedAcrossRecoveryAndCannotDowngrade(t *testing.T) {
	f := newCommitteeFixture(t)
	path := filepath.Join(t.TempDir(), "member.db")
	s, err := OpenSignerWithPolicy(path, f.verifier, f.genesis, "member-0", f.private["member-0"], RegistryCertificateMode, true)
	if err != nil {
		t.Fatal(err)
	}
	batch := witnessedBatch(t, f, 0, "one", 1, []PlacementTransaction{
		{"first", 1, testAccount(1000), testAccount(1), 0, 0},
	}, Binding{testAccount(1), 0})
	requireSignerVote(t, f, s, batch, "member-0")
	if _, err := s.Commit(f.sign(t, batch)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if downgraded, err := OpenSignerWithMode(path, f.verifier, f.genesis, "member-0", f.private["member-0"], RegistryCertificateMode); err == nil {
		downgraded.Close()
		t.Fatal("strict placement store silently downgraded")
	}
	reopened, err := OpenSignerWithPolicy(path, f.verifier, f.genesis, "member-0", f.private["member-0"], RegistryCertificateMode, true)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	requireSignerSnapshot(t, reopened, f, 1)
	requireSignerVote(t, f, reopened, batch, "member-0")
}
