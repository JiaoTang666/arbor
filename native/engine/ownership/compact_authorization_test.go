package ownership

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func compactFixture(t *testing.T) *committeeFixture {
	t.Helper()
	f := newCommitteeFixture(t)
	f.config.Context.AuthorizationSchema = CompactAuthorizationSchema
	var err error
	f.verifier, err = NewVerifier(f.config)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func compactBatch(f *committeeFixture, base, start uint64, proposal string, transactions []PlacementTransaction, bindings ...Binding) Batch {
	batch := Batch{Schema: Schema, Context: f.config.Context, CommitteeDigest: f.verifier.CommitteeDigest(),
		BaseVersion: base, ProposalID: proposal, Bindings: bindings, EvidenceVersion: 1,
		EvidenceDigest: digestBytes([]byte("historical evidence")), EvidenceCutoff: start,
		AdmissionStart: start, AdmissionEnd: start + uint64(len(transactions)),
		PlacementWitness: &PlacementWitness{PlacementWitnessSchema, PlacementInputDigest(transactions), transactions}}
	batch.PlacementWitnessDigest = PlacementWitnessDigest(batch.PlacementWitness)
	return batch
}

func compactNO(f *committeeFixture, base, start, old, new uint64, proposal string) Batch {
	return compactBatch(f, base, start, proposal, []PlacementTransaction{
		{proposal, start, testAccount(old), testAccount(new), 0, 0},
	}, Binding{testAccount(new), 0})
}

func publicCompact(cert Certificate) Certificate {
	cert.Batch.PlacementWitness = nil
	return cert
}

func openCompactSigner(t *testing.T, f *committeeFixture, path, member string) *Signer {
	t.Helper()
	s, err := OpenSignerWithPolicy(path, f.verifier, f.genesis, member, f.private[member], RegistryCertificateMode, true)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestCompactAuthorizationCommitmentIsolationAndDomain(t *testing.T) {
	f := compactFixture(t)
	batch := compactNO(f, 0, 0, 1000, 1, "first")
	full := f.sign(t, batch)
	compact := publicCompact(full)
	left, _ := SigningBytes(full.Batch)
	right, _ := SigningBytes(compact.Batch)
	if !bytes.Equal(left, right) {
		t.Fatal("full and compact representations sign different facts")
	}
	checked, _, err := f.verifier.Verify(full)
	if err != nil {
		t.Fatal(err)
	}
	checked.Batch.PlacementWitness.Transactions[0].RecipientShard = 1
	checked.Batch.Bindings[0].Shard = 1
	checked.Signatures[0].Value[0] ^= 1
	if _, _, err := f.verifier.Verify(full); err != nil {
		t.Fatal("verified copy aliases the untrusted certificate")
	}
	for _, edit := range []func(*Certificate){
		func(c *Certificate) { c.Batch.PlacementWitness.Transactions[0].RecipientShard = 1 },
		func(c *Certificate) { c.Batch.PlacementWitnessDigest = digestBytes([]byte("wrong witness")) },
		func(c *Certificate) { c.Batch.Context.AuthorizationSchema = "" },
		func(c *Certificate) { c.Signatures = c.Signatures[:2] },
	} {
		bad := cloneCertificate(t, full)
		edit(&bad)
		if _, _, err := f.verifier.Verify(bad); err == nil {
			t.Fatal("tamper, downgrade or insufficient quorum accepted")
		}
	}
	if _, _, err := f.verifier.Verify(compact); err != nil {
		t.Fatal(err)
	}
	if legacy, err := OpenSignerWithPolicy(filepath.Join(t.TempDir(), "bad.db"), f.verifier, f.genesis, "member-0", f.private["member-0"], RegistryCertificateMode, false); err == nil {
		legacy.Close()
		t.Fatal("compact signer opened without pinned independent placement")
	}
}

func TestCompactAdvanceVoteAndRecoveryPreserveFullVoteEvidence(t *testing.T) {
	f := compactFixture(t)
	path := filepath.Join(t.TempDir(), "member.db")
	s := openCompactSigner(t, f, path, "member-0")
	first := compactNO(f, 0, 0, 1000, 1, "first")
	requireSignerVote(t, f, s, first, "member-0")
	next := compactNO(f, 1, 1, 1, 2, "next")
	parent := publicCompact(f.sign(t, first))
	if _, err := s.AdvanceVote([]Certificate{parent}, next); err != nil {
		t.Fatal(err)
	}
	requireSignerSnapshot(t, s, f, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openCompactSigner(t, f, path, "member-0")
	defer s.Close()
	requireSignerSnapshot(t, s, f, 1)
	requireSignerVote(t, f, s, next, "member-0")
	if _, err := s.Commit(publicCompact(f.sign(t, next))); err != nil {
		t.Fatal(err)
	}
	requireSignerSnapshot(t, s, f, 2)
	conflict := cloneBatch(next)
	conflict.ProposalID = "conflict"
	requireRejectedSignerVote(t, s, conflict)
	missing := cloneBatch(next)
	missing.PlacementWitness = nil
	requireRejectedSignerVote(t, s, missing)
}

func TestCompactParentRequiresOwnIncludedVoteOrFullWitness(t *testing.T) {
	for _, member := range []string{"member-0", "member-3"} {
		t.Run(member, func(t *testing.T) {
			f := compactFixture(t)
			path := filepath.Join(t.TempDir(), "member.db")
			s := openCompactSigner(t, f, path, member)
			first := compactNO(f, 0, 0, 1000, 1, "first")
			if member == "member-3" {
				// A same-payload vote omitted from the QC is not blindly reused.
				requireSignerVote(t, f, s, first, member)
			}
			full := f.sign(t, first)
			if changed, err := s.Commit(publicCompact(full)); err == nil || changed {
				t.Fatal("missing or nonincluded private vote bypassed the full witness")
			}
			requireSignerSnapshot(t, s, f, 0)
			bad := cloneCertificate(t, full)
			bad.Batch.PlacementWitness.Transactions[0].SenderShard = 1
			bad.Batch.PlacementWitnessDigest = PlacementWitnessDigest(bad.Batch.PlacementWitness)
			if changed, err := s.Commit(f.sign(t, bad.Batch)); err == nil || changed {
				t.Fatal("valid signatures bypassed independent OO/NO contract")
			}
			if _, err := s.Commit(full); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openCompactSigner(t, f, path, member)
			defer s.Close()
			requireSignerSnapshot(t, s, f, 1)
		})
	}
}

func TestCompactRecoveryRejectsMissingOrTamperedWitness(t *testing.T) {
	for _, missing := range []bool{false, true} {
		f := compactFixture(t)
		path := filepath.Join(t.TempDir(), "member.db")
		s := openCompactSigner(t, f, path, "member-0")
		batch := compactNO(f, 0, 0, 1000, 1, "first")
		requireSignerVote(t, f, s, batch, "member-0")
		if _, err := s.Commit(publicCompact(f.sign(t, batch))); err != nil {
			t.Fatal(err)
		}
		s.Close()
		db, err := bolt.Open(path, 0600, nil)
		if err != nil {
			t.Fatal(err)
		}
		err = db.Update(func(tx *bolt.Tx) error {
			votes := tx.Bucket(signerVoteBucket)
			vote, err := decodeVote(votes.Get(voteKey(0)))
			if err != nil {
				return err
			}
			if missing {
				vote.Batch.PlacementWitness = nil
			} else {
				vote.Batch.PlacementWitness.Transactions[0].RecipientShard = 1
			}
			raw, err := json.Marshal(vote)
			if err != nil {
				return err
			}
			return votes.Put(voteKey(0), raw)
		})
		db.Close()
		if err != nil {
			t.Fatal(err)
		}
		if reopened, err := OpenSignerWithPolicy(path, f.verifier, f.genesis, "member-0", f.private["member-0"], RegistryCertificateMode, true); err == nil {
			reopened.Close()
			t.Fatal("compact recovery accepted missing or changed full vote evidence")
		}
	}
}

func TestCompactAuthorizationCrossLanguageVector(t *testing.T) {
	f := newCommitteeFixture(t)
	f.config.Context.AuthorizationSchema = CompactAuthorizationSchema
	f.config.Members = nil
	for i := 0; i < 4; i++ {
		seed := bytes.Repeat([]byte{byte(i + 1)}, ed25519.SeedSize)
		key := ed25519.NewKeyFromSeed(seed)
		id := "member-" + string(rune('0'+i))
		f.private[id] = key
		f.config.Members = append(f.config.Members, Member{id, key.Public().(ed25519.PublicKey)})
	}
	var err error
	f.verifier, err = NewVerifier(f.config)
	if err != nil {
		t.Fatal(err)
	}
	batch := compactNO(f, 0, 0, 1000, 1, "vector-first")
	signed, _ := SigningBytes(batch)
	vector := struct {
		Config        Config      `json:"config"`
		Batch         Batch       `json:"batch"`
		WitnessDigest string      `json:"witness_digest"`
		SigningDigest string      `json:"signing_digest"`
		Certificate   Certificate `json:"certificate"`
	}{f.config, batch, batch.PlacementWitnessDigest, hex.EncodeToString(signed), publicCompact(f.sign(t, batch))}
	raw, _ := json.Marshal(vector)
	t.Logf("COMPACT_VECTOR %s", raw)
}

func TestCompactOrderedNNOOAndNOStillValidated(t *testing.T) {
	f := compactFixture(t)
	s := openCompactSigner(t, f, filepath.Join(t.TempDir(), "member.db"), "member-0")
	defer s.Close()
	good := compactBatch(f, 0, 0, "ordered", []PlacementTransaction{
		{"nn", 0, testAccount(1), testAccount(2), 1, 1},
		{"no", 1, testAccount(2), testAccount(3), 1, 1},
		{"oo", 2, testAccount(1000), testAccount(1), 0, 1},
	}, Binding{testAccount(1), 1}, Binding{testAccount(2), 1}, Binding{testAccount(3), 1})
	for _, index := range []int{0, 1, 2} {
		bad := cloneBatch(good)
		if index == 2 {
			bad.PlacementWitness.Transactions[index].SenderShard = 1
		} else {
			bad.PlacementWitness.Transactions[index].RecipientShard = 0
		}
		// A consistent digest is not a substitute for independent policy checks.
		bad.PlacementWitnessDigest = PlacementWitnessDigest(bad.PlacementWitness)
		requireRejectedSignerVote(t, s, bad)
	}
	requireSignerVote(t, f, s, good, "member-0")
}

func TestCompactAdvanceVoteDurabilityFaultsRecoverTogether(t *testing.T) {
	for _, after := range []bool{false, true} {
		f := compactFixture(t)
		path := filepath.Join(t.TempDir(), "member.db")
		s := openCompactSigner(t, f, path, "member-0")
		first := compactNO(f, 0, 0, 1000, 1, "first")
		requireSignerVote(t, f, s, first, "member-0")
		parent := publicCompact(f.sign(t, first))
		next := compactNO(f, 1, 1, 1, 2, "next")
		fault := func() error { return errors.New("compact fused persistence fault") }
		if after {
			s.afterVoteCommit = fault
		} else {
			s.beforeVoteCommit = fault
		}
		if signature, err := s.AdvanceVote([]Certificate{parent}, next); err == nil || len(signature.Value) != 0 || !s.registry.fenced {
			t.Fatal("uncertain commit disclosed a signature or continued unfenced")
		}
		s.Close()
		s = openCompactSigner(t, f, path, "member-0")
		version := uint64(0)
		if after {
			version = 1
		}
		requireSignerSnapshot(t, s, f, version)
		if _, err := s.AdvanceVote([]Certificate{parent}, next); err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
}
