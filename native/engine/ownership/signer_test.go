package ownership

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func openFixtureSigner(t *testing.T, f *committeeFixture, member string) (*Signer, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "signer.db")
	s, err := OpenSigner(path, f.verifier, f.genesis, member, f.private[member])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, path
}

func requireSignerVote(t *testing.T, f *committeeFixture, s *Signer, batch Batch, member string) Signature {
	t.Helper()
	sig, err := s.Vote(batch)
	if err != nil {
		t.Fatalf("vote: %v", err)
	}
	raw, err := SigningBytes(batch)
	if err != nil {
		t.Fatal(err)
	}
	public := f.private[member].Public().(ed25519.PublicKey)
	if sig.MemberID != member || !ed25519.Verify(public, raw, sig.Value) {
		t.Fatal("vote did not return this member's valid payload-bound signature")
	}
	return sig
}

func requireRejectedSignerVote(t *testing.T, s *Signer, batch Batch) {
	t.Helper()
	sig, err := s.Vote(batch)
	if err == nil || len(sig.Value) != 0 {
		t.Fatalf("rejected vote disclosed a signature or success: signature bytes=%d err=%v", len(sig.Value), err)
	}
}

func requireSignerSnapshot(t *testing.T, s *Signer, f *committeeFixture, version uint64) *View {
	t.Helper()
	view, err := s.Snapshot(f.genesis)
	if err != nil || view == nil {
		t.Fatalf("snapshot: %v", err)
	}
	if view.Version() != version || view.Check(testAccount(1000), 0) != nil {
		t.Fatalf("snapshot version=%d want=%d or genesis ownership changed", view.Version(), version)
	}
	return view
}

func TestSignerConcurrentConflictHasOnlyOneDurableVote(t *testing.T) {
	f := newCommitteeFixture(t)
	s, path := openFixtureSigner(t, f, "member-0")
	batches := []Batch{
		f.certificate(t, 0, "left", Binding{testAccount(1), 0}).Batch,
		f.certificate(t, 0, "right", Binding{testAccount(1), 1}).Batch,
	}
	type result struct {
		index int
		sig   Signature
		err   error
	}
	start, results := make(chan struct{}), make(chan result, len(batches))
	for i, batch := range batches {
		go func(i int, batch Batch) {
			<-start
			sig, err := s.Vote(batch)
			results <- result{i, sig, err}
		}(i, batch)
	}
	close(start)
	winner, successes, rejected := -1, 0, 0
	for range batches {
		got := <-results
		if got.err == nil && len(got.sig.Value) == ed25519.SignatureSize {
			winner, successes = got.index, successes+1
		} else if got.err != nil && len(got.sig.Value) == 0 {
			rejected++
		} else {
			t.Fatalf("invalid concurrent outcome: %+v", got)
		}
	}
	if successes != 1 || rejected != 1 {
		t.Fatalf("conflict outcomes successful=%d rejected=%d", successes, rejected)
	}
	first := requireSignerVote(t, f, s, batches[winner], "member-0")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSigner(path, f.verifier, f.genesis, "member-0", f.private["member-0"])
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replay := requireSignerVote(t, f, reopened, batches[winner], "member-0")
	if !bytes.Equal(first.Value, replay.Value) {
		t.Fatal("reopen changed the stored vote signature")
	}
	requireRejectedSignerVote(t, reopened, batches[1-winner])
	// Voting is not a quorum commit and must not create an account owner.
	view := requireSignerSnapshot(t, reopened, f, 0)
	if _, exists := view.Owner(testAccount(1)); exists {
		t.Fatal("single-member vote changed committed ownership")
	}
}

func TestSignerOrderedCommitRequiredBeforeNextSlot(t *testing.T) {
	f := newCommitteeFixture(t)
	s, _ := openFixtureSigner(t, f, "member-0")
	first := f.certificate(t, 0, "first", Binding{testAccount(1), 1})
	second := f.certificate(t, 1, "second", Binding{testAccount(2), 0})
	requireRejectedSignerVote(t, s, second.Batch)
	if changed, err := s.Commit(second); err == nil || changed {
		t.Fatal("non-prefix quorum certificate committed")
	}
	insufficient := cloneCertificate(t, first)
	insufficient.Signatures = insufficient.Signatures[:2]
	if changed, err := s.Commit(insufficient); err == nil || changed {
		t.Fatal("minority signatures committed ownership")
	}
	requireSignerVote(t, f, s, first.Batch, "member-0")
	requireSignerSnapshot(t, s, f, 0)
	requireRejectedSignerVote(t, s, second.Batch)
	if changed, err := s.Commit(first); err != nil || !changed {
		t.Fatalf("first quorum commit: changed=%v err=%v", changed, err)
	}
	if changed, err := s.Commit(first); err != nil || changed {
		t.Fatalf("same quorum commit replay: changed=%v err=%v", changed, err)
	}
	requireSignerVote(t, f, s, second.Batch, "member-0")
	if changed, err := s.Commit(second); err != nil || !changed {
		t.Fatalf("next prefix commit: changed=%v err=%v", changed, err)
	}
	view := requireSignerSnapshot(t, s, f, 2)
	if view.Check(testAccount(1), 1) != nil || view.Check(testAccount(2), 0) != nil {
		t.Fatal("prefix commit did not retain both first owners")
	}
}

func TestSignerCrossSlotWriteOnceAndContractRejectDoesNotConsumeVote(t *testing.T) {
	f := newCommitteeFixture(t)
	s, _ := openFixtureSigner(t, f, "member-0")
	requireRejectedSignerVote(t, s, f.certificate(t, 0, "genesis-rewrite", Binding{testAccount(1000), 1}).Batch)
	first := f.certificate(t, 0, "first", Binding{testAccount(1), 1})
	requireSignerVote(t, f, s, first.Batch, "member-0")
	if changed, err := s.Commit(first); err != nil || !changed {
		t.Fatalf("first commit: %v %v", changed, err)
	}
	for _, target := range []uint64{0, 1} {
		repeated := f.certificate(t, 1, "rewrite", Binding{testAccount(1), target})
		requireRejectedSignerVote(t, s, repeated.Batch)
		if changed, err := s.Commit(repeated); err == nil || changed {
			t.Fatal("cross-slot repeated first-owner binding committed")
		}
	}
	reusedProposal := f.certificate(t, 1, "first", Binding{testAccount(2), 0})
	requireRejectedSignerVote(t, s, reusedProposal.Batch)
	if changed, err := s.Commit(reusedProposal); err == nil || changed {
		t.Fatal("historical proposal ID was reused to commit an unrelated new owner")
	}
	// Invalid batches must not lock the slot or partially install their other item.
	mixed := f.certificate(t, 1, "mixed", Binding{testAccount(2), 0}, Binding{testAccount(1000), 1})
	requireRejectedSignerVote(t, s, mixed.Batch)
	second := f.certificate(t, 1, "second", Binding{testAccount(2), 0})
	requireSignerVote(t, f, s, second.Batch, "member-0")
	view := requireSignerSnapshot(t, s, f, 1)
	if _, exists := view.Owner(testAccount(2)); exists {
		t.Fatal("invalid batch or uncommitted vote published an owner")
	}
}

func TestSignerMinorityVoteCanLearnQuorumWithoutOverwritingItsVote(t *testing.T) {
	f := newCommitteeFixture(t)
	s, path := openFixtureSigner(t, f, "member-3")
	minority := f.certificate(t, 0, "minority", Binding{testAccount(1), 0}).Batch
	stored := requireSignerVote(t, f, s, minority, "member-3")
	winner := f.certificate(t, 0, "winner", Binding{testAccount(1), 1})
	if changed, err := s.Commit(winner); err != nil || !changed {
		t.Fatalf("minority voter rejected valid quorum certificate: %v %v", changed, err)
	}
	if requireSignerSnapshot(t, s, f, 1).Check(testAccount(1), 1) != nil {
		t.Fatal("minority voter did not learn the quorum's owner")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSigner(path, f.verifier, f.genesis, "member-3", f.private["member-3"])
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replay := requireSignerVote(t, f, reopened, minority, "member-3")
	if !bytes.Equal(stored.Value, replay.Value) {
		t.Fatal("learning a different quorum certificate overwrote the old vote")
	}
	requireRejectedSignerVote(t, reopened, winner.Batch)
	next := f.certificate(t, 1, "next", Binding{testAccount(2), 0})
	requireSignerVote(t, f, reopened, next.Batch, "member-3")
}

func TestSignerVoteDurabilityFaultsFenceAndRecover(t *testing.T) {
	for _, afterCommit := range []bool{false, true} {
		name := "before-commit"
		if afterCommit {
			name = "after-commit"
		}
		t.Run(name, func(t *testing.T) {
			f := newCommitteeFixture(t)
			s, path := openFixtureSigner(t, f, "member-0")
			fault := func() error { return errors.New("injected vote durability fault") }
			if afterCommit {
				s.afterVoteCommit = fault
			} else {
				s.beforeVoteCommit = fault
			}
			first := f.certificate(t, 0, "first", Binding{testAccount(1), 1}).Batch
			other := f.certificate(t, 0, "other", Binding{testAccount(1), 0}).Batch
			requireRejectedSignerVote(t, s, first)
			// An uncertain write cannot be followed by either a replay or another vote.
			requireRejectedSignerVote(t, s, first)
			requireRejectedSignerVote(t, s, other)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenSigner(path, f.verifier, f.genesis, "member-0", f.private["member-0"])
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if afterCommit {
				// Commit was durable despite its uncertain acknowledgement: only replay.
				requireSignerVote(t, f, reopened, first, "member-0")
				requireRejectedSignerVote(t, reopened, other)
			} else {
				// The transaction aborted: recovery must not leave a phantom vote lock.
				requireSignerVote(t, f, reopened, other, "member-0")
				requireRejectedSignerVote(t, reopened, first)
			}
			requireSignerSnapshot(t, reopened, f, 0)
		})
	}
}

func TestSignerRejectsWrongMemberPrivateKey(t *testing.T) {
	f := newCommitteeFixture(t)
	inconsistent := append(ed25519.PrivateKey(nil), f.private["member-0"]...)
	inconsistent[0] ^= 1 // Public() alone would still report member-0's public suffix.
	cases := []struct {
		name   string
		member string
		key    ed25519.PrivateKey
	}{
		{"another-member-key", "member-0", f.private["member-1"]},
		{"inconsistent-seed-public-key", "member-0", inconsistent},
		{"unknown-member", "outside", f.private["member-0"]},
		{"short-private-key", "member-0", []byte{1}},
		{"missing-private-key", "member-0", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := OpenSigner(filepath.Join(t.TempDir(), "signer.db"), f.verifier, f.genesis, tc.member, tc.key)
			if s != nil {
				s.Close()
			}
			if err == nil {
				t.Fatal("untrusted member signing identity accepted")
			}
		})
	}
}

func TestSignerMissingOrCorruptVoteBucketFailsRecovery(t *testing.T) {
	for _, name := range []string{"missing-bucket", "corrupt-vote", "missing-vote"} {
		t.Run(name, func(t *testing.T) {
			f := newCommitteeFixture(t)
			s, path := openFixtureSigner(t, f, "member-0")
			requireSignerVote(t, f, s, f.certificate(t, 0, "first", Binding{testAccount(1), 1}).Batch, "member-0")
			if err := s.registry.db.Update(func(tx *bolt.Tx) error {
				if name == "missing-bucket" {
					return tx.DeleteBucket(signerVoteBucket)
				}
				b := tx.Bucket(signerVoteBucket)
				if b == nil {
					return errors.New("missing vote bucket before corruption fixture")
				}
				key := voteKey(0)
				if b.Get(key) == nil {
					return errors.New("missing slot-zero vote after successful vote")
				}
				if name == "missing-vote" {
					return b.Delete(key)
				}
				return b.Put(key, []byte("corrupt-durable-vote"))
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenSigner(path, f.verifier, f.genesis, "member-0", f.private["member-0"])
			if reopened != nil {
				reopened.Close()
			}
			if err == nil {
				t.Fatal("missing or corrupt vote history silently reset signing authority")
			}
		})
	}
}
