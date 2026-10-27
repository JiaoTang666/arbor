package ownership

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"path/filepath"
	"testing"
)

func faultContractBatch(f *committeeFixture, proposal string, target uint64) Batch {
	return Batch{Schema: Schema, Context: f.config.Context, CommitteeDigest: f.verifier.CommitteeDigest(),
		BaseVersion: 0, ProposalID: proposal, Bindings: []Binding{{testAccount(1), target}}}
}

// Intentionally bypass Signer: a Byzantine member controls its private key and
// can sign both payloads despite the honest implementation's durable vote lock.
func byzantineKeyVote(t *testing.T, f *committeeFixture, member string, batch Batch) Signature {
	t.Helper()
	message, err := SigningBytes(batch)
	if err != nil {
		t.Fatal(err)
	}
	sig := Signature{MemberID: member, Value: ed25519.Sign(f.private[member], message)}
	if !ed25519.Verify(f.verifier.keys[member], message, sig.Value) {
		t.Fatal("Byzantine fixture did not produce a valid member signature")
	}
	return sig
}

func reopenFaultContractSigner(t *testing.T, f *committeeFixture, member string, s *Signer, path string) *Signer {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSigner(path, f.verifier, f.genesis, member, f.private[member])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	return reopened
}

// n=4, f=1, q=3: two quorums intersect in at least two identities, so one
// Byzantine member alone cannot supply their entire intersection. Exhaust all
// eight allocations of the three honest members' votes between two payloads.
// Each honest identity retains one original store; no rollback or key/store
// cloning is modeled. This is authorization safety, not a PBFT or liveness test.
func TestSignerOneByzantineEquivocationCannotProduceTwoQuorums(t *testing.T) {
	for allocation := 0; allocation < 8; allocation++ {
		t.Run(fmt.Sprintf("honest-allocation-%03b", allocation), func(t *testing.T) {
			f := newCommitteeFixture(t)
			if f.config.Faults != 1 || len(f.config.Members) != 4 || f.verifier.quorum != 3 {
				t.Fatal("fault contract requires n=4, f=1, q=3")
			}
			batches := [2]Batch{faultContractBatch(f, "equivocation-A", 0), faultContractBatch(f, "equivocation-B", 1)}
			certs := [2]Certificate{{Batch: batches[0]}, {Batch: batches[1]}}
			for which := range certs {
				certs[which].Signatures = []Signature{byzantineKeyVote(t, f, "member-3", batches[which])}
			}
			for i := 0; i < 3; i++ {
				member := fmt.Sprintf("member-%d", i)
				which := (allocation >> i) & 1
				s, path := openFixtureSigner(t, f, member)
				stored := requireSignerVote(t, f, s, batches[which], member)
				certs[which].Signatures = append(certs[which].Signatures, stored)
				s = reopenFaultContractSigner(t, f, member, s, path)
				if replay := requireSignerVote(t, f, s, batches[which], member); !bytes.Equal(stored.Value, replay.Value) {
					t.Fatal("reopen changed an honest member's durable signature")
				}
				requireRejectedSignerVote(t, s, batches[1-which])
				if _, exists := requireSignerSnapshot(t, s, f, 0).Owner(testAccount(1)); exists {
					t.Fatal("votes alone published first-owner authorization")
				}
			}
			winner, verified := -1, 0
			for which, cert := range certs {
				_, _, err := f.verifier.Verify(cert)
				if len(cert.Signatures) >= 3 {
					if err != nil {
						t.Fatalf("valid quorum %d rejected: %v", which, err)
					}
					winner, verified = which, verified+1
				} else if err == nil {
					t.Fatal("Byzantine plus minority signatures verified as a quorum")
				}
			}
			// Since the Byzantine member signs both and every honest member votes,
			// exactly one side meets q=3. A withholding adversary need not allow this.
			if verified != 1 {
				t.Fatalf("contradictory verified certificates=%d, want exactly one", verified)
			}
			loser := 1 - winner
			// Copying honest signatures from the other payload cannot fill the
			// missing quorum, even when the candidate has three distinct IDs.
			crossPayload := cloneCertificate(t, certs[loser])
			for _, sig := range certs[winner].Signatures[1:] {
				if len(crossPayload.Signatures) == 3 {
					break
				}
				crossPayload.Signatures = append(crossPayload.Signatures, sig)
			}
			if len(crossPayload.Signatures) != 3 {
				t.Fatal("cross-payload quorum attempt did not contain three identities")
			}
			if _, _, err := f.verifier.Verify(crossPayload); err == nil {
				t.Fatal("honest signature reused on the contradictory payload verified")
			}
			registry, err := Open(filepath.Join(t.TempDir(), "authorized.db"), f.verifier, f.genesis)
			if err != nil {
				t.Fatal(err)
			}
			defer registry.Close()
			if changed, err := registry.Apply(certs[loser]); err == nil || changed {
				t.Fatal("contradictory minority certificate authorized an owner")
			}
			if changed, err := registry.Apply(certs[winner]); err != nil || !changed {
				t.Fatalf("single valid quorum did not authorize its owner: changed=%v err=%v", changed, err)
			}
			if changed, err := registry.Apply(crossPayload); err == nil || changed {
				t.Fatal("cross-payload signature reuse overwrote authorized ownership")
			}
			view, err := registry.Snapshot(f.genesis)
			if err != nil || view.Version() != 1 || view.Check(testAccount(1), uint64(winner)) != nil || view.Check(testAccount(1), uint64(loser)) == nil {
				t.Fatalf("registry did not retain exactly the quorum's owner: %v", err)
			}
		})
	}
}

// Only a fresh uncontested slot is covered: one member supplies no vote, while
// the three available honest members sign and commit the same valid proposal.
// No coordinator, retry protocol, deadline or split-vote recovery is claimed.
func TestSignerOneUnavailableMemberFreshProposalGetsHonestQuorum(t *testing.T) {
	f := newCommitteeFixture(t)
	batch := faultContractBatch(f, "uncontested-with-member-3-unavailable", 1)
	cert := Certificate{Batch: batch}
	var signers [3]*Signer
	for i := range signers {
		member := fmt.Sprintf("member-%d", i)
		s, path := openFixtureSigner(t, f, member)
		stored := requireSignerVote(t, f, s, batch, member)
		signers[i] = reopenFaultContractSigner(t, f, member, s, path)
		if replay := requireSignerVote(t, f, signers[i], batch, member); !bytes.Equal(stored.Value, replay.Value) {
			t.Fatal("available honest member did not retain its vote")
		}
		cert.Signatures = append(cert.Signatures, stored)
	}
	// member-3 is never opened or asked for a signature in this scenario.
	if len(cert.Signatures) != 3 {
		t.Fatal("fresh proposal did not obtain the three honest signatures")
	}
	if _, _, err := f.verifier.Verify(cert); err != nil {
		t.Fatalf("three honest members did not form a valid authorization quorum: %v", err)
	}
	for _, s := range signers {
		if changed, err := s.Commit(cert); err != nil || !changed {
			t.Fatalf("available honest signer could not commit uncontested quorum: changed=%v err=%v", changed, err)
		}
		if requireSignerSnapshot(t, s, f, 1).Check(testAccount(1), 1) != nil {
			t.Fatal("available honest member did not authorize the agreed owner")
		}
	}
}

// Two Byzantine keys exceed the configured f=1 assumption. Both q=3
// certificates can then be authentic: their two-member intersection consists
// entirely of equivocators. This boundary example documents the trust model,
// not a failure under the supported one-Byzantine contract.
func TestSignerTwoByzantineKeysOutsideFaultModelCanProduceContradictoryQuorums(t *testing.T) {
	f := newCommitteeFixture(t)
	batches := [2]Batch{faultContractBatch(f, "outside-f1-A", 0), faultContractBatch(f, "outside-f1-B", 1)}
	for which, batch := range batches {
		member := fmt.Sprintf("member-%d", which)
		s, _ := openFixtureSigner(t, f, member)
		cert := Certificate{Batch: batch, Signatures: []Signature{
			requireSignerVote(t, f, s, batch, member),
			byzantineKeyVote(t, f, "member-2", batch),
			byzantineKeyVote(t, f, "member-3", batch),
		}}
		if _, _, err := f.verifier.Verify(cert); err != nil {
			t.Fatalf("outside-f=1 authentic quorum rejected: %v", err)
		}
		// Independent honest stores can learn different authentic certificates
		// when the fault bound is exceeded; each local registry still writes once.
		if changed, err := s.Commit(cert); err != nil || !changed {
			t.Fatalf("outside-f=1 authentic quorum commit: changed=%v err=%v", changed, err)
		}
		if requireSignerSnapshot(t, s, f, 1).Check(testAccount(1), uint64(which)) != nil {
			t.Fatal("outside-f=1 example did not install each contradictory owner")
		}
	}
}
