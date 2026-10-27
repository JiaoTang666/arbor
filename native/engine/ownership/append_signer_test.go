package ownership

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// These tests are independent of the append implementation and reuse only the
// existing certificate fixtures. No coordinator payload or native chain exists.
func openAppendFixture(t *testing.T, f *committeeFixture, path, member string) *AppendSigner {
	t.Helper()
	s, err := OpenAppendSignerWithPolicy(path, f.verifier, f.genesis, member, f.private[member], true)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func requireAppendVote(t *testing.T, f *committeeFixture, s *AppendSigner, batch Batch, member string) Signature {
	t.Helper()
	signature, err := s.Vote(batch)
	message, encodeErr := SigningBytes(batch)
	if err != nil || encodeErr != nil || signature.MemberID != member ||
		!ed25519.Verify(f.private[member].Public().(ed25519.PublicKey), message, signature.Value) {
		t.Fatalf("append vote did not return a valid payload-bound member signature: vote=%v encode=%v", err, encodeErr)
	}
	return signature
}

func requireAppendReject(t *testing.T, s *AppendSigner, batch Batch) {
	t.Helper()
	if signature, err := s.Vote(batch); err == nil || len(signature.Value) != 0 {
		t.Fatal("rejected append vote disclosed a signature or success")
	}
}

func requireAppendView(t *testing.T, f *committeeFixture, s *AppendSigner, version uint64) *View {
	t.Helper()
	view, err := s.Snapshot(f.genesis)
	if err != nil || view == nil {
		t.Fatalf("append snapshot: %v", err)
	}
	if view.Version() != version || view.Check(testAccount(1000), 0) != nil {
		t.Fatalf("append snapshot version=%d want=%d or genesis changed", view.Version(), version)
	}
	return view
}

func appendFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func lastAppendFrame(t *testing.T, raw []byte) []byte {
	t.Helper()
	last, position := 0, 0
	for position < len(raw) {
		if len(raw)-position < 112 || !bytes.Equal(raw[position:position+8], []byte("CCSWAL1\x00")) {
			t.Fatal("fixture is not a complete append frame sequence")
		}
		length := binary.BigEndian.Uint64(raw[position+8 : position+16])
		if length > uint64(len(raw)-position-112) {
			t.Fatal("fixture contains a short append payload")
		}
		last = position
		position += 112 + int(length)
	}
	return raw[last:]
}

func TestAppendSignerReplayAndDurableSlotLock(t *testing.T) {
	f := compactFixture(t)
	path := filepath.Join(t.TempDir(), "member.wal")
	s := openAppendFixture(t, f, path, "member-0")
	first := compactNO(f, 0, 0, 1000, 1, "first")
	stored := requireAppendVote(t, f, s, first, "member-0")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openAppendFixture(t, f, path, "member-0")
	defer s.Close()
	before := appendFileBytes(t, path)
	replayed := requireAppendVote(t, f, s, first, "member-0")
	if !bytes.Equal(stored.Value, replayed.Value) || !bytes.Equal(before, appendFileBytes(t, path)) {
		t.Fatal("exact restart replay changed signature or appended another vote")
	}
	conflict := cloneBatch(first)
	conflict.ProposalID = "same-slot-other-proposal"
	requireAppendReject(t, s, conflict)
	view := requireAppendView(t, f, s, 0)
	if _, exists := view.Owner(testAccount(1)); exists {
		t.Fatal("a single durable vote installed an owner")
	}
}

func TestAppendSignerHeaderRejectsDifferentIdentityOrPolicy(t *testing.T) {
	for _, name := range []string{"different-config", "different-member", "placement-policy-disabled"} {
		t.Run(name, func(t *testing.T) {
			f := compactFixture(t)
			path := filepath.Join(t.TempDir(), "member.wal")
			s := openAppendFixture(t, f, path, "member-0")
			requireAppendVote(t, f, s, compactNO(f, 0, 0, 1000, 1, "first"), "member-0")
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			before := appendFileBytes(t, path)
			verifier, member, placement := f.verifier, "member-0", true
			if name == "different-config" {
				config := cloneConfig(t, f.config)
				config.Context.PolicyDigest = digestBytes([]byte("different-policy"))
				var err error
				verifier, err = NewVerifier(config)
				if err != nil {
					t.Fatal(err)
				}
			} else if name == "different-member" {
				member = "member-1"
			} else {
				placement = false
			}
			bad, err := OpenAppendSignerWithPolicy(path, verifier, f.genesis, member, f.private[member], placement)
			if bad != nil {
				bad.Close()
			}
			if err == nil || !bytes.Equal(before, appendFileBytes(t, path)) {
				t.Fatal("different pinned identity/policy opened or rewrote the WAL")
			}
		})
	}
}

func TestAppendSignerBadNextVoteCannotPartiallyApplyParent(t *testing.T) {
	f := compactFixture(t)
	path := filepath.Join(t.TempDir(), "member.wal")
	s := openAppendFixture(t, f, path, "member-0")
	defer s.Close()
	first := compactNO(f, 0, 0, 1000, 1, "first")
	requireAppendVote(t, f, s, first, "member-0")
	parent := publicCompact(f.sign(t, first))
	next := compactNO(f, 1, 1, 1, 2, "next")
	bad := cloneBatch(next)
	bad.Bindings[0].Shard = 1
	bad.PlacementWitness.Transactions[0].RecipientShard = 1
	bad.PlacementWitnessDigest = PlacementWitnessDigest(bad.PlacementWitness)
	before := appendFileBytes(t, path)
	if signature, err := s.AdvanceVote([]Certificate{parent}, bad); err == nil || len(signature.Value) != 0 {
		t.Fatal("independently invalid next NO placement accepted")
	}
	view := requireAppendView(t, f, s, 0)
	if _, exists := view.Owner(testAccount(1)); exists || !bytes.Equal(before, appendFileBytes(t, path)) {
		t.Fatal("contract rejection partially applied the parent or altered durable log")
	}
	if _, err := s.AdvanceVote([]Certificate{parent}, next); err != nil {
		t.Fatal(err)
	}
	view = requireAppendView(t, f, s, 1)
	if view.Check(testAccount(1), 0) != nil {
		t.Fatal("valid fused transition did not install parent")
	}
	if _, exists := view.Owner(testAccount(2)); exists {
		t.Fatal("next vote was mistaken for a committed owner")
	}
}

func TestAppendSignerDurabilityCutsFenceAndRecoverAtomicTransition(t *testing.T) {
	for _, cut := range []string{"before-append", "after-write-before-sync", "after-sync-before-reply"} {
		t.Run(cut, func(t *testing.T) {
			f := compactFixture(t)
			path := filepath.Join(t.TempDir(), "member.wal")
			s := openAppendFixture(t, f, path, "member-0")
			first := compactNO(f, 0, 0, 1000, 1, "first")
			requireAppendVote(t, f, s, first, "member-0")
			parent, next := publicCompact(f.sign(t, first)), compactNO(f, 1, 1, 1, 2, "next")
			fault := func() error { return errors.New("independent append durability-cut fixture") }
			switch cut {
			case "before-append":
				s.beforeAppend = fault
			case "after-write-before-sync":
				s.afterWrite = fault
			case "after-sync-before-reply":
				s.afterAppend = fault
			}
			if signature, err := s.AdvanceVote([]Certificate{parent}, next); err == nil || len(signature.Value) != 0 {
				t.Fatal("uncertain durable operation disclosed its signature")
			}
			requireAppendReject(t, s, first)
			requireAppendReject(t, s, next)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openAppendFixture(t, f, path, "member-0")
			defer s.Close()
			version := uint64(1)
			if cut == "before-append" {
				version = 0
			}
			// The afterWrite fixture leaves a COMPLETE valid frame in this OS;
			// this tests uncertain acknowledgement, not simulated power-loss.
			requireAppendView(t, f, s, version)
			if _, err := s.AdvanceVote([]Certificate{parent}, next); err != nil {
				t.Fatal(err)
			}
			conflict := cloneBatch(next)
			conflict.ProposalID = "conflicting-recovered-slot"
			requireAppendReject(t, s, conflict)
			requireAppendView(t, f, s, 1)
		})
	}
}

func TestAppendSignerCompactParentsRequireOwnVoteOrFullMaterial(t *testing.T) {
	for _, member := range []string{"member-0", "member-3"} {
		t.Run(member, func(t *testing.T) {
			f := compactFixture(t)
			path := filepath.Join(t.TempDir(), "member.wal")
			s := openAppendFixture(t, f, path, member)
			winner := compactBatch(f, 0, 0, "winner", []PlacementTransaction{
				{"nn", 0, testAccount(1), testAccount(2), 0, 0},
			}, Binding{testAccount(1), 0}, Binding{testAccount(2), 0})
			minority := cloneBatch(winner)
			minority.ProposalID = "minority"
			minority.Bindings[0].Shard, minority.Bindings[1].Shard = 1, 1
			minority.PlacementWitness.Transactions[0].SenderShard = 1
			minority.PlacementWitness.Transactions[0].RecipientShard = 1
			minority.PlacementWitnessDigest = PlacementWitnessDigest(minority.PlacementWitness)
			var original Signature
			if member == "member-3" {
				original = requireAppendVote(t, f, s, minority, member)
			}
			full := f.sign(t, winner)
			if changed, err := s.Commit(publicCompact(full)); err == nil || changed {
				t.Fatal("missing or different private vote bypassed full witness validation")
			}
			requireAppendView(t, f, s, 0)
			if _, err := s.Commit(full); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openAppendFixture(t, f, path, member)
			defer s.Close()
			view := requireAppendView(t, f, s, 1)
			if view.Check(testAccount(1), 0) != nil || view.Check(testAccount(2), 0) != nil {
				t.Fatal("independent full material failed to restore committed owners")
			}
			if member == "member-3" {
				replay := requireAppendVote(t, f, s, minority, member)
				if !bytes.Equal(original.Value, replay.Value) {
					t.Fatal("learning a different QC overwrote the durable minority vote")
				}
				requireAppendReject(t, s, winner)
			}
		})
	}
}

func TestAppendSignerTornTailArchivedButCompleteCorruptionRejected(t *testing.T) {
	for _, name := range []string{"short-tail", "complete-corrupt-frame", "complete-header-length-corruption"} {
		t.Run(name, func(t *testing.T) {
			f := compactFixture(t)
			path := filepath.Join(t.TempDir(), "member.wal")
			s := openAppendFixture(t, f, path, "member-0")
			first := compactNO(f, 0, 0, 1000, 1, "first")
			requireAppendVote(t, f, s, first, "member-0")
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			prefix := appendFileBytes(t, path)
			frame := lastAppendFrame(t, prefix)
			if name != "short-tail" {
				broken := append([]byte(nil), prefix...)
				if name == "complete-header-length-corruption" {
					start := len(prefix) - len(frame)
					length := binary.BigEndian.Uint64(broken[start+8 : start+16])
					// A corrupt length must not impersonate an incomplete payload.
					// The intact old header checksum makes this corruption detectable.
					binary.BigEndian.PutUint64(broken[start+8:start+16], length+1024)
				} else {
					broken[len(broken)-1] ^= 1
				}
				if err := os.WriteFile(path, broken, 0600); err != nil {
					t.Fatal(err)
				}
				bad, err := OpenAppendSignerWithPolicy(path, f.verifier, f.genesis, "member-0", f.private["member-0"], true)
				if bad != nil {
					bad.Close()
				}
				if err == nil || !bytes.Equal(broken, appendFileBytes(t, path)) {
					t.Fatal("complete corrupted frame was accepted or silently repaired")
				}
				archives, globErr := filepath.Glob(path + ".torn-*")
				if globErr != nil || len(archives) != 0 {
					t.Fatal("complete corruption was incorrectly classified and archived as a torn tail")
				}
				return
			}
			tail := append([]byte(nil), frame[:10]...)
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.Write(tail); err != nil {
				t.Fatal(err)
			}
			file.Close()
			s = openAppendFixture(t, f, path, "member-0")
			defer s.Close()
			requireAppendVote(t, f, s, first, "member-0")
			if !bytes.Equal(prefix, appendFileBytes(t, path)) {
				t.Fatal("torn recovery modified committed prefix")
			}
			archives, err := filepath.Glob(path + ".torn-*")
			if err != nil {
				t.Fatal(err)
			}
			retained := false
			for _, archive := range archives {
				if bytes.Equal(tail, appendFileBytes(t, archive)) {
					retained = true
				}
			}
			if !retained {
				t.Fatal("recovery discarded the exact short tail instead of archiving it")
			}
		})
	}
}

func TestAppendSignerRejectsSecondOpenWithoutBlocking(t *testing.T) {
	f := compactFixture(t)
	path := filepath.Join(t.TempDir(), "member.wal")
	s := openAppendFixture(t, f, path, "member-0")
	defer s.Close()
	began := time.Now()
	other, err := OpenAppendSignerWithPolicy(path, f.verifier, f.genesis, "member-0", f.private["member-0"], true)
	if other != nil {
		other.Close()
	}
	if err == nil || time.Since(began) > time.Second {
		t.Fatal("second signer opened the same durable file or blocked instead of failing its lock")
	}
}
