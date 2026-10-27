package ownership

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func openCertificateSigner(t *testing.T, f *committeeFixture) (*Signer, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "certificate.bolt")
	s, err := OpenSignerWithMode(path, f.verifier, f.genesis, "member-0", f.private["member-0"], RegistryCertificateMode)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, path
}

func requireCertificateRecords(t *testing.T, s *Signer, proposals int) {
	t.Helper()
	if err := s.registry.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(registryBucket)
		if string(b.Get([]byte("mode"))) != RegistryCertificateMode || b.Stats().KeyN != 3+2*proposals {
			return errors.New("certificate store contains more than signed facts and metadata")
		}
		return b.ForEach(func(key, _ []byte) error {
			if len(key) > 0 && key[0] == 'a' {
				return errors.New("redundant address index persisted")
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCertificateRegistry4096GenesisRecoveryAndWriteOnce(t *testing.T) {
	f := largeGenesisFixture(t)
	s, path := openCertificateSigner(t, f)
	cert := f.certificate(t, 0, "first", Binding{testAccount(9000), 1}, Binding{testAccount(9001), 0})
	first := requireSignerVote(t, f, s, cert.Batch, "member-0")
	requireCertificateRecords(t, s, 0)
	if len(s.registry.ownerCache) != 4096 {
		t.Fatal("uncommitted vote changed ownership")
	}
	if changed, err := s.Commit(cert); err != nil || !changed {
		t.Fatalf("commit: %v %v", changed, err)
	}
	requireCertificateRecords(t, s, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenSignerWithMode(path, f.verifier, f.genesis, "member-0", f.private["member-0"], RegistryCertificateMode)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	view, err := recovered.Snapshot(f.genesis)
	if err != nil || view.Version() != 1 || view.ownerCountLocked() != 4098 {
		t.Fatalf("recovery: %v", err)
	}
	if view.Check(testAccount(9000), 1) != nil || view.Check(testAccount(9001), 0) != nil {
		t.Fatal("certificate replay lost ownership")
	}
	replay := requireSignerVote(t, f, recovered, cert.Batch, "member-0")
	if !bytes.Equal(first.Value, replay.Value) {
		t.Fatal("recovery changed the durable vote")
	}
	if changed, err := recovered.Commit(cert); err != nil || changed {
		t.Fatalf("commit replay: %v %v", changed, err)
	}
	for _, account := range []string{testAccount(1), testAccount(9000)} {
		rebound := f.certificate(t, 1, "rebind", Binding{account, 0})
		requireRejectedSignerVote(t, recovered, rebound.Batch)
		if changed, err := recovered.Commit(rebound); err == nil || changed {
			t.Fatal("rebound historical or committed owner")
		}
	}
	delete(view.owners, testAccount(9000))
	if _, err := recovered.Commit(f.certificate(t, 1, "snapshot-alias", Binding{testAccount(9000), 0})); err == nil {
		t.Fatal("public snapshot mutated the private owner cache")
	}
	requireCertificateRecords(t, recovered, 1)
}

func TestCertificateRegistryDurabilityFaultRecovery(t *testing.T) {
	for _, phase := range []string{"vote", "commit"} {
		for _, after := range []bool{false, true} {
			name := phase + map[bool]string{false: "-before", true: "-after"}[after]
			t.Run(name, func(t *testing.T) {
				f := newCommitteeFixture(t)
				s, path := openCertificateSigner(t, f)
				cert := f.certificate(t, 0, "fault", Binding{testAccount(1), 1})
				fault := func() error { return errors.New("injected durability acknowledgement failure") }
				if phase == "vote" {
					if after {
						s.afterVoteCommit = fault
					} else {
						s.beforeVoteCommit = fault
					}
					requireRejectedSignerVote(t, s, cert.Batch)
				} else {
					requireSignerVote(t, f, s, cert.Batch, "member-0")
					if after {
						s.registry.afterCommit = fault
					} else {
						s.registry.beforeCommit = fault
					}
					if changed, err := s.Commit(cert); err == nil || changed {
						t.Fatal("uncertain commit returned success")
					}
				}
				if _, bound := s.registry.ownerCache[testAccount(1)]; bound || !s.registry.fenced {
					t.Fatal("uncertain operation advanced cache or failed to fence")
				}
				if _, err := s.Version(); err == nil {
					t.Fatal("fenced signer advertised a usable head")
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				recovered, err := OpenSignerWithMode(path, f.verifier, f.genesis, "member-0", f.private["member-0"], RegistryCertificateMode)
				if err != nil {
					t.Fatal(err)
				}
				defer recovered.Close()
				committed := phase == "commit" && after
				if _, bound := recovered.registry.ownerCache[testAccount(1)]; bound != committed {
					t.Fatal("recovered ownership differs from the durable prefix")
				}
				if phase == "vote" {
					if err := recovered.registry.db.View(func(tx *bolt.Tx) error {
						if found := tx.Bucket(signerVoteBucket).Get(voteKey(0)) != nil; found != after {
							return errors.New("durable vote differs from the interruption boundary")
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					if after {
						requireRejectedSignerVote(t, recovered, f.certificate(t, 0, "conflict", Binding{testAccount(2), 0}).Batch)
					}
				}
				requireSignerVote(t, f, recovered, cert.Batch, "member-0")
				if changed, err := recovered.Commit(cert); err != nil || changed == committed {
					t.Fatalf("resolved replay: %v %v", changed, err)
				}
				requireCertificateRecords(t, recovered, 1)
			})
		}
	}
}

func TestCertificateRegistryCorruptPrefixFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*bolt.Bucket) error
	}{
		{"missing-certificate", func(b *bolt.Bucket) error { return b.Delete(versionKey(1)) }},
		{"invalid-certificate", func(b *bolt.Bucket) error { return b.Put(versionKey(1), []byte("{}")) }},
		{"missing-proposal", func(b *bolt.Bucket) error { return b.Delete([]byte("pfirst")) }},
		{"extra-proposal", func(b *bolt.Bucket) error { return b.Put([]byte("pextra"), []byte("bad")) }},
		{"wrong-proposal", func(b *bolt.Bucket) error { return b.Put([]byte("pfirst"), []byte("bad")) }},
		{"redundant-address", func(b *bolt.Bucket) error { return b.Put([]byte("a"+testAccount(1)), uintBytes(1)) }},
		{"extra-certificate", func(b *bolt.Bucket) error { return b.Put(versionKey(2), b.Get(versionKey(1))) }},
		{"missing-mode", func(b *bolt.Bucket) error { return b.Delete([]byte("mode")) }},
		{"wrong-mode", func(b *bolt.Bucket) error { return b.Put([]byte("mode"), []byte(registryDeltaMode)) }},
		{"head-beyond-prefix", func(b *bolt.Bucket) error { return b.Put([]byte("version"), uintBytes(2)) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newCommitteeFixture(t)
			s, path := openCertificateSigner(t, f)
			if _, err := s.Commit(f.certificate(t, 0, "first", Binding{testAccount(1), 1})); err != nil {
				t.Fatal(err)
			}
			if err := s.registry.db.Update(func(tx *bolt.Tx) error { return tc.mutate(tx.Bucket(registryBucket)) }); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenSigner(path, f.verifier, f.genesis, "member-0", f.private["member-0"])
			if reopened != nil {
				_ = reopened.Close()
			}
			if err == nil {
				t.Fatal("corrupt certificate store opened")
			}
		})
	}
}

func TestCertificateRegistryModeChangesFenceEveryEntry(t *testing.T) {
	for _, entry := range []string{"snapshot", "vote", "commit", "commit-replay", "status"} {
		t.Run(entry, func(t *testing.T) {
			f := newCommitteeFixture(t)
			s, _ := openCertificateSigner(t, f)
			cert := f.certificate(t, 0, "after-mode-change", Binding{testAccount(1), 1})
			if entry == "commit-replay" {
				if _, err := s.Commit(cert); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.registry.db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(registryBucket).Put([]byte("mode"), []byte(registryDeltaMode))
			}); err != nil {
				t.Fatal(err)
			}
			var err error
			switch entry {
			case "snapshot":
				_, err = s.Snapshot(f.genesis)
			case "vote":
				_, err = s.Vote(cert.Batch)
			case "commit", "commit-replay":
				_, err = s.Commit(cert)
			case "status":
				_, err = s.Version()
			}
			if err == nil || !s.registry.fenced {
				t.Fatal("entry accepted a changed storage mode")
			}
			if _, bound := s.registry.ownerCache[testAccount(1)]; bound != (entry == "commit-replay") {
				t.Fatal("mode drift changed ownership")
			}
		})
	}
}

func TestCertificateRegistryExplicitModeNeverConvertsOldStore(t *testing.T) {
	f := newCommitteeFixture(t)
	s, path := openFixtureSigner(t, f, "member-0")
	cert := f.certificate(t, 0, "old", Binding{testAccount(1), 1})
	if _, err := s.Commit(cert); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if rejected, err := OpenSignerWithMode(path, f.verifier, f.genesis, "member-0", f.private["member-0"], RegistryCertificateMode); err == nil || rejected != nil {
		t.Fatal("explicit certificate mode silently converted delta store")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("rejected mode changed old store")
	}
	recovered, err := OpenSigner(path, f.verifier, f.genesis, "member-0", f.private["member-0"])
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	requireSignerSnapshot(t, recovered, f, 1)
}

func TestCertificateRegistryConflictsHaveNoPartialOwnership(t *testing.T) {
	f := newCommitteeFixture(t)
	s, _ := openCertificateSigner(t, f)
	cert := f.certificate(t, 0, "mixed", Binding{testAccount(1), 1}, Binding{testAccount(1000), 1})
	if changed, err := s.Commit(cert); err == nil || changed || s.registry.fenced {
		t.Fatal("mixed conflict changed state or unnecessarily fenced")
	}
	requireCertificateRecords(t, s, 0)
	if _, bound := s.registry.ownerCache[testAccount(1)]; bound {
		t.Fatal("rejected batch published its first item")
	}
	valid := f.certificate(t, 0, "valid", Binding{testAccount(1), 1})
	if changed, err := s.Commit(valid); err != nil || !changed {
		t.Fatalf("valid batch after rejection: %v %v", changed, err)
	}
	bad := cloneCertificate(t, valid)
	bad.Signatures = bad.Signatures[:2]
	if changed, err := s.Commit(bad); err == nil || changed {
		t.Fatal("unsigned/minority replay bypassed verification")
	}
}

func TestCertificateRegistryRejectsValidlySignedDuplicateOwnerOnRecovery(t *testing.T) {
	f := newCommitteeFixture(t)
	s, path := openCertificateSigner(t, f)
	first := f.certificate(t, 0, "first", Binding{testAccount(1), 1})
	if _, err := s.Commit(first); err != nil {
		t.Fatal(err)
	}
	second := f.certificate(t, 1, "second", Binding{testAccount(1), 0})
	_, digest, err := f.verifier.Verify(second)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(second)
	if err := s.registry.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(registryBucket)
		if err := b.Put(versionKey(2), raw); err != nil {
			return err
		}
		if err := b.Put([]byte("psecond"), []byte(digest)); err != nil {
			return err
		}
		return b.Put([]byte("version"), uintBytes(2))
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if recovered, err := OpenSigner(path, f.verifier, f.genesis, "member-0", f.private["member-0"]); err == nil || recovered != nil {
		t.Fatal("valid signatures overrode write-once semantics during recovery")
	}
}

func TestCertificateRegistryConcurrentHeadCAS(t *testing.T) {
	f := newCommitteeFixture(t)
	s, _ := openCertificateSigner(t, f)
	start, done := make(chan struct{}), make(chan bool, 2)
	for _, cert := range []Certificate{
		f.certificate(t, 0, "left", Binding{testAccount(1), 1}),
		f.certificate(t, 0, "right", Binding{testAccount(2), 0}),
	} {
		go func(cert Certificate) {
			<-start
			changed, err := s.Commit(cert)
			done <- changed && err == nil
		}(cert)
	}
	close(start)
	a, b := <-done, <-done
	if a == b {
		t.Fatal("concurrent commits did not choose exactly one head")
	}
	view := requireSignerSnapshot(t, s, f, 1)
	if view.ownerCountLocked() != 2 {
		t.Fatal("concurrent loser left partial ownership")
	}
	requireCertificateRecords(t, s, 1)
}

func TestCertificateRegistryUnknownRequestedModeCreatesNothing(t *testing.T) {
	f := newCommitteeFixture(t)
	path := filepath.Join(t.TempDir(), "unknown.bolt")
	if s, err := OpenSignerWithMode(path, f.verifier, f.genesis, "member-0", f.private["member-0"], "unknown"); err == nil || s != nil {
		t.Fatal("unknown requested mode accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("invalid mode created a store")
	}
}

func TestCertificateRegistrySnapshotDoesNotAliasCache(t *testing.T) {
	f := newCommitteeFixture(t)
	s, _ := openCertificateSigner(t, f)
	cert := f.certificate(t, 0, "first", Binding{testAccount(1), 1})
	if _, err := s.Commit(cert); err != nil {
		t.Fatal(err)
	}
	view := requireSignerSnapshot(t, s, f, 1)
	if !reflect.DeepEqual(view.ownerSnapshot(), s.registry.ownerCache) {
		t.Fatal("cache differs from verified prefix")
	}
	view.owners[testAccount(1)] = 0
	delete(view.genesis.routes, testAccount(1000)) // Deliberately corrupt this isolated snapshot.
	if s.registry.ownerCache[testAccount(1)] != 1 || len(s.registry.ownerCache) != 2 {
		t.Fatal("public view aliased cache")
	}
}
