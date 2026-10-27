package ownership

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func openFixtureRegistry(t *testing.T, f *committeeFixture) (*Registry, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "first-owner.db")
	r, err := Open(path, f.verifier, f.genesis)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r, path
}

func requireRegistrySnapshot(t *testing.T, r *Registry, f *committeeFixture, version uint64) *View {
	t.Helper()
	view, err := r.Snapshot(f.genesis)
	if err != nil {
		t.Fatal(err)
	}
	if view.Version() != version {
		t.Fatalf("version=%d want=%d", view.Version(), version)
	}
	if err := view.Check(testAccount(1000), 0); err != nil {
		t.Fatalf("genesis owner changed: %v", err)
	}
	return view
}

func TestRegistrySignatureOrderReplayAndProposalConflict(t *testing.T) {
	f := newCommitteeFixture(t)
	r, _ := openFixtureRegistry(t, f)
	first := f.certificate(t, 0, "first", Binding{testAccount(1), 1})
	if changed, err := r.Apply(first); err != nil || !changed {
		t.Fatalf("first apply: %v %v", changed, err)
	}
	second := f.certificate(t, 1, "second", Binding{testAccount(2), 0})
	if changed, err := r.Apply(second); err != nil || !changed {
		t.Fatalf("second apply: %v %v", changed, err)
	}
	replayed := cloneCertificate(t, first)
	replayed.Signatures[0], replayed.Signatures[2] = replayed.Signatures[2], replayed.Signatures[0]
	if changed, err := r.Apply(replayed); err != nil || changed {
		t.Fatalf("signature-order replay: %v %v", changed, err)
	}
	conflict := f.certificate(t, 2, "first", Binding{testAccount(3), 0})
	if changed, err := r.Apply(conflict); err == nil || changed {
		t.Fatalf("proposal conflict accepted: %v %v", changed, err)
	}
	view := requireRegistrySnapshot(t, r, f, 2)
	if view.ownerCountLocked() != 3 || view.Check(testAccount(1), 1) != nil || view.Check(testAccount(2), 0) != nil {
		t.Fatal("replay changed owners")
	}
}

func TestRegistryConcurrentCASAndCrossVersionWriteOnce(t *testing.T) {
	f := newCommitteeFixture(t)
	r, _ := openFixtureRegistry(t, f)
	certs := []Certificate{
		f.certificate(t, 0, "one", Binding{testAccount(1), 1}),
		f.certificate(t, 0, "two", Binding{testAccount(2), 0}),
	}
	type result struct {
		changed bool
		err     error
	}
	start, results := make(chan struct{}), make(chan result, 2)
	for _, cert := range certs {
		go func(cert Certificate) { <-start; changed, err := r.Apply(cert); results <- result{changed, err} }(cert)
	}
	close(start)
	success, rejected := 0, 0
	for i := 0; i < 2; i++ {
		got := <-results
		if got.changed && got.err == nil {
			success++
		} else if !got.changed && got.err != nil {
			rejected++
		} else {
			t.Fatalf("invalid CAS outcome: %+v", got)
		}
	}
	if success != 1 || rejected != 1 {
		t.Fatalf("CAS outcomes success=%d rejected=%d", success, rejected)
	}
	view := requireRegistrySnapshot(t, r, f, 1)
	if view.ownerCountLocked() != 2 {
		t.Fatal("CAS partially installed loser")
	}
	var account string
	var owner uint64
	for _, cert := range certs {
		binding := cert.Batch.Bindings[0]
		if current, exists := view.Owner(binding.Account); exists {
			account, owner = binding.Account, current
		}
	}
	for i, target := range []uint64{owner, 1 - owner} {
		cert := f.certificate(t, 1, []string{"repeat-same-owner", "repeat-other-owner"}[i], Binding{account, target})
		if changed, err := r.Apply(cert); err == nil || changed {
			t.Fatal("cross-version first-owner rewrite accepted")
		}
	}
	requireRegistrySnapshot(t, r, f, 1)
}

func TestRegistryConflictingBatchHasZeroPartialWrites(t *testing.T) {
	f := newCommitteeFixture(t)
	r, _ := openFixtureRegistry(t, f)
	cert := f.certificate(t, 0, "mixed", Binding{testAccount(1), 1}, Binding{testAccount(1000), 1})
	if changed, err := r.Apply(cert); err == nil || changed {
		t.Fatal("mixed conflict batch accepted")
	}
	view := requireRegistrySnapshot(t, r, f, 0)
	if _, exists := view.Owner(testAccount(1)); exists || len(view.proposals) != 0 {
		t.Fatal("first item of rejected batch persisted")
	}
	// A contract rejection is not an uncertain disk failure and must not fence.
	if changed, err := r.Apply(f.certificate(t, 0, "valid", Binding{testAccount(1), 1})); err != nil || !changed {
		t.Fatalf("valid batch after contract rejection: %v %v", changed, err)
	}
}

func TestRegistryBoltReopenRetainsOwnersAndReplay(t *testing.T) {
	f := newCommitteeFixture(t)
	r, path := openFixtureRegistry(t, f)
	cert := f.certificate(t, 0, "first", Binding{testAccount(1), 1})
	if changed, err := r.Apply(cert); err != nil || !changed {
		t.Fatalf("apply: %v %v", changed, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, f.verifier, f.genesis)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	view := requireRegistrySnapshot(t, reopened, f, 1)
	if view.Check(testAccount(1), 1) != nil {
		t.Fatal("committed binding missing after reopen")
	}
	if changed, err := reopened.Apply(cert); err != nil || changed {
		t.Fatalf("recovered replay: %v %v", changed, err)
	}
}

func TestRegistryUncertainCommitFencesUntilVerifiedRecovery(t *testing.T) {
	f := newCommitteeFixture(t)
	r, path := openFixtureRegistry(t, f)
	cert := f.certificate(t, 0, "first", Binding{testAccount(1), 1})
	r.afterCommit = func() error { return errors.New("injected uncertain commit acknowledgement") }
	if changed, err := r.Apply(cert); err == nil || changed {
		t.Fatal("uncertain commit published success")
	}
	if _, err := r.Snapshot(f.genesis); err == nil {
		t.Fatal("uncertain registry published a snapshot")
	}
	if changed, err := r.Apply(f.certificate(t, 1, "next", Binding{testAccount(2), 0})); err == nil || changed {
		t.Fatal("fenced registry accepted another batch")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, f.verifier, f.genesis)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	view := requireRegistrySnapshot(t, reopened, f, 1)
	if view.Check(testAccount(1), 1) != nil {
		t.Fatal("durably committed uncertain batch lost")
	}
	if changed, err := reopened.Apply(cert); err != nil || changed {
		t.Fatalf("resolved uncertain replay: %v %v", changed, err)
	}
	if changed, err := reopened.Apply(f.certificate(t, 1, "next", Binding{testAccount(2), 0})); err != nil || !changed {
		t.Fatalf("recovered next batch: %v %v", changed, err)
	}
}

func TestRegistryBeforeCommitRollsBackEntireBatch(t *testing.T) {
	f := newCommitteeFixture(t)
	r, path := openFixtureRegistry(t, f)
	r.beforeCommit = func() error { return errors.New("injected abort before Bolt commit") }
	cert := f.certificate(t, 0, "first", Binding{testAccount(1), 1}, Binding{testAccount(2), 0})
	if changed, err := r.Apply(cert); err == nil || changed {
		t.Fatal("aborted transaction published success")
	}
	if changed, err := r.Apply(cert); err == nil || changed {
		t.Fatal("failed write continued without recovery")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, f.verifier, f.genesis)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	view := requireRegistrySnapshot(t, reopened, f, 0)
	if view.ownerCountLocked() != 1 || len(view.proposals) != 0 {
		t.Fatal("aborted transaction left partial keys")
	}
	if changed, err := reopened.Apply(cert); err != nil || !changed {
		t.Fatalf("retry after rollback recovery: %v %v", changed, err)
	}
}

func TestRegistryCorruptIndexesOrEventFailClosed(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(*bolt.Bucket) error
	}{
		{"owner-index", func(b *bolt.Bucket) error { return b.Put([]byte("a"+testAccount(1)), uintBytes(0)) }},
		{"missing-owner", func(b *bolt.Bucket) error { return b.Delete([]byte("a" + testAccount(1))) }},
		{"proposal-index", func(b *bolt.Bucket) error { return b.Put([]byte("pfirst"), []byte("incorrect-digest")) }},
		{"missing-proposal", func(b *bolt.Bucket) error { return b.Delete([]byte("pfirst")) }},
		{"missing-event", func(b *bolt.Bucket) error { return b.Delete(versionKey(1)) }},
		{"tampered-event", func(b *bolt.Bucket) error {
			var cert Certificate
			if err := json.Unmarshal(b.Get(versionKey(1)), &cert); err != nil {
				return err
			}
			cert.Batch.Bindings[0].Shard = 0
			raw, err := json.Marshal(cert)
			if err != nil {
				return err
			}
			return b.Put(versionKey(1), raw)
		}},
		{"counter", func(b *bolt.Bucket) error { return b.Put([]byte("version"), uintBytes(2)) }},
		{"configuration", func(b *bolt.Bucket) error { return b.Put([]byte("config"), []byte("{}")) }},
		{"extra-key", func(b *bolt.Bucket) error { return b.Put([]byte("unexpected"), []byte("value")) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newCommitteeFixture(t)
			r, path := openFixtureRegistry(t, f)
			if changed, err := r.Apply(f.certificate(t, 0, "first", Binding{testAccount(1), 1})); err != nil || !changed {
				t.Fatalf("apply: %v %v", changed, err)
			}
			if err := r.db.Update(func(tx *bolt.Tx) error { return tc.corrupt(tx.Bucket(registryBucket)) }); err != nil {
				t.Fatal(err)
			}
			if view, err := r.Snapshot(f.genesis); err == nil || view != nil {
				t.Fatal("corrupt live snapshot published a view")
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(path, f.verifier, f.genesis)
			if reopened != nil {
				reopened.Close()
			}
			if err == nil {
				t.Fatal("corrupt registry reopened successfully")
			}
		})
	}
}

func TestRegistryReopenRejectsDifferentTrustedConfiguration(t *testing.T) {
	f := newCommitteeFixture(t)
	r, path := openFixtureRegistry(t, f)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	config := cloneConfig(t, f.config)
	config.Context.CommitteeEpoch++
	v, err := NewVerifier(config)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, v, f.genesis)
	if reopened != nil {
		reopened.Close()
	}
	if err == nil {
		t.Fatal("different fixed committee context reopened old registry")
	}
}

func TestRegistryMissingBucketCannotResetOwnership(t *testing.T) {
	f := newCommitteeFixture(t)
	r, path := openFixtureRegistry(t, f)
	if _, err := r.Apply(f.certificate(t, 0, "first", Binding{testAccount(1), 1})); err != nil {
		t.Fatal(err)
	}
	if err := r.db.Update(func(tx *bolt.Tx) error { return tx.DeleteBucket(registryBucket) }); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, f.verifier, f.genesis)
	if reopened != nil {
		reopened.Close()
	}
	if err == nil {
		t.Fatal("missing bucket silently reset first-owner history")
	}
}
