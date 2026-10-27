package ownership

import (
	"errors"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func requireDeltaIndexSize(t *testing.T, r *Registry, owners, proposals int) {
	t.Helper()
	if err := r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(registryBucket)
		if string(b.Get([]byte("mode"))) != registryDeltaMode || b.Stats().KeyN != 3+owners+2*proposals {
			return errors.New("delta index contains more than metadata and committed dynamic bindings")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDeltaRegistry4096GenesisPersistsOnlySignedDelta(t *testing.T) {
	f := largeGenesisFixture(t)
	path := filepath.Join(t.TempDir(), "member.bolt")
	s, err := OpenSigner(path, f.verifier, f.genesis, "member-0", f.private["member-0"])
	if err != nil {
		t.Fatal(err)
	}
	requireDeltaIndexSize(t, s.registry, 0, 0)
	if len(s.registry.ownerCache) != 4096 {
		t.Fatal("verified genesis missing from private cache")
	}
	cert := f.certificate(t, 0, "delta", Binding{testAccount(9000), 1}, Binding{testAccount(9001), 0})
	if _, err := s.Vote(cert.Batch); err != nil {
		t.Fatal(err)
	}
	requireDeltaIndexSize(t, s.registry, 0, 0) // A durable vote is not an owner commit.
	if len(s.registry.ownerCache) != 4096 {
		t.Fatal("vote prematurely changed owner cache")
	}
	if changed, err := s.Commit(cert); err != nil || !changed {
		t.Fatalf("commit: %v %v", changed, err)
	}
	requireDeltaIndexSize(t, s.registry, 2, 1)
	if len(s.registry.ownerCache) != 4098 {
		t.Fatal("committed cache not extended incrementally")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSigner(path, f.verifier, f.genesis, "member-0", f.private["member-0"])
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	view, err := reopened.Snapshot(f.genesis)
	if err != nil || view.Version() != 1 || view.ownerCountLocked() != 4098 {
		t.Fatalf("recovery: %v", err)
	}
	for account, owner := range f.genesis {
		if view.Check(account, owner) != nil {
			t.Fatal("genesis route changed")
		}
	}
	if view.Check(testAccount(9000), 1) != nil || view.Check(testAccount(9001), 0) != nil {
		t.Fatal("delta route missing")
	}
	requireRejectedSignerVote(t, reopened, f.certificate(t, 1, "rebind-genesis", Binding{testAccount(1), 0}).Batch)
	requireRejectedSignerVote(t, reopened, f.certificate(t, 1, "rebind-delta", Binding{testAccount(9000), 0}).Batch)
}

// Construct the exact previous no-mode layout in this test's private store.
// Production never silently converts, deletes, or reseeds a legacy store.
func TestDeltaRegistryLegacySignerReopensAndAppendsWithoutConversion(t *testing.T) {
	f := newCommitteeFixture(t)
	s, path := openFixtureSigner(t, f, "member-0")
	first := f.certificate(t, 0, "old", Binding{testAccount(1), 1})
	requireSignerVote(t, f, s, first.Batch, "member-0")
	if _, err := s.Commit(first); err != nil {
		t.Fatal(err)
	}
	if err := s.registry.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(registryBucket)
		if err := b.Delete([]byte("mode")); err != nil {
			return err
		}
		for _, account := range orderedGenesisAccounts(f.genesis) {
			if err := b.Put([]byte("a"+account), uintBytes(f.genesis[account])); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSigner(path, f.verifier, f.genesis, "member-0", f.private["member-0"])
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	next := f.certificate(t, 1, "new", Binding{testAccount(2), 0})
	requireSignerVote(t, f, reopened, next.Batch, "member-0")
	if changed, err := reopened.Commit(next); err != nil || !changed {
		t.Fatalf("legacy append: %v %v", changed, err)
	}
	view := requireSignerSnapshot(t, reopened, f, 2)
	if view.Check(testAccount(1), 1) != nil || view.Check(testAccount(2), 0) != nil {
		t.Fatal("legacy route lost")
	}
	if err := reopened.registry.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(registryBucket)
		if b.Get([]byte("mode")) != nil || b.Get([]byte("a"+testAccount(1000))) == nil || b.Stats().KeyN != 9 {
			return errors.New("legacy schema silently converted or incomplete")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenSigner(path, f.verifier, f.genesis, "member-0", f.private["member-0"])
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	requireSignerSnapshot(t, recovered, f, 2)
	requireSignerVote(t, f, recovered, first.Batch, "member-0")
	requireSignerVote(t, f, recovered, next.Batch, "member-0")
}

func TestDeltaRegistryModeAndIndexCorruptionRejectStartup(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(*bolt.Bucket) error
	}{
		{"unknown-mode", func(b *bolt.Bucket) error { return b.Put([]byte("mode"), []byte("delta_v2")) }},
		{"empty-mode", func(b *bolt.Bucket) error { return b.Put([]byte("mode"), []byte{}) }},
		{"genesis-index", func(b *bolt.Bucket) error { return b.Put([]byte("a"+testAccount(1000)), uintBytes(0)) }},
		{"extra-delta", func(b *bolt.Bucket) error { return b.Put([]byte("a"+testAccount(2)), uintBytes(0)) }},
		{"missing-delta", func(b *bolt.Bucket) error { return b.Delete([]byte("a" + testAccount(1))) }},
		{"wrong-delta", func(b *bolt.Bucket) error { return b.Put([]byte("a"+testAccount(1)), uintBytes(0)) }},
		{"deleted-mode-not-legacy", func(b *bolt.Bucket) error { return b.Delete([]byte("mode")) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newCommitteeFixture(t)
			r, path := openFixtureRegistry(t, f)
			if _, err := r.Apply(f.certificate(t, 0, "first", Binding{testAccount(1), 1})); err != nil {
				t.Fatal(err)
			}
			if err := r.db.Update(func(tx *bolt.Tx) error { return tc.corrupt(tx.Bucket(registryBucket)) }); err != nil {
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
				t.Fatal("corrupt delta registry opened")
			}
		})
	}
}

func TestDeltaRegistryCacheNeverAliasesCallerOrSnapshot(t *testing.T) {
	f := newCommitteeFixture(t)
	s, _ := openFixtureSigner(t, f, "member-0")
	delete(f.genesis, testAccount(1000))
	f.genesis[testAccount(1)] = 1
	// Caller mutations cannot remove a verified genesis binding or invent one.
	requireRejectedSignerVote(t, s, f.certificate(t, 0, "genesis", Binding{testAccount(1000), 1}).Batch)
	cert := f.certificate(t, 0, "valid", Binding{testAccount(1), 1})
	requireSignerVote(t, f, s, cert.Batch, "member-0")
	delete(f.genesis, testAccount(1))
	f.genesis[testAccount(1000)] = 0
	view := requireSignerSnapshot(t, s, f, 0)
	delete(view.genesis.routes, testAccount(1000)) // Deliberately corrupt this isolated snapshot.
	view.owners[testAccount(1)] = 0
	requireRejectedSignerVote(t, s, f.certificate(t, 0, "snapshot-genesis", Binding{testAccount(1000), 1}).Batch)
	if changed, err := s.Commit(cert); err != nil || !changed {
		t.Fatalf("snapshot alias changed private cache: %v", err)
	}
	view = requireSignerSnapshot(t, s, f, 1)
	delete(view.owners, testAccount(1))
	if _, err := s.Commit(f.certificate(t, 1, "snapshot-rebind", Binding{testAccount(1), 0})); err == nil {
		t.Fatal("snapshot removed committed cache binding")
	}
	if requireSignerSnapshot(t, s, f, 1).Check(testAccount(1), 1) != nil {
		t.Fatal("separate snapshots alias")
	}
}

func TestDeltaRegistryCachePublishesOnlyAfterAcknowledgedCommit(t *testing.T) {
	f := newCommitteeFixture(t)
	r, _ := openFixtureRegistry(t, f)
	cert := f.certificate(t, 0, "first", Binding{testAccount(1), 1})
	hooks := 0
	check := func() error {
		hooks++
		if _, visible := r.ownerCache[testAccount(1)]; visible {
			return errors.New("cache published before durability acknowledgement")
		}
		return nil
	}
	r.beforeCommit, r.afterCommit = check, check
	if changed, err := r.Apply(cert); err != nil || !changed || hooks != 2 {
		t.Fatalf("commit: %v %v hooks=%d", changed, err, hooks)
	}
	if owner, ok := r.ownerCache[testAccount(1)]; !ok || owner != 1 {
		t.Fatal("cache missing acknowledged owner")
	}
}

func TestDeltaRegistryCacheFaultFencesAndRebuildsFromDurablePrefix(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[after], func(t *testing.T) {
			f := newCommitteeFixture(t)
			r, path := openFixtureRegistry(t, f)
			cert := f.certificate(t, 0, "first", Binding{testAccount(1), 1})
			fault := func() error { return errors.New("durability fault") }
			if after {
				r.afterCommit = fault
			} else {
				r.beforeCommit = fault
			}
			if changed, err := r.Apply(cert); err == nil || changed {
				t.Fatal("fault published commit")
			}
			if _, ok := r.ownerCache[testAccount(1)]; ok || !r.fenced {
				t.Fatal("fault advanced cache or failed to fence")
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(path, f.verifier, f.genesis)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			_, bound := reopened.ownerCache[testAccount(1)]
			if bound != after {
				t.Fatal("recovered cache differs from durable prefix")
			}
			if changed, err := reopened.Apply(cert); err != nil || changed == after {
				t.Fatalf("resolved replay: %v %v", changed, err)
			}
		})
	}
}
