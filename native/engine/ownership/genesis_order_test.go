package ownership

import (
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	bolt "go.etcd.io/bbolt"
)

// Used only to construct deterministic legacy-format test stores. Fresh
// production delta stores no longer bulk-persist immutable genesis owners.
func orderedGenesisAccounts(owners map[string]uint64) []string {
	accounts := make([]string, 0, len(owners))
	for account := range owners {
		accounts = append(accounts, account)
	}
	sort.Strings(accounts)
	return accounts
}

func TestOrderedGenesisAccountsSortedWithoutMutation(t *testing.T) {
	owners := map[string]uint64{testAccount(9): 1, testAccount(3): 0, testAccount(7): 1}
	want := []string{testAccount(3), testAccount(7), testAccount(9)}
	for i := 0; i < 10; i++ {
		if got := orderedGenesisAccounts(owners); !reflect.DeepEqual(got, want) {
			t.Fatalf("genesis insertion keys not deterministic and monotone: %v", got)
		}
	}
	if owners[testAccount(9)] != 1 || owners[testAccount(3)] != 0 || len(owners) != 3 || len(orderedGenesisAccounts(nil)) != 0 {
		t.Fatal("ordering changed logical ownership")
	}
}

func largeGenesisFixture(t *testing.T) *committeeFixture {
	t.Helper()
	f := newCommitteeFixture(t)
	f.genesis = map[string]uint64{}
	for id := 4096; id > 0; id-- {
		f.genesis[testAccount(uint64(id))] = uint64(id % 2)
	}
	var err error
	f.config.Context.GenesisRoutesDigest, err = RoutesDigest(f.genesis, 2)
	if err != nil {
		t.Fatal(err)
	}
	f.verifier, err = NewVerifier(f.config)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestOrderedGenesisSignerInitAndRecovery4096Accounts(t *testing.T) {
	f := largeGenesisFixture(t)
	path := filepath.Join(t.TempDir(), "signer.bolt")
	signer, err := OpenSigner(path, f.verifier, f.genesis, "member-0", f.private["member-0"])
	if err != nil {
		t.Fatal(err)
	}
	cert := f.certificate(t, 0, "after-genesis", Binding{testAccount(9000), 1})
	if _, err := signer.Vote(cert.Batch); err != nil {
		t.Fatal(err)
	}
	if changed, err := signer.Commit(cert); err != nil || !changed {
		t.Fatalf("incremental certificate failed: %v", err)
	}
	if err := signer.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSigner(path, f.verifier, f.genesis, "member-0", f.private["member-0"])
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	view, err := reopened.Snapshot(f.genesis)
	if err != nil || view.Version() != 1 || view.ownerCountLocked() != 4097 {
		t.Fatalf("recovered prefix incomplete: %v", err)
	}
	for account, owner := range f.genesis {
		if err := view.Check(account, owner); err != nil {
			t.Fatalf("genesis route changed: %s %v", account, err)
		}
	}
	if err := view.Check(testAccount(9000), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Vote(cert.Batch); err != nil {
		t.Fatalf("durable old vote not replayable: %v", err)
	}
}

func TestOrderedGenesisInitRemainsOneAtomicTransaction(t *testing.T) {
	f := largeGenesisFixture(t)
	path := filepath.Join(t.TempDir(), "failed-init.bolt")
	registry, err := openRegistry(path, f.verifier, f.genesis, func(*bolt.Tx, bool) error {
		return fmt.Errorf("injected initialization failure after metadata writes")
	})
	if err == nil || registry != nil {
		t.Fatal("failed initialization published a registry")
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(registryBucket) != nil {
			return fmt.Errorf("partial initial owner bucket escaped rollback")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
