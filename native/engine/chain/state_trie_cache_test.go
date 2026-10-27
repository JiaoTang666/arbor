package chain

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
)

type cacheCountingDB struct {
	ethdb.Database
	reads      int
	failWrites bool
}

func (db *cacheCountingDB) Get(key []byte) ([]byte, error) {
	db.reads++
	return db.Database.Get(key)
}

type cacheFailBatch struct{ ethdb.Batch }

func (b *cacheFailBatch) Write() error { return errors.New("injected cache-test write failure") }

func (db *cacheCountingDB) NewBatch() ethdb.Batch {
	b := db.Database.NewBatch()
	if db.failWrites {
		return &cacheFailBatch{b}
	}
	return b
}

func cacheCommit(t *testing.T, st *trie.Trie, db *trie.Database) common.Hash {
	t.Helper()
	root, nodes := st.Commit(false)
	if nodes != nil {
		if err := db.Update(trie.NewWithNodeSet(nodes)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Commit(root, false); err != nil {
		t.Fatal(err)
	}
	return root
}

func cacheRead(t *testing.T, db *trie.Database, root common.Hash, expected string) {
	t.Helper()
	st, err := trie.New(trie.TrieID(root), db)
	if err != nil {
		t.Fatal(err)
	}
	value, err := st.Get([]byte("0000000000000000000000000000000000000001"))
	if err != nil || string(value) != expected {
		t.Fatalf("read: %q %v, want %q", value, err, expected)
	}
}

func TestStateTrieCacheConfiguration(t *testing.T) {
	for _, value := range []string{"", "0", "64", "-1", "1025", "invalid", "1.5"} {
		t.Run(fmt.Sprintf("value_%s", value), func(t *testing.T) {
			t.Setenv(stateTrieCacheEnv, value)
			db := rawdb.NewMemoryDatabase()
			defer db.Close()
			_, err := newStateTrieDatabase(db)
			valid := value == "" || value == "0" || value == "64"
			if (err == nil) != valid {
				t.Fatalf("valid=%t err=%v", valid, err)
			}
		})
	}
}

func TestStateTrieCacheReadsRootsAndColdRestart(t *testing.T) {
	var canonical common.Hash
	for _, mode := range []string{"0", "64"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(stateTrieCacheEnv, mode)
			backing := &cacheCountingDB{Database: rawdb.NewMemoryDatabase()}
			defer backing.Close()
			db, err := newStateTrieDatabase(backing)
			if err != nil {
				t.Fatal(err)
			}
			st := trie.NewEmpty(db)
			for i := 0; i < 128; i++ {
				if err := st.Update([]byte(fmt.Sprintf("%040x", i)), []byte("original persistent account state")); err != nil {
					t.Fatal(err)
				}
			}
			root := cacheCommit(t, st, db)
			if mode == "0" {
				canonical = root
			} else if root != canonical {
				t.Fatal("cache changed state root")
			}
			backing.reads = 0
			for i := 0; i < 8; i++ {
				cacheRead(t, db, root, "original persistent account state")
			}
			t.Logf("clean_cache_mb=%s backend_gets_after_commit=%d repeated_root_opens=8", mode, backing.reads)
			if (mode == "0" && backing.reads == 0) || (mode == "64" && backing.reads != 0) {
				t.Fatal("cache did not eliminate committed-node reloads")
			}
			st, err = trie.New(trie.TrieID(root), db)
			if err != nil {
				t.Fatal(err)
			}
			if err := st.Update([]byte("0000000000000000000000000000000000000001"), []byte("changed state")); err != nil {
				t.Fatal(err)
			}
			changed := cacheCommit(t, st, db)
			if changed == root {
				t.Fatal("state mutation did not change root")
			}
			cacheRead(t, db, root, "original persistent account state")
			cacheRead(t, db, changed, "changed state")
			// A new trie database has an empty volatile cache and must still read
			// both committed versions from the persistent backing database.
			reopened, err := newStateTrieDatabase(backing)
			if err != nil {
				t.Fatal(err)
			}
			backing.reads = 0
			cacheRead(t, reopened, root, "original persistent account state")
			cacheRead(t, reopened, changed, "changed state")
			if backing.reads == 0 {
				t.Fatal("cold reopen did not recover from backing store")
			}
		})
	}
}

func TestStateTrieCachePersistenceFailureNotHidden(t *testing.T) {
	t.Setenv(stateTrieCacheEnv, "64")
	backing := &cacheCountingDB{Database: rawdb.NewMemoryDatabase()}
	defer backing.Close()
	db, err := newStateTrieDatabase(backing)
	if err != nil {
		t.Fatal(err)
	}
	st := trie.NewEmpty(db)
	if err := st.Update([]byte("account"), bytes.Repeat([]byte{1}, 64)); err != nil {
		t.Fatal(err)
	}
	root, nodes := st.Commit(false)
	if err := db.Update(trie.NewWithNodeSet(nodes)); err != nil {
		t.Fatal(err)
	}
	backing.failWrites = true
	if err := db.Commit(root, false); err == nil {
		t.Fatal("cache hid persistence failure")
	}
	reopened, err := newStateTrieDatabase(backing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := trie.New(trie.TrieID(root), reopened); err == nil {
		t.Fatal("failed root appeared durable after restart")
	}
	backing.failWrites = false
	if err := db.Commit(root, false); err != nil {
		t.Fatal(err)
	}
	reopened, err = newStateTrieDatabase(backing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := trie.New(trie.TrieID(root), reopened); err != nil {
		t.Fatal(err)
	}
}

func TestStateTrieCacheNativeContracts(t *testing.T) {
	t.Setenv(stateTrieCacheEnv, "64")
	for _, item := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"persistent_restart", TestAccountStateInvariantsAndTrieRecovery},
		{"replica_local_self_relay", TestBlockExecutionContractReplicaLocalSelfAndRelay},
		{"state_write_failure", TestBlockExecutionContractStateFailureAllModes},
		{"block_write_failure", TestBlockExecutionContractBlockStorageFailureDoesNotAdvance},
		{"signed_creation", TestOwnershipSecurityNativeLocalAndRelayCreation},
		{"authorization_bypass", TestOwnershipSecurityCachedRootAndQueueBypassRejected},
		{"invalid_suffix", TestOwnershipRuntimeRejectsBadSuffixBeforeQueueOrProposal},
	} {
		t.Run(item.name, item.run)
	}
}
