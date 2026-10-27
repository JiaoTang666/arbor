package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

// Opt-in replay of existing real blocks. Both databases are opened read-only;
// validation operates on a private trie and never persists or advances a head.
// This establishes state-transition equivalence, NOT committee-certificate,
// remote-debit-proof or Byzantine-safety validation of the archived workload.
func TestBlockExecutionArchiveReplay(t *testing.T) {
	run := os.Getenv("COOLCHAIN_EXECUTION_ARCHIVE_RUN")
	if run == "" {
		t.Skip("set COOLCHAIN_EXECUTION_ARCHIVE_RUN to an existing completed run")
	}
	if info, err := os.Stat(run); err != nil || !info.IsDir() {
		t.Fatalf("archive run is not an existing directory: %s (%v)", run, err)
	}
	// A fresh native process first encodes AccountState while seeding genesis.
	// Legacy gob embeds process-global type IDs, so replay must initialize the
	// codec in the same order rather than silently accept another state root.
	(&core.AccountState{Balance: big.NewInt(0)}).Encode()
	oldShards := params.ShardNum
	t.Cleanup(func() { params.ShardNum = oldShards })
	params.ShardNum = 16
	t.Setenv(NativeEffectIndexEnv, "false")
	t.Setenv("BLOCK_EMULATOR_CLPA_EPOCH_V1", "")
	for _, shard := range []uint64{0, 3, 4, 6, 11} {
		t.Run(fmt.Sprintf("shard_%d", shard), func(t *testing.T) {
			blockPath := filepath.Join(run, "runtime", "database", "chainDB", fmt.Sprintf("S%d_N0", shard))
			statePath := filepath.Join(run, "runtime", "database", "mptDB", "ldb", fmt.Sprintf("s%d", shard), "n0")
			blocks, parents := archiveFirstBlocks(t, blockPath, 2)
			if info, err := os.Stat(statePath); err != nil || !info.IsDir() {
				t.Fatalf("missing archived state database: %s (%v)", statePath, err)
			}
			db, err := rawdb.NewLevelDBDatabase(statePath, 32, 16, "execution-archive-readonly", true)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for i, block := range blocks {
				parent := parents[i]
				for _, tx := range block.Body {
					if tx == nil || !tx.RoutingFrozen || tx.HasBroker {
						t.Fatal("archive fixture requires frozen RawRelay execution routes")
					}
				}
				// The archived parent supplies actual committed account states.
				// Existing genesis accounts are not recreated, so no full genesis
				// route table is required to reproduce first-owner records here.
				triedb, err := newStateTrieDatabase(db)
				if err != nil {
					t.Fatal(err)
				}
				bc := &BlockChain{db: db, triedb: triedb,
					ChainConfig:  &params.ChainConfig{ShardID: shard, ShardNums: 16, Nodes_perShard: 4},
					CurrentBlock: parent, PartitionMap: make(map[string]uint64)}
				before := parent.Encode()
				parentTrie, err := openStateTrieAtRoot(common.BytesToHash(parent.Header.StateRoot), bc.triedb)
				if err != nil {
					t.Fatal(err)
				}
				for _, tx := range block.Body {
					raw, err := readTrieValue(parentTrie, []byte(tx.Sender))
					if err != nil {
						t.Fatal(err)
					}
					if len(raw) > 0 {
						reencoded := core.DecodeAS(raw).Encode()
						t.Logf("archive_account_roundtrip shard=%d block=%d byte_equal=%t stored_prefix=%x reencoded_prefix=%x", shard, block.Header.Number, bytes.Equal(raw, reencoded), archivePrefix(raw), archivePrefix(reencoded))
						if !bytes.Equal(raw, reencoded) {
							t.Fatal("archive account codec initialization differs from native; exact-root replay cannot be evaluated")
						}
						break
					}
				}
				started := time.Now()
				result, err := bc.validateBlockExecution(block)
				if err != nil {
					t.Fatalf("archived block %d transition differs: %v (parent_match=%t stored_hash=%x reencoded_hash=%x)",
						block.Header.Number, err, bytes.Equal(block.Header.ParentBlockHash, parent.Hash), block.Hash, block.Header.Hash())
				}
				if result.persisted || bc.CurrentBlock != parent || !bytes.Equal(before, parent.Encode()) {
					t.Fatal("read-only replay persisted execution or modified its parent")
				}
				if !bytes.Equal(result.root.Bytes(), block.Header.StateRoot) || len(result.applied) != len(block.Body) {
					t.Fatal("archived execution root or positional outcome coverage differs")
				}
				applied := 0
				for _, success := range result.applied {
					if success {
						applied++
					}
				}
				t.Logf("archive_replay shard=%d block=%d txs=%d applied=%d root=%x elapsed_ms=%d readonly=true certificate_validation=false",
					shard, block.Header.Number, len(block.Body), applied, result.root.Bytes(), time.Since(started).Milliseconds())
			}
		})
	}
}

func archiveFirstBlocks(t *testing.T, path string, count int) ([]*core.Block, []*core.Block) {
	t.Helper()
	if info, err := os.Stat(path); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("missing archived block database: %s (%v)", path, err)
	}
	db, err := bolt.Open(path, 0400, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	blocks := make([]*core.Block, 0, count+1)
	parents := make([]*core.Block, 0, count)
	if err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("block"))
		if bucket == nil {
			return fmt.Errorf("archived block bucket missing")
		}
		if err := bucket.ForEach(func(_, raw []byte) error {
			if raw == nil {
				return nil
			}
			block := core.DecodeB(raw)
			if block.Header == nil || block.Header.Number == 0 || len(block.Body) == 0 {
				return nil
			}
			blocks = append(blocks, block)
			sort.Slice(blocks, func(i, j int) bool { return blocks[i].Header.Number < blocks[j].Header.Number })
			if len(blocks) > count {
				blocks = blocks[:count]
			}
			return nil
		}); err != nil {
			return err
		}
		if len(blocks) != count {
			return fmt.Errorf("need %d nonempty archived blocks, found %d", count, len(blocks))
		}
		for _, block := range blocks {
			storedHeader := tx.Bucket([]byte("blockHeader")).Get(block.Hash)
			reencoded := block.Header.Encode()
			t.Logf("archive_header height=%d stored_header_hash=%x declared_hash=%x reencoded_hash=%x wire_equal=%t stored_prefix=%x reencoded_prefix=%x",
				block.Header.Number, sha256.Sum256(storedHeader), block.Hash, sha256.Sum256(reencoded), bytes.Equal(storedHeader, reencoded), archivePrefix(storedHeader), archivePrefix(reencoded))
			raw := bucket.Get(block.Header.ParentBlockHash)
			if raw == nil {
				return fmt.Errorf("archived parent of block %d missing", block.Header.Number)
			}
			parents = append(parents, core.DecodeB(raw))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return blocks, parents
}

func archivePrefix(raw []byte) []byte {
	if len(raw) > 16 {
		return raw[:16]
	}
	return raw
}
