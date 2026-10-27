package chain

import (
	"blockEmulator/initialledger"
	"blockEmulator/params"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitialLedgerRealTrieLayouts(t *testing.T) {
	oldPath, oldShards := params.DatabaseWrite_path, params.ShardNum
	t.Cleanup(func() { params.DatabaseWrite_path = oldPath; params.ShardNum = oldShards })
	params.ShardNum = 2
	accounts := []initialledger.Account{
		{Address: strings.Repeat("0", 39) + "1", Balance: "77", Nonce: 4, StorageRoot: strings.Repeat("12", 32), CodeHash: strings.Repeat("34", 32), Shard: 0},
		{Address: strings.Repeat("0", 39) + "2", Balance: "123", Nonce: 8, Shard: 1},
	}
	logical := ""
	for arm := 0; arm < 2; arm++ {
		if arm == 1 {
			for i := range accounts {
				accounts[i].Shard = 1 - accounts[i].Shard
			}
		}
		raw, _ := json.Marshal(initialledger.Manifest{Schema: "synthetic_initial_ledger_v1", Shards: 2, Accounts: accounts})
		path := filepath.Join(t.TempDir(), "ledger.json")
		if e := os.WriteFile(path, raw, 0600); e != nil {
			t.Fatal(e)
		}
		hash := sha256.Sum256(raw)
		digest := hex.EncodeToString(hash[:])
		l, e := initialledger.Load(path, digest, 2)
		if e != nil {
			t.Fatal(e)
		}
		if arm == 0 {
			logical = l.LogicalHash
		} else if logical != l.LogicalHash {
			t.Fatal("logical ledger changed")
		}
		t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_PATH", path)
		t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256", digest)
		roots := map[uint64][]byte{}
		for replica := 0; replica < 2; replica++ {
			for shard := uint64(0); shard < 2; shard++ {
				root := t.TempDir()
				params.DatabaseWrite_path = root + string(filepath.Separator)
				bc := newMigrationChain(t, root, shard)
				if bc.CurrentBlock.Header.Number != 0 || len(bc.CurrentBlock.Body) != 0 {
					t.Fatal("not height-zero empty-body genesis")
				}
				for _, a := range accounts {
					if bc.PartitionMap[a.Address] != a.Shard {
						t.Fatal("node route mismatch")
					}
					got, e := bc.ExportAccountState(a.Address)
					if a.Shard != shard {
						if e == nil {
							t.Fatal("account exists on wrong shard")
						}
						continue
					}
					if e != nil {
						t.Fatal(e)
					}
					if !bytes.Equal(got.EncodedState, a.State().Encode()) {
						t.Fatal("full logical state not preserved")
					}
				}
				if _, e := bc.ExportAccountState(strings.Repeat("f", 40)); e == nil {
					t.Fatal("future account precreated")
				}
				if replica == 0 {
					roots[shard] = append([]byte(nil), bc.CurrentBlock.Header.StateRoot...)
				} else if !bytes.Equal(roots[shard], bc.CurrentBlock.Header.StateRoot) {
					t.Fatal("same-height replica root mismatch")
				}
				// Reusing the live MPT must fail before another block database is opened.
				if _, e := NewBlockChain(bc.ChainConfig, bc.db); e == nil {
					t.Fatal("marked database accepted")
				}
				t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_PATH", "")
				t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256", "")
				if _, e := NewBlockChain(bc.ChainConfig, bc.db); e == nil {
					t.Fatal("marker bypass by removing configuration")
				}
				t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_PATH", path)
				t.Setenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256", digest)
				bc.CloseBlockChain()
			}
		}
	}
}
