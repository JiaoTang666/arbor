package committee

import (
	"blockEmulator/core"
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/trie"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// This oracle is intentionally specific to the single-account self-transfer
// fixture. Nodes must be stopped. It never starts nodes or writes databases.
func checkNativePostRouteDisk(root string, hash []byte) (string, error) {
	const account = "0000000000000000000000000000000000000010"
	var states [2]*core.AccountState
	var committedState *core.AccountState
	var matchedBlock uint64
	for shard := 0; shard < 2; shard++ {
		db, err := bolt.Open(filepath.Join(root, fmt.Sprintf("exp/database/chainDB/S%d_N0", shard)), 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
		if err != nil {
			return "", err
		}
		var head, matched *core.Block
		count := 0
		err = db.View(func(tx *bolt.Tx) error {
			heads, blocks := tx.Bucket([]byte("newestBlockHash")), tx.Bucket([]byte("block"))
			if heads == nil || blocks == nil {
				return fmt.Errorf("shard %d missing chain buckets", shard)
			}
			key := append([]byte(nil), heads.Get([]byte("OnlyNewestBlock"))...)
			seen := map[string]bool{}
			for len(key) > 0 {
				if seen[string(key)] {
					return fmt.Errorf("chain cycle")
				}
				seen[string(key)] = true
				raw := blocks.Get(key)
				if len(raw) == 0 {
					return fmt.Errorf("missing canonical block %x", key)
				}
				b := core.DecodeB(raw)
				if !bytes.Equal(b.Hash, key) {
					return fmt.Errorf("block key mismatch")
				}
				if head == nil {
					head = b
				}
				for _, entry := range b.Body {
					if bytes.Equal(entry.TxHash, hash) {
						count++
						matched = b
						if entry.Sender != account || entry.Recipient != account || entry.Relayed || !entry.RoutingFrozen || entry.RoutedSenderShard != 1 || entry.RoutedRecipientShard != 1 || entry.MappingVersion != 1 || entry.Value.String() != "1" {
							return fmt.Errorf("transaction route/body mismatch")
						}
					}
				}
				if b.Header.Number == 0 {
					break
				}
				key = b.Header.ParentBlockHash
			}
			if head == nil {
				return fmt.Errorf("empty chain")
			}
			return nil
		})
		db.Close()
		if err != nil {
			return "", err
		}
		if (shard == 0 && count != 0) || (shard == 1 && count != 1) {
			return "", fmt.Errorf("shard %d expected transaction count %d, got %d", shard, shard, count)
		}
		stateDB, err := rawdb.NewLevelDBDatabase(filepath.Join(root, fmt.Sprintf("exp/database/mptDB/ldb/s%d/n0", shard)), 0, 1, "postroute-audit", true)
		if err != nil {
			return "", err
		}
		read := func(b *core.Block) (*core.AccountState, error) {
			st, e := trie.New(trie.TrieID(common.BytesToHash(b.Header.StateRoot)), trie.NewDatabase(stateDB))
			if e != nil {
				return nil, e
			}
			raw, e := st.Get([]byte(account))
			if e != nil {
				return nil, e
			}
			if len(raw) == 0 {
				return nil, fmt.Errorf("account absent")
			}
			return core.DecodeAS(raw), nil
		}
		states[shard], err = read(head)
		if err == nil && matched != nil {
			committedState, err = read(matched)
			matchedBlock = matched.Header.Number
		}
		stateDB.Close()
		if err != nil {
			return "", err
		}
	}
	if states[0].Nonce != 1 || states[1].Nonce != 2 || committedState == nil || committedState.Nonce != 2 {
		return "", fmt.Errorf("source/target/commit nonce contract failed")
	}
	if states[0].Balance.Cmp(states[1].Balance) != 0 || states[0].Balance.Cmp(committedState.Balance) != 0 {
		return "", fmt.Errorf("self-transfer balance contract failed")
	}
	return fmt.Sprintf("hash=%x target_block=%d canonical_count=1 source_count=0 source_nonce=1 target_nonce=2 commit_nonce=2 balance_equal=true", hash, matchedBlock), nil
}

func verifyNativePostRouteDisk(t *testing.T, root string, hash []byte) {
	t.Helper()
	result, err := checkNativePostRouteDisk(root, hash)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("DISK PASS " + result)
}

func TestNativePostRouteDiskReplay(t *testing.T) {
	root := os.Getenv("SAA_NATIVE_DISK_REPLAY_DIR")
	if root == "" {
		t.Skip("explicit stopped-node evidence replay only")
	}
	hash, err := hex.DecodeString("559727a28ae3b1d20722c0b087f5000f32b5db6e99edbe7591b35cd17e635238")
	if err != nil {
		t.Fatal(err)
	}
	t.Run("committed_transaction", func(t *testing.T) { verifyNativePostRouteDisk(t, root, hash) })
	t.Run("missing_transaction_rejected", func(t *testing.T) {
		wrong := make([]byte, 32)
		if _, err := checkNativePostRouteDisk(root, wrong); err == nil {
			t.Fatal("accepted nonexistent transaction")
		}
	})
}
