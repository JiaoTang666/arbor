package committee

import (
	"blockEmulator/core"
	"bytes"
	"encoding/hex"
	"encoding/json"
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

func TestCostPairDiskAudit(t *testing.T) {
	root := os.Getenv("COST_PAIR_AUDIT_DIR")
	if root == "" {
		t.Skip("explicit stopped-run disk audit")
	}
	raw, e := os.ReadFile(filepath.Join(root, "result.json"))
	if e != nil {
		t.Fatal(e)
	}
	var result struct {
		Pass bool `json:"pass"`
	}
	if json.Unmarshal(raw, &result) != nil || !result.Pass {
		t.Fatal("native run did not pass")
	}
	raw, e = os.ReadFile(filepath.Join(root, "timing.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	var hot uint64
	for _, line := range bytes.Split(raw, []byte("\n")) {
		var x struct {
			Event   string `json:"event"`
			Details struct {
				Hot uint64 `json:"hot_account_input"`
			} `json:"details"`
		}
		if json.Unmarshal(line, &x) == nil && x.Event == "all_completed" {
			hot = x.Details.Hot
		}
	}
	if hot == 0 {
		t.Fatal("no completion count")
	}
	account := fmt.Sprintf("%040x", 16)
	move := filepath.Base(root) == "native_cost_move"
	var prepared core.SourcePreparation
	if move {
		raw, e = os.ReadFile(filepath.Join(root, "control/source_prepared.json"))
		if e != nil {
			t.Fatal(e)
		}
		if e = json.Unmarshal(raw, &prepared); e != nil {
			t.Fatal(e)
		}
	}
	ackFiles, _ := filepath.Glob(filepath.Join(root, "ack", "*.ack.json"))
	acks := []core.HandoffAck{}
	for _, p := range ackFiles {
		raw, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		var a core.HandoffAck
		if e = json.Unmarshal(raw, &a); e != nil {
			t.Fatal(e)
		}
		acks = append(acks, a)
	}
	roots := map[string]string{}
	heads := [2][4]uint64{}
	out := []map[string]interface{}{}
	for s := 0; s < 2; s++ {
		for n := 0; n < 4; n++ {
			db, e := bolt.Open(filepath.Join(root, fmt.Sprintf("exp/database/chainDB/S%d_N%d", s, n)), 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
			if e != nil {
				t.Fatal(e)
			}
			var head *core.Block
			e = db.View(func(tx *bolt.Tx) error {
				bs := tx.Bucket([]byte("block"))
				head = core.DecodeB(bs.Get(tx.Bucket([]byte("newestBlockHash")).Get([]byte("OnlyNewestBlock"))))
				b := head
				for {
					key := fmt.Sprintf("%d:%d", s, b.Header.Number)
					state := hex.EncodeToString(b.Header.StateRoot)
					if old, ok := roots[key]; ok && old != state {
						return fmt.Errorf("same-height replica state root mismatch %s", key)
					}
					roots[key] = state
					if b.Header.Number == 0 {
						break
					}
					b = core.DecodeB(bs.Get(b.Header.ParentBlockHash))
				}
				if s == 1 {
					for _, ack := range acks {
						hash, e := hex.DecodeString(ack.BlockHash)
						if e != nil {
							return e
						}
						raw := bs.Get(hash)
						if len(raw) == 0 {
							return fmt.Errorf("ack block missing")
						}
						b := core.DecodeB(raw)
						if b.Header.Number != ack.Block || hex.EncodeToString(b.Header.StateRoot) != ack.StateRoot {
							return fmt.Errorf("ack block binding mismatch")
						}
					}
				}
				return nil
			})
			db.Close()
			if e != nil {
				t.Fatal(e)
			}
			heads[s][n] = head.Header.Number
			state, e := rawdb.NewLevelDBDatabase(filepath.Join(root, fmt.Sprintf("exp/database/mptDB/ldb/s%d/n%d", s, n)), 0, 1, "audit", true)
			if e != nil {
				t.Fatal(e)
			}
			st, e := trie.New(trie.TrieID(common.BytesToHash(head.Header.StateRoot)), trie.NewDatabase(state))
			if e != nil {
				t.Fatal(e)
			}
			raw, e := st.Get([]byte(account))
			if e != nil {
				t.Fatal(e)
			}
			ac := &core.AccountState{}
			if len(raw) > 0 {
				ac = core.DecodeAS(raw)
			}
			if move && s == 0 && !bytes.Equal(raw, prepared.State) {
				t.Fatal("source state mutated after prepare")
			}
			if ((move && s == 1) || (!move && s == 0)) && (len(raw) == 0 || ac.Nonce != hot || ac.Balance.String() != "1000000000000000000") {
				t.Fatal("target count/balance mismatch", ac.Nonce, hot)
			}
			if !move && s == 1 && len(raw) != 0 {
				t.Fatal("unexpected hot account on keep target")
			}
			background := 32
			want := uint64(44 * 320)
			if s == 1 {
				background = 33
				want = 44 * 40
			}
			backgroundRaw, err := st.Get([]byte(fmt.Sprintf("%040x", background)))
			if err != nil || len(backgroundRaw) == 0 {
				t.Fatal("missing background state", err)
			}
			backgroundState := core.DecodeAS(backgroundRaw)
			if backgroundState.Nonce != want || backgroundState.Balance.String() != "1000000000000000000" {
				t.Fatal("background count/balance mismatch")
			}
			if s == 1 {
				for _, ack := range acks {
					raw, e := st.Get([]byte("__sef_handoff_executed_v1__" + ack.ID))
					if e != nil || len(raw) == 0 {
						t.Fatal("durable dedup marker missing")
					}
				}
			}
			state.Close()
			out = append(out, map[string]interface{}{"shard": s, "node": n, "height": head.Header.Number, "hot_nonce": ac.Nonce, "background_nonce": backgroundState.Nonce})
		}
	}
	raw, _ = json.MarshalIndent(map[string]interface{}{"status": "PASS", "replicas": out, "handoff_acks_verified_per_target_replica": len(acks), "hot_transactions": hot, "same_height_roots": "matched at all shared committed heights"}, "", "  ")
	if e = os.WriteFile(filepath.Join(root, "disk_audit.json"), raw, 0600); e != nil {
		t.Fatal(e)
	}
}
