package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/trie"
)

// Opt-in forensic reader. Both original chain databases are opened read-only;
// queue recovery operates on private temporary files, never the stopped run.
func TestStoppedCompletionAudit(t *testing.T) {
	run := os.Getenv("SAA_COMPLETION_AUDIT_RUN")
	if run == "" {
		t.Skip("requires stopped native run")
	}
	t.Setenv(NativeEffectIndexEnv, "true")
	open := func(shard int) *BlockChain {
		t.Helper()
		base := filepath.Join(run, "runtime/database")
		blocks, err := bolt.Open(filepath.Join(base, "chainDB", fmt.Sprintf("S%d_N0", shard)), 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		defer blocks.Close()
		var block *core.Block
		err = blocks.View(func(tx *bolt.Tx) error {
			key := tx.Bucket([]byte("newestBlockHash")).Get([]byte("OnlyNewestBlock"))
			block = core.DecodeB(tx.Bucket([]byte("block")).Get(key))
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		db, err := rawdb.NewLevelDBDatabase(filepath.Join(base, "mptDB/ldb", fmt.Sprintf("s%d", shard), "n0"), 16, 16, "audit", true)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return &BlockChain{CurrentBlock: block, ChainConfig: &params.ChainConfig{ShardID: uint64(shard), ShardNums: 16}, PartitionMap: make(map[string]uint64), Txpool: core.NewTxPool(), db: db, triedb: trie.NewDatabase(db)}
	}
	source, target := open(3), open(14)
	if err := source.restoreGroupSourcePreparation(); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(run, "SAA_NATIVE_SOURCE_QUEUE_ROOT/s3-n0")
	copyDir := t.TempDir()
	files, err := os.ReadDir(journal)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if f.Type().IsRegular() {
			raw, err := os.ReadFile(filepath.Join(journal, f.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(copyDir, f.Name()), raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	raw, err := os.ReadFile(filepath.Join(copyDir, "00000000000000000001.queue.json"))
	if err != nil {
		t.Fatal(err)
	}
	var first struct{ Accounts []string }
	if err = json.Unmarshal(raw, &first); err != nil {
		t.Fatal(err)
	}
	pool := core.NewTxPool()
	pool.EnableIndexedMigrationAdmission()
	if err = pool.EnableGroupMigrationQueueJournal(copyDir, first.Accounts); err != nil {
		t.Fatal(err)
	}
	pending := pool.SnapshotGroupMigrationTransactions(first.Accounts)
	identities := func(txs []*core.Transaction) map[string]int {
		result := make(map[string]int, len(txs))
		for _, tx := range txs {
			result[string(tx.Encode())]++
		}
		return result
	}
	expectedRemaining := identities(pending)
	sst, err := source.stateTrie()
	if err != nil {
		t.Fatal(err)
	}
	tst, err := target.stateTrie()
	if err != nil {
		t.Fatal(err)
	}
	errors := 0
	for _, tx := range pending {
		_, err := source.CollectSourceAppliedCredits([]*core.Transaction{tx}, 1)
		if err == nil {
			continue
		}
		errors++
		if errors > 3 {
			continue
		}
		sd, _ := ReadTransactionEffectReceipt(sst, tx, core.TransactionDebit)
		sc, _ := ReadTransactionEffectReceipt(sst, tx, core.TransactionCredit)
		td, _ := ReadTransactionEffectReceipt(tst, tx, core.TransactionDebit)
		block := func(r *TransactionEffectReceipt) uint64 {
			if r == nil {
				return 0
			}
			return r.Block
		}
		row := map[string]interface{}{"error": err.Error(), "admission_id": tx.AdmissionID, "sender": tx.Sender, "recipient": tx.Recipient, "source_debit_block": block(sd), "source_credit_block": block(sc), "target_debit_block": block(td), "source_height": source.CurrentBlock.Header.Number, "target_height": target.CurrentBlock.Header.Number}
		encoded, _ := json.Marshal(row)
		t.Log(string(encoded))
	}
	t.Logf("pending=%d completion_errors=%d", len(pending), errors)
	if errors != 0 {
		t.Fatalf("unresolved completion classification errors: %d", errors)
	}
	if pool.GetTxQueueLen() != len(pending) {
		t.Fatal("read-only classification removed work")
	}
	// Replay only genuine complete ACKs on private queue/outbox copies. This
	// checks the downstream path, not just suppression of the earlier error.
	outbox := t.TempDir()
	entries, err := os.ReadDir(filepath.Join(run, "SAA_NATIVE_HANDOFF_OUTBOX_DIR"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Type().IsRegular() {
			raw, e := os.ReadFile(filepath.Join(run, "SAA_NATIVE_HANDOFF_OUTBOX_DIR", entry.Name()))
			if e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(filepath.Join(outbox, entry.Name()), raw, 0600); e != nil {
				t.Fatal(e)
			}
		}
	}
	records, err := core.LoadMigrationArtifacts(outbox, ".handoff.json", false)
	if err != nil {
		t.Fatal(err)
	}
	ackDir := filepath.Join(run, "SAA_NATIVE_HANDOFF_ACK_DIR")
	acks, err := core.LoadMigrationArtifacts(ackDir, ".ack.json", false)
	if err != nil {
		t.Fatal(err)
	}
	relays, err := core.LoadMigrationArtifacts(ackDir, ".relay-ack.json", false)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]*core.Transaction{}
	for _, tx := range pending {
		keys[string(tx.Encode())] = tx
	}
	var completed []core.CompletedHandoffConfirmation
	for _, id := range records.IDs() {
		var h core.MigrationHandoff
		if err = json.Unmarshal(records[id], &h); err != nil {
			t.Fatal(err)
		}
		original := core.DecodeTx(h.Original)
		var inbound *core.Transaction
		key := string(original.Encode())
		if keys[key] == nil && original.Relayed {
			original.Relayed = false
			key = string(original.Encode())
			inbound = keys[key]
		}
		if keys[key] == nil {
			continue
		}
		raw, ok := acks[id]
		if !ok {
			continue
		}
		item := core.CompletedHandoffConfirmation{H: &h, InboundOriginal: inbound}
		if err = json.Unmarshal(raw, &item.Ack); err != nil {
			t.Fatal(err)
		}
		if item.Ack.RelayPending {
			raw, ok = relays[id]
			if !ok {
				continue
			}
			item.Relay = &core.HandoffRelayAck{}
			if err = json.Unmarshal(raw, item.Relay); err != nil {
				t.Fatal(err)
			}
		}
		completed = append(completed, item)
		removed := core.DecodeTx(h.Original)
		if item.InboundOriginal != nil {
			removed = item.InboundOriginal
		}
		wire := string(removed.Encode())
		if expectedRemaining[wire] != 1 {
			t.Fatal("ambiguous or repeated confirmed occurrence")
		}
		delete(expectedRemaining, wire)
	}
	for i := 0; i < len(completed); i += 256 {
		end := i + 256
		if end > len(completed) {
			end = len(completed)
		}
		if err = pool.ConfirmCompletedHandoffBatch(outbox, completed[i:end]); err != nil {
			t.Fatal(err)
		}
	}
	want := len(pending) - len(completed)
	if pool.GetTxQueueLen() != want {
		t.Fatal("real ACK dequeue count mismatch")
	}
	fresh := core.NewTxPool()
	fresh.EnableIndexedMigrationAdmission()
	if err = fresh.EnableGroupMigrationQueueJournal(copyDir, first.Accounts); err != nil {
		t.Fatal(err)
	}
	if fresh.GetTxQueueLen() != want {
		t.Fatal("real ACK removal not recovered")
	}
	if !reflect.DeepEqual(identities(pool.SnapshotGroupMigrationTransactions(first.Accounts)), expectedRemaining) ||
		!reflect.DeepEqual(identities(fresh.SnapshotGroupMigrationTransactions(first.Accounts)), expectedRemaining) {
		t.Fatal("remaining occurrence identities differ after confirmation/recovery")
	}
	for i := 0; i < len(completed); i += 256 {
		end := i + 256
		if end > len(completed) {
			end = len(completed)
		}
		if err = fresh.ConfirmCompletedHandoffBatch(outbox, completed[i:end]); err != nil {
			t.Fatal(err)
		}
	}
	if fresh.GetTxQueueLen() != want {
		t.Fatal("repeat ACK changed remaining work")
	}
	if !reflect.DeepEqual(identities(fresh.SnapshotGroupMigrationTransactions(first.Accounts)), expectedRemaining) {
		t.Fatal("repeat ACK changed remaining occurrence identities")
	}
	t.Logf("real_complete_ACKs=%d remaining=%d recovered=%d repeated_ACKs_idempotent=true identity_sets_equal=true", len(completed), want, fresh.GetTxQueueLen())
}
