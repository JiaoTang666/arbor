package core

import (
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMigrationJournalDeltaBoundsAndRecovery(t *testing.T) {
	a, b, c := strings.Repeat("0", 39)+"a", strings.Repeat("0", 39)+"b", strings.Repeat("0", 39)+"c"
	makeTx := func(sender, recipient string, n uint64) *Transaction {
		tx := NewTransaction(sender, recipient, big.NewInt(1), n, time.Unix(1, 0))
		tx.RoutingFrozen = true
		return tx
	}
	p := NewTxPool()
	dir := t.TempDir()
	for i := 0; i < 10000; i++ {
		p.AddTx2Pool(makeTx(c, c, uint64(i)))
	}
	p.AddTx2Pool(makeTx(a, b, 1))
	if e := p.EnableGroupMigrationQueueJournal(dir, []string{a, b}); e != nil {
		t.Fatal(e)
	}
	full, _ := os.Stat(filepath.Join(dir, "00000000000000000001.queue.json"))
	p.AddTxs2Pool([]*Transaction{makeTx(c, c, 10001), makeTx(a, c, 2), makeTx(b, c, 3)})
	addition, _ := os.ReadFile(filepath.Join(dir, "00000000000000000002.queue.json"))
	var delta migrationQueueSnapshot
	if e := json.Unmarshal(addition, &delta); e != nil || delta.Kind != "append" || len(delta.Transactions) != 2 || int64(len(addition)) >= full.Size()*2 {
		t.Fatal("append contains backlog or wrong entries", e)
	}
	p.AddTxs2Pool_Head([]*Transaction{makeTx(c, c, 10002), makeTx(a, c, 4)})
	head, _ := os.ReadFile(filepath.Join(dir, "00000000000000000003.queue.json"))
	if e := json.Unmarshal(head, &delta); e != nil || delta.Kind != "prepend" || len(delta.Transactions) != 1 {
		t.Fatal("prepend delta invalid", e)
	}
	if p.migrationJournal.sequence != 3 {
		t.Fatal("unexpected journal generations")
	}
	restored := NewTxPool()
	if e := restored.EnableGroupMigrationQueueJournal(dir, []string{a, b}); e != nil {
		t.Fatal(e)
	}
	if len(restored.pendingQueueLocked()) != 4 {
		t.Fatal("recovered queue size")
	}
	for i, want := range []uint64{4, 1, 2, 3} {
		if restored.pendingQueueLocked()[i].Nonce != want {
			t.Fatalf("order[%d]=%d, want %d", i, restored.pendingQueueLocked()[i].Nonce, want)
		}
	}
	if len(restored.PackTxs(100)) != 0 {
		t.Fatal("migration fence lost on recovery")
	}
}

func TestMigrationJournalDeltaConflictPreservesMemory(t *testing.T) {
	a := strings.Repeat("0", 39) + "a"
	p := NewTxPool()
	dir := t.TempDir()
	if e := p.EnableMigrationQueueJournal(dir, a); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(dir, "00000000000000000002.queue.json"), []byte("conflict"), 0600); e != nil {
		t.Fatal(e)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("delta conflict accepted")
			}
		}()
		p.AddTx2Pool(NewTransaction(a, a, big.NewInt(1), 1, time.Unix(1, 0)))
	}()
	if len(p.pendingQueueLocked()) != 0 || p.migrationJournal.sequence != 1 {
		t.Fatal("failed delta admitted transaction")
	}
}
