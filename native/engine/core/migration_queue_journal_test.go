package core

import (
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMigrationQueueJournalRestoresBacklogLateAndHead(t *testing.T) {
	a := "0000000000000000000000000000000000000010"
	makeTx := func(n uint64) *Transaction {
		tx := NewTransaction(a, a, big.NewInt(1), n, time.Unix(1, 0))
		tx.RoutingFrozen = true
		return tx
	}
	dir := t.TempDir()
	p := NewTxPool()
	p.AddTx2Pool(makeTx(1))
	if err := p.EnableMigrationQueueJournal(dir, a); err != nil {
		t.Fatal(err)
	}
	p.AddTxs2Pool([]*Transaction{makeTx(2)})
	p.AddTxs2Pool_Head([]*Transaction{makeTx(3)})
	restored := NewTxPool()
	if err := restored.EnableMigrationQueueJournal(dir, a); err != nil {
		t.Fatal(err)
	}
	if len(restored.pendingQueueLocked()) != 3 {
		t.Fatal("lost backlog or late traffic")
	}
	for i, n := range []uint64{3, 1, 2} {
		if restored.pendingQueueLocked()[i].Nonce != n {
			t.Fatal("queue order changed")
		}
	}
	if len(restored.PackTxs(3)) != 0 {
		t.Fatal("restored migration entries were not fenced")
	}
	// Simulate a completed durable dequeue, then reconstruct the pool again.
	restored.lock.Lock()
	err := restored.persistMigrationQueueLocked(restored.pendingQueueLocked()[1:])
	restored.lock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	again := NewTxPool()
	if err = again.EnableMigrationQueueJournal(dir, a); err != nil || len(again.pendingQueueLocked()) != 2 {
		t.Fatalf("dequeue not durable: %v", err)
	}
}

func TestMigrationQueueJournalWriteFailureRejectsAdmission(t *testing.T) {
	dir := t.TempDir()
	p := NewTxPool()
	a := "0000000000000000000000000000000000000010"
	if err := p.EnableMigrationQueueJournal(dir, a); err != nil {
		t.Fatal(err)
	}
	// Reserve the next generation with conflicting content, avoiding permission-dependent tests.
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000002.queue.json"), []byte("conflict"), 0600); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("write failure did not stop admission")
			}
		}()
		p.AddTx2Pool(NewTransaction(a, a, big.NewInt(1), 1, time.Unix(1, 0)))
	}()
	if p.GetTxQueueLen() != 0 {
		t.Fatal("failed admission changed memory queue")
	}
}
