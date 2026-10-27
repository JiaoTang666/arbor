package core

import (
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMigrationBatchOnePublicationAndRecovery(t *testing.T) {
	a := "0000000000000000000000000000000000000010"
	b := "0000000000000000000000000000000000000020"
	p := NewTxPool()
	dir := t.TempDir()
	if e := p.EnableMigrationQueueJournal(dir, a); e != nil {
		t.Fatal(e)
	}
	batch := make([]*Transaction, 400)
	for i := range batch {
		account := b
		if i < 80 {
			account = a
		}
		batch[i] = NewTransaction(account, account, big.NewInt(1), uint64(i+1), time.Unix(1, 0))
	}
	p.AddTxs2Pool(batch)
	if p.migrationJournal.sequence != 2 || len(p.pendingQueueLocked()) != 400 {
		t.Fatal("batch not published exactly once")
	}
	for i, tx := range batch {
		if p.pendingQueueLocked()[i] != tx {
			t.Fatal("batch order changed")
		}
	}
	again := NewTxPool()
	if e := again.EnableMigrationQueueJournal(dir, a); e != nil {
		t.Fatal(e)
	}
	if len(again.pendingQueueLocked()) != 80 || len(again.PackTxs(100)) != 0 {
		t.Fatal("restored migration entries lost or unfenced")
	}
	for i, tx := range again.pendingQueueLocked() {
		if tx.Nonce != uint64(i+1) {
			t.Fatal("restored order changed")
		}
	}
	p.AddTxs2Pool(batch[80:])
	p.AddTxs2Pool(nil)
	if p.migrationJournal.sequence != 2 || len(p.pendingQueueLocked()) != 720 {
		t.Fatal("unrelated batch rewrote journal")
	}
}
func TestMigrationBatchFailureDoesNotPublishPrefix(t *testing.T) {
	a := "0000000000000000000000000000000000000010"
	p := NewTxPool()
	dir := t.TempDir()
	if e := p.EnableMigrationQueueJournal(dir, a); e != nil {
		t.Fatal(e)
	}
	tx := NewTransaction(a, a, big.NewInt(1), 1, time.Unix(1, 0))
	panics := func(batch []*Transaction) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Error("expected failure")
			}
		}()
		p.AddTxs2Pool(batch)
	}
	panics([]*Transaction{tx, nil})
	if len(p.pendingQueueLocked()) != 0 || p.migrationJournal.sequence != 1 {
		t.Fatal("invalid batch admitted prefix")
	}
	if e := os.WriteFile(filepath.Join(dir, "00000000000000000002.queue.json"), []byte("conflict"), 0600); e != nil {
		t.Fatal(e)
	}
	panics([]*Transaction{tx, tx})
	if len(p.pendingQueueLocked()) != 0 || p.migrationJournal.sequence != 1 {
		t.Fatal("failed publication changed queue")
	}
}
