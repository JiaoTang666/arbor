package core

import (
	"bytes"
	"math/big"
	"testing"
	"time"
)

func TestHandoffOutboxPersistsWithoutDequeuing(t *testing.T) {
	a := "0000000000000000000000000000000000000010"
	b := "0000000000000000000000000000000000000011"
	tx := NewTransaction(a, b, big.NewInt(3), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	tx.RoutedRecipientShard = 1
	pool := NewTxPool()
	pool.AddTx2Pool(tx)
	pool.HoldSourceAccount(a)
	snapshot := pool.SnapshotMigrationTransactions(a)
	if len(snapshot) != 1 || snapshot[0] == tx {
		t.Fatal("snapshot not independent")
	}
	h, err := BuildMigrationHandoff(snapshot[0], "p", a, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err = PersistMigrationHandoff(dir, h); err != nil {
		t.Fatal(err)
	}
	if err = PersistMigrationHandoff(dir, h); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadMigrationOutbox(dir)
	if err != nil || len(loaded) != 1 || !bytes.Equal(loaded[0].Execution, h.Execution) {
		t.Fatal("outbox not recoverable")
	}
	if pool.GetTxQueueLen() != 1 || len(pool.PackTxs(1)) != 0 {
		t.Fatal("persisting handoff dequeued or released source")
	}
	loaded[0].Execution = []byte("changed")
	if PersistMigrationHandoff(dir, loaded[0]) == nil {
		t.Fatal("changed payload accepted")
	}
}
