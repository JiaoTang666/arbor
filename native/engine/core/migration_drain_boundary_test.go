package core

import (
	"math/big"
	"testing"
	"time"
)

func TestDrainBoundaryDoesNotReleaseLateTraffic(t *testing.T) {
	a := "0000000000000000000000000000000000000010"
	p := NewTxPool()
	if _, err := p.MigrationDrainBoundary(a); err == nil {
		t.Fatal("volatile empty queue accepted")
	}
	dir := t.TempDir()
	if err := p.EnableMigrationQueueJournal(dir, a); err != nil {
		t.Fatal(err)
	}
	first, err := p.MigrationDrainBoundary(a)
	if err != nil || first.Generation != 1 {
		t.Fatal(err)
	}
	late := NewTransaction(a, a, big.NewInt(1), 1, time.Unix(1, 0))
	p.AddTx2Pool(late)
	if _, err := p.MigrationDrainBoundary(a); err == nil {
		t.Fatal("late arrival hidden behind old boundary")
	}
	if len(p.PackTxs(1)) != 0 {
		t.Fatal("drain boundary released source")
	}
	restored := NewTxPool()
	if err := restored.EnableMigrationQueueJournal(dir, a); err != nil {
		t.Fatal(err)
	}
	if restored.GetTxQueueLen() != 1 || len(restored.PackTxs(1)) != 0 {
		t.Fatal("late traffic not retained across recovery")
	}
}
