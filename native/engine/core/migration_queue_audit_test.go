package core

import (
	"math/big"
	"testing"
	"time"
)

func TestQueueAuditDetectsTrafficAfterDrainedGeneration(t *testing.T) {
	dir := t.TempDir()
	a := "0000000000000000000000000000000000000010"
	if _, err := AuditMigrationQueue(dir, a); err == nil {
		t.Fatal("missing journal reported drained")
	}
	p := NewTxPool()
	if err := p.EnableMigrationQueueJournal(dir, a); err != nil {
		t.Fatal(err)
	}
	before, err := AuditMigrationQueue(dir, a)
	if err != nil || before.Pending != 0 {
		t.Fatal(err)
	}
	p.AddTx2Pool(NewTransaction(a, a, big.NewInt(1), 1, time.Unix(1, 0)))
	after, err := AuditMigrationQueue(dir, a)
	if err != nil || after.Pending != 1 || after.Boundary.Generation <= before.Boundary.Generation || after.Boundary.SnapshotHash == before.Boundary.SnapshotHash {
		t.Fatal("late traffic hidden by historical drain")
	}
}
