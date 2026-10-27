package core

import "testing"

func TestSingletonGroupJournalExtendAndRecover(t *testing.T) {
	a := serialAccounts(3)
	p := NewTxPool()
	dir := t.TempDir()
	p.AddTx2Pool(serialTx(a[0], a[2], 1))
	if err := p.EnableGroupMigrationQueueJournal(dir, a[:1]); err != nil {
		t.Fatal(err)
	}
	p.AddTx2Pool(serialTx(a[1], a[2], 2))
	if err := p.ExtendGroupMigrationQueueJournal(a[1:2]); err != nil {
		t.Fatal(err)
	}
	recovered := NewTxPool()
	if err := recovered.RestoreGroupMigrationQueueJournal(dir, a[:2]); err != nil {
		t.Fatal(err)
	}
	if len(recovered.pendingQueueLocked()) != 2 || len(recovered.PackTxs(10)) != 0 {
		t.Fatal("singleton groups lost holds or pending work on recovery")
	}
}
