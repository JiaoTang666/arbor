package core

import "testing"

func TestSubsetDrainPreservesOtherGroupAndLateRelay(t *testing.T) {
	a := serialAccounts(5)
	p := NewTxPool()
	dir := t.TempDir()
	held := serialTx(a[0], a[1], 1)
	ordinary := serialTx(a[4], a[2], 2)
	ordinary.RoutingFrozen = true
	ordinary.RoutedSenderShard = 0
	ordinary.RoutedRecipientShard = 1
	p.AddTxs2Pool([]*Transaction{held, ordinary})
	if e := p.EnableGroupMigrationQueueJournal(dir, a[:2]); e != nil {
		t.Fatal(e)
	}
	if e := p.ExtendGroupMigrationQueueJournal(a[2:4]); e != nil {
		t.Fatal(e)
	}
	if raw, e := p.GroupDrainSnapshot(a[2:4]); e != nil || raw == nil {
		t.Fatal("ordinary debit blocked subset", e)
	}
	if got := p.PackTxs(10); len(got) != 1 || got[0] != ordinary {
		t.Fatal("ordinary debit lost")
	}
	fresh := NewTxPool()
	if e := fresh.RestoreGroupMigrationQueueJournal(dir, a[:4]); e != nil {
		t.Fatal(e)
	}
	if fresh.GetTxQueueLen() != 1 || len(fresh.PackTxs(10)) != 0 {
		t.Fatal("other group's recovery obligation lost")
	}
	late := serialTx(a[4], a[2], 3)
	late.Relayed = true
	late.RoutingFrozen = true
	p.AddTx2Pool(late)
	if raw, e := p.GroupDrainSnapshot(a[2:4]); e != nil || raw != nil {
		t.Fatal("late relay must block a new snapshot", e)
	}
	again := NewTxPool()
	if e := again.RestoreGroupMigrationQueueJournal(dir, a[:4]); e != nil {
		t.Fatal(e)
	}
	if again.GetTxQueueLen() != 2 || len(again.PackTxs(10)) != 0 {
		t.Fatal("late relay bypassed durable fence")
	}
}
