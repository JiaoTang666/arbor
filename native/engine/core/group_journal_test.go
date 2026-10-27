package core

import (
	"math/big"
	"testing"
	"time"
)

func TestGroupJournalUnionAndLateRelayRecovery(t *testing.T) {
	a, b, c := "0000000000000000000000000000000000000010", "0000000000000000000000000000000000000020", "0000000000000000000000000000000000000030"
	dir := t.TempDir()
	p := NewTxPool()
	inside := NewTransaction(a, b, big.NewInt(1), 1, time.Unix(1, 0))
	p.AddTxs2Pool([]*Transaction{inside})
	if err := p.EnableGroupMigrationQueueJournal(dir, []string{a, b}); err != nil {
		t.Fatal(err)
	}
	relay := NewTransaction(c, b, big.NewInt(1), 2, time.Unix(1, 0))
	relay.Relayed = true
	unrelated := NewTransaction(c, c, big.NewInt(1), 3, time.Unix(1, 0))
	p.AddTxs2Pool([]*Transaction{relay, unrelated})
	q := NewTxPool()
	if err := q.EnableGroupMigrationQueueJournal(dir, []string{a, b}); err != nil {
		t.Fatal(err)
	}
	if len(q.pendingQueueLocked()) != 2 {
		t.Fatal("union duplicated internal transaction or lost late relay", len(q.pendingQueueLocked()))
	}
	if len(q.PackTxs(100)) != 0 {
		t.Fatal("recovery failed to restore both holds")
	}
	if len(p.PackTxs(100)) != 1 {
		t.Fatal("unrelated transaction blocked")
	}
	wrong := NewTxPool()
	if err := wrong.EnableMigrationQueueJournal(dir, a); err == nil {
		t.Fatal("group journal opened as singleton")
	}
}
