package core

import (
	"testing"
	"time"
)

func TestExistingSourceFenceDoesNotWaitForJournalIO(t *testing.T) {
	p := NewTxPool()
	accounts := []string{"000000000000000000000000000000000000000a"}
	if err := p.EnableGroupMigrationQueueJournal(t.TempDir(), accounts); err != nil {
		t.Fatal(err)
	}
	p.journalGate.Lock() // another admission's durable I/O is outstanding
	done := make(chan struct{})
	go func() { p.HoldSourceAccounts(accounts); close(done) }()
	select {
	case <-done:
		p.journalGate.Unlock()
	case <-time.After(200 * time.Millisecond):
		p.journalGate.Unlock()
		<-done
		t.Fatal("already installed fence waited for unrelated journal I/O")
	}
	if !p.heldMigrationAccounts[accounts[0]] {
		t.Fatal("fence lost")
	}
}

func TestNewSourceFenceStillSerializesWithJournal(t *testing.T) {
	p := NewTxPool()
	p.journalGate.Lock()
	done := make(chan struct{})
	go func() { p.HoldSourceAccount("000000000000000000000000000000000000000a"); close(done) }()
	select {
	case <-done:
		p.journalGate.Unlock()
		t.Fatal("new fence bypassed journal serialization")
	case <-time.After(20 * time.Millisecond):
	}
	p.journalGate.Unlock()
	<-done
}
