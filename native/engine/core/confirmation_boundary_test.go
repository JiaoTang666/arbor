package core

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"testing"
	"time"
)

func TestCompletedHandoffForeignWireIDs(t *testing.T) {
	p, dir, journal, items := completedFixture(t, 1)
	h := items[0].H
	// A different wire type registration encodes identical transaction fields
	// with different gob descriptors, as happens across independent processes.
	type foreignTransaction Transaction
	var wire bytes.Buffer
	if err := gob.NewEncoder(&wire).Encode((*foreignTransaction)(DecodeTx(h.Original))); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(wire.Bytes(), h.Original) {
		t.Fatal("fixture did not change wire descriptors")
	}
	h.Original = wire.Bytes()
	sum := groupHandoffFingerprint(h)
	h.ID = hex.EncodeToString(sum[:])
	items[0].Ack.ID = h.ID
	if err := PersistMigrationHandoff(dir, h); err != nil {
		t.Fatal(err)
	}
	if err := p.ConfirmCompletedHandoffBatch(dir, items); err != nil {
		t.Fatal(err)
	}
	if p.GetTxQueueLen() != 2000 {
		t.Fatal("valid foreign wire receipt failed to remove its local queue entry")
	}
	fresh := NewTxPool()
	if err := fresh.EnableGroupMigrationQueueJournal(journal, p.migrationJournal.accounts); err != nil {
		t.Fatal(err)
	}
	if fresh.GetTxQueueLen() != 0 {
		t.Fatal("removed obligation returned after journal recovery")
	}
}

func TestConfirmationStageVisibleBeforeJournalWait(t *testing.T) {
	p, dir, _, items := completedFixture(t, 1)
	p.journalGate.Lock()
	waiting, done := make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := p.ConfirmCompletedHandoffBatchObserved(dir, items, func(stage string) {
			if stage == "journal_gate_wait" {
				close(waiting)
			}
		})
		done <- err
	}()
	select {
	case <-waiting:
	case <-time.After(2 * time.Second):
		p.journalGate.Unlock()
		<-done
		t.Fatal("blocked operation did not publish stage entry")
	}
	if packed := p.PackTxs(3000); len(packed) != 2000 {
		p.journalGate.Unlock()
		<-done
		t.Fatal("ordinary work blocked or held obligation executed")
	}
	if p.GetTxQueueLen() != 1 {
		p.journalGate.Unlock()
		<-done
		t.Fatal("premature dequeue")
	}
	p.journalGate.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if p.GetTxQueueLen() != 0 {
		t.Fatal("completed obligation not removed")
	}
}
