package core

import (
	"encoding/json"
	"runtime"
	"testing"
	"time"
)

func waitGate(t *testing.T, g *journalGate, control, admission uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		g.mu.Lock()
		c, a := g.next[0]-g.head[0], g.next[1]-g.head[1]
		g.mu.Unlock()
		if c == control && a == admission {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("gate waiters=%d,%d wanted=%d,%d", c, a, control, admission)
		}
		runtime.Gosched()
	}
}

func TestJournalGateAlternatesWithoutAdmissionBacklogStarvation(t *testing.T) {
	var g journalGate
	g.Lock()
	order := make(chan string, 6)
	release := make(chan struct{})
	start := func(name string, admission bool) {
		go func() {
			if admission {
				g.LockAdmission()
			} else {
				g.Lock()
			}
			order <- name
			<-release
			g.Unlock()
		}()
	}
	for i, name := range []string{"a1", "a2", "a3"} {
		start(name, true)
		waitGate(t, &g, 0, uint64(i+1))
	}
	for i, name := range []string{"c1", "c2", "c3"} {
		start(name, false)
		waitGate(t, &g, uint64(i+1), 3)
	}
	g.Unlock()
	for _, want := range []string{"a1", "c1", "a2", "c2", "a3", "c3"} {
		select {
		case got := <-order:
			if got != want {
				close(release)
				t.Fatalf("order=%s wanted=%s", got, want)
			}
			if g.TryLock() {
				t.Fatal("entered while owner active")
			}
			release <- struct{}{}
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("waiter did not advance")
		}
	}
}

func TestJournalConfirmationBypassesQueuedAdmissionsAndRecovers(t *testing.T) {
	p, dir, journal, items := completedFixture(t, 1)
	p.journalGate.Lock()
	added := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		tx := DecodeTx(items[0].H.Original)
		tx.Nonce += uint64(100 + i)
		go func() { p.AddTx2Pool(tx); added <- struct{}{} }()
		waitGate(t, &p.journalGate, 0, uint64(i+1))
	}
	done := make(chan error, 1)
	go func() { done <- p.ConfirmCompletedHandoffBatch(dir, items) }()
	waitGate(t, &p.journalGate, 1, 2)
	kinds := make(chan string, 3)
	p.migrationJournalPublish = func(path string, raw []byte) error {
		var s migrationQueueSnapshot
		if err := json.Unmarshal(raw, &s); err != nil {
			return err
		}
		kinds <- s.Kind
		return PublishSourcePrepareFile(path, raw)
	}
	p.journalGate.Unlock()
	for i := 0; i < 2; i++ {
		select {
		case <-added:
		case <-time.After(3 * time.Second):
			t.Fatal("admission stuck")
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"append", confirmRemoveKind, "append"} {
		if got := <-kinds; got != want {
			t.Fatalf("%s instead of %s", got, want)
		}
	}
	fresh := NewTxPool()
	if err := fresh.EnableGroupMigrationQueueJournal(journal, items[0].H.GroupAccounts); err != nil {
		t.Fatal(err)
	}
	if fresh.GetTxQueueLen() != 2 || p.GetTxQueueLen() != 2002 {
		t.Fatal("dequeue/admission recovery mismatch")
	}
	if err := fresh.ConfirmCompletedHandoffBatch(dir, items); err != nil {
		t.Fatal(err)
	}
	if fresh.GetTxQueueLen() != 2 {
		t.Fatal("duplicate completion changed recovered work")
	}
}
