package core

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func seedLegacyOrdinaryDelta(t *testing.T, p *TxPool, tx *Transaction) {
	t.Helper()
	j := p.migrationJournal
	s := migrationQueueSnapshot{Account: j.account, Accounts: j.accounts, Sequence: j.sequence + 1, Kind: "append", Transactions: [][]byte{tx.Encode()}}
	raw, e := json.Marshal(s)
	if e != nil {
		t.Fatal(e)
	}
	if e = PublishSourcePrepareFile(filepath.Join(j.directory, fmt.Sprintf("%020d.queue.json", s.Sequence)), raw); e != nil {
		t.Fatal(e)
	}
	j.sequence = s.Sequence
}

func TestNewRouteArrivalDuringDrainPublicationRemainsExecutable(t *testing.T) {
	a, b, c := strings.Repeat("0", 39)+"a", strings.Repeat("0", 39)+"b", strings.Repeat("0", 39)+"c"
	p := NewTxPool()
	dir := t.TempDir()
	accounts := []string{a, b}
	if e := p.EnableGroupMigrationQueueJournal(dir, accounts); e != nil {
		t.Fatal(e)
	}
	tx := isolationTx(c, 1)
	tx.Recipient = a
	tx.RoutingFrozen = true
	tx.RoutedRecipientShard = 1
	seedLegacyOrdinaryDelta(t, p, tx)
	started, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	p.migrationJournalPublish = func(path string, raw []byte) error {
		close(started)
		<-release
		return PublishSourcePrepareFile(path, raw)
	}
	done := make(chan []byte, 1)
	go func() {
		raw, e := p.GroupDrainSnapshot(accounts)
		if e != nil {
			t.Error(e)
		}
		done <- raw
	}()
	awaitIsolation(t, started, "drain did not publish legacy normalization")
	admitted := make(chan struct{})
	go func() { p.AddTx2Pool(tx); close(admitted) }()
	awaitIsolation(t, admitted, "new-route arrival blocked on drain")
	close(release)
	select {
	case raw := <-done:
		if raw == nil {
			t.Fatal("ordinary contact incorrectly blocked recovery drain")
		}
	case <-time.After(time.Second):
		t.Fatal("drain stuck")
	}
	if len(p.PackTxs(1)) != 1 {
		t.Fatal("new-route work lost")
	}
	if raw, e := p.GroupDrainSnapshot(accounts); e != nil || raw == nil {
		t.Fatal("empty drain not restored", e)
	}
}

// A new frozen cross-shard route needs normal sender execution, not old-route
// handoff recovery. Its entire injection message must bypass a busy journal.
func TestNewRouteMixedAdmissionBypassesMigrationJournal(t *testing.T) {
	a, b := strings.Repeat("0", 39)+"a", strings.Repeat("0", 39)+"b"
	p, dir := NewTxPool(), t.TempDir()
	if e := p.EnableMigrationQueueJournal(dir, a); e != nil {
		t.Fatal(e)
	}
	newRoute := isolationTx(b, 1)
	newRoute.Recipient = a
	newRoute.RoutingFrozen = true
	newRoute.RoutedRecipientShard = 1
	companion := isolationTx(b, 2)
	p.journalGate.Lock()
	done := make(chan struct{})
	go func() { p.AddTxs2Pool([]*Transaction{newRoute, companion}); close(done) }()
	select {
	case <-done:
		p.journalGate.Unlock()
	case <-time.After(200 * time.Millisecond):
		p.journalGate.Unlock()
		<-done
		t.Fatal("new-route mixed message waited for migration journal")
	}
	packed := p.PackTxs(10)
	if len(packed) != 2 || packed[0] != newRoute || packed[1] != companion {
		t.Fatal("ordinary FIFO changed")
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.queue.json"))
	if len(files) != 1 {
		t.Fatal("normal cross-shard execution created migration journal delta")
	}
	fresh := NewTxPool()
	if e := fresh.EnableMigrationQueueJournal(dir, a); e != nil || fresh.GetTxQueueLen() != 0 {
		t.Fatal("normal execution incorrectly replayed as migration obligation", e)
	}
}

func TestMigrationRecoveryScopePreservesActualObligations(t *testing.T) {
	a, b, c := strings.Repeat("0", 39)+"a", strings.Repeat("0", 39)+"b", strings.Repeat("0", 39)+"c"
	p, dir := NewTxPool(), t.TempDir()
	oldInbound := isolationTx(c, 1)
	oldInbound.Recipient = a
	oldInbound.RoutingFrozen = true
	newInbound := isolationTx(c, 2)
	newInbound.Recipient = a
	newInbound.RoutingFrozen = true
	newInbound.RoutedRecipientShard = 1
	lateCredit := isolationTx(c, 3)
	lateCredit.Recipient = a
	lateCredit.Relayed = true
	lateCredit.RoutingFrozen = true
	lateCredit.RoutedSenderShard = 1
	senderHeld := isolationTx(a, 4)
	senderHeld.Recipient = c
	senderHeld.RoutingFrozen = true
	senderHeld.RoutedRecipientShard = 1
	bothMembers := isolationTx(b, 5)
	bothMembers.Recipient = a
	bothMembers.RoutingFrozen = true
	bothMembers.RoutedRecipientShard = 1
	unfrozen := isolationTx(c, 6)
	unfrozen.Recipient = a
	unfrozen.RoutedRecipientShard = 1
	p.AddTxs2Pool([]*Transaction{oldInbound, newInbound})
	if e := p.EnableGroupMigrationQueueJournal(dir, []string{a, b}); e != nil {
		t.Fatal(e)
	}
	p.AddTxs2Pool([]*Transaction{lateCredit, senderHeld, bothMembers, unfrozen})
	packed := p.PackTxs(20)
	if len(packed) != 1 || packed[0] != newInbound {
		t.Fatal("wrong ordinary execution eligibility")
	}
	if p.GetTxQueueLen() != 5 {
		t.Fatal("lost held obligations")
	}
	fresh := NewTxPool()
	if e := fresh.EnableGroupMigrationQueueJournal(dir, []string{a, b}); e != nil {
		t.Fatal(e)
	}
	if fresh.GetTxQueueLen() != 5 || len(fresh.PackTxs(20)) != 0 {
		t.Fatal("recovery lost or executed held work")
	}
}
