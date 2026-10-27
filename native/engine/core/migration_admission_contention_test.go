package core

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHeadOrdinaryContactsBypassBusyJournalAndPreserveOrder(t *testing.T) {
	a := serialAccounts(3)
	p, dir := NewTxPool(), t.TempDir()
	if err := p.EnableGroupMigrationQueueJournal(dir, a[:2]); err != nil {
		t.Fatal(err)
	}
	tail := serialTx(a[2], a[2], 3)
	p.AddTx2Pool(tail)
	newRoute := serialTx(a[2], a[0], 1)
	newRoute.RoutingFrozen, newRoute.RoutedRecipientShard = true, 1
	companion := serialTx(a[2], a[2], 2)
	p.journalGate.Lock()
	done := make(chan struct{})
	go func() { p.AddTxs2Pool_Head([]*Transaction{newRoute, companion}); close(done) }()
	select {
	case <-done:
		p.journalGate.Unlock()
	case <-time.After(time.Second):
		p.journalGate.Unlock()
		<-done
		t.Fatal("ordinary head waited for migration persistence")
	}
	packed := p.PackTxs(10)
	if len(packed) != 3 || packed[0] != newRoute || packed[1] != companion || packed[2] != tail {
		t.Fatal("head/tail admission order changed")
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.queue.json"))
	if len(files) != 1 {
		t.Fatal("ordinary head created a recovery generation")
	}
	fresh := NewTxPool()
	if err := fresh.EnableGroupMigrationQueueJournal(dir, a[:2]); err != nil || fresh.GetTxQueueLen() != 0 {
		t.Fatal("ordinary contact replayed as recovery debt", err)
	}
}

func TestHeadMixedAdmissionPersistsBeforeVisibility(t *testing.T) {
	a := serialAccounts(3)
	p, dir := NewTxPool(), t.TempDir()
	if err := p.EnableGroupMigrationQueueJournal(dir, a[:2]); err != nil {
		t.Fatal(err)
	}
	held, companion := serialTx(a[0], a[1], 1), serialTx(a[2], a[2], 2)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	p.migrationJournalPublish = func(path string, raw []byte) error {
		close(entered)
		<-release
		return PublishSourcePrepareFile(path, raw)
	}
	go func() { p.AddTxs2Pool_Head([]*Transaction{held, companion}); close(done) }()
	awaitIsolation(t, entered, "head did not reach persistence")
	if p.GetTxQueueLen() != 0 {
		close(release)
		<-done
		t.Fatal("undurable head batch became visible")
	}
	ordinary := serialTx(a[2], a[2], 3)
	p.AddTx2Pool(ordinary)
	if packed := p.PackTxs(10); len(packed) != 1 || packed[0] != ordinary {
		close(release)
		<-done
		t.Fatal("unrelated work blocked behind mixed head")
	}
	close(release)
	awaitIsolation(t, done, "head admission stuck")
	if packed := p.PackTxs(10); len(packed) != 1 || packed[0] != companion || p.GetTxQueueLen() != 1 {
		t.Fatal("head recovery hold or companion changed")
	}
	fresh := NewTxPool()
	if err := fresh.EnableGroupMigrationQueueJournal(dir, a[:2]); err != nil || fresh.GetTxQueueLen() != 1 {
		t.Fatal("head obligation missing after restart", err)
	}
}

func TestPreparedAdmissionRechecksExtendedMembership(t *testing.T) {
	a := serialAccounts(4)
	p, dir := NewTxPool(), t.TempDir()
	if err := p.EnableGroupMigrationQueueJournal(dir, a[:2]); err != nil {
		t.Fatal(err)
	}
	old, next := serialTx(a[0], a[1], 1), serialTx(a[2], a[3], 2)
	// The first caller prepared bytes before the second group joined. Missing
	// bytes are NOT an excuse to exclude the new group's durable obligation.
	encoded := map[*Transaction][]byte{old: old.Encode()}
	if err := p.ExtendGroupMigrationQueueJournal(a[2:]); err != nil {
		t.Fatal(err)
	}
	p.journalGate.Lock()
	err := p.persistMigrationQueueAdditionEncodedLocked([]*Transaction{old, next}, false, encoded, nil)
	if err == nil {
		p.lock.Lock()
		p.appendPendingEncodedLocked([]*Transaction{old, next}, false, encoded)
		p.lock.Unlock()
	}
	p.journalGate.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	fresh := NewTxPool()
	if err := fresh.RestoreGroupMigrationQueueJournal(dir, a); err != nil || fresh.GetTxQueueLen() != 2 || len(fresh.PackTxs(10)) != 0 {
		t.Fatal("membership extension escaped reclassification", err)
	}
}

func TestAdmissionIdentityCacheUsesOriginalsAndPrunesCompletedWork(t *testing.T) {
	p, journal, accounts, tx := indexedAdmissionFixture(t, true)
	p.AddTx2Pool(tx)
	if err := p.ValidateIndexedMigrationAdmission(); err != nil {
		t.Fatal(err)
	}
	if len(p.migrationAdmissionCache) != 1 {
		t.Fatal("pending original was not cached")
	}
	if _, ok := p.migrationAdmissionCache[string(tx.Encode())]; !ok {
		t.Fatal("cache not keyed by exact original bytes")
	}
	// A different pointer with identical immutable transport is a retry. A
	// conflicting transport must still fail without publishing or admission.
	p.AddTxs2Pool_Head([]*Transaction{DecodeTx(tx.Encode())})
	conflict := DecodeTx(tx.Encode())
	conflict.Value = big.NewInt(6)
	if indexedAdmissionPanic(func() { p.AddTx2Pool(conflict) }) == nil || p.GetTxQueueLen() != 1 || p.migrationJournal.sequence != 2 {
		t.Fatal("warm cache hid a conflicting transport")
	}
	dir := t.TempDir()
	h, err := BuildGroupMigrationHandoff(tx, "cache", accounts, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := PersistMigrationHandoff(dir, h); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(h.Execution)
	item := CompletedHandoffConfirmation{H: h, Ack: HandoffAck{ID: h.ID, ProposalID: "cache", TargetShard: 1, ExecutionHash: hex.EncodeToString(hash[:]), Block: 1, BlockHash: strings.Repeat("1", 64), StateRoot: strings.Repeat("2", 64), Executed: true}}
	timing, err := p.ConfirmCompletedHandoffBatchTimed(dir, []CompletedHandoffConfirmation{item})
	if err != nil || timing.Removed != 1 {
		t.Fatal("durable completed dequeue not reported", timing, err)
	}
	if err := p.ValidateIndexedMigrationAdmission(); err != nil || len(p.migrationAdmissionCache) != 0 {
		t.Fatal("completed originals retained in disposable cache", err)
	}
	fresh := NewTxPool()
	fresh.EnableIndexedMigrationAdmission()
	if err := fresh.RestoreGroupMigrationQueueJournal(journal, accounts); err != nil || fresh.GetTxQueueLen() != 0 {
		t.Fatal("cache changed recovery semantics", err)
	}
}

func TestAdmissionIdentityCachePreservesFullTransportRepresentation(t *testing.T) {
	p, _, _, tx := indexedAdmissionFixture(t, true)
	tx.Signature = []byte{}
	p.AddTx2Pool(tx)
	if err := p.ValidateIndexedMigrationAdmission(); err != nil {
		t.Fatal(err)
	}
	// Gob collapses []byte{} to nil. The live admission's original JSON
	// representation still distinguishes them, just as before caching.
	retry := DecodeTx(tx.Encode())
	retry.Signature = []byte{}
	if failure := indexedAdmissionPanic(func() { p.AddTx2Pool(retry) }); failure != nil {
		t.Fatal("identity cache normalized a valid live transport retry", failure)
	}
	conflict := DecodeTx(tx.Encode())
	if indexedAdmissionPanic(func() { p.AddTx2Pool(conflict) }) == nil || p.GetTxQueueLen() != 1 || p.migrationJournal.sequence != 2 {
		t.Fatal("identity cache hid an altered JSON transport representation")
	}
}

func TestConfirmationReportsNoRemovalBeforeDurablePublication(t *testing.T) {
	p, dir, _, items := completedFixture(t, 1)
	p.migrationJournalPublish = func(string, []byte) error { return errors.New("injected publication failure") }
	timing, err := p.ConfirmCompletedHandoffBatchTimed(dir, items)
	if err == nil || timing.Removed != 0 || p.GetTxQueueLen() != 2001 {
		t.Fatal("failed publication reported or performed a dequeue", timing, err)
	}
}

func TestGroupMigrationSnapshotSeparatesContactFromRecoveryAndOwnsBytes(t *testing.T) {
	a := serialAccounts(3)
	p := NewTxPool()
	if err := p.EnableGroupMigrationQueueJournal(t.TempDir(), a[:2]); err != nil {
		t.Fatal(err)
	}
	newRoute := serialTx(a[2], a[0], 1)
	newRoute.RoutingFrozen, newRoute.RoutedRecipientShard = true, 1
	ordinary := serialTx(a[2], a[2], 2)
	late := serialTx(a[2], a[0], 3)
	late.RoutingFrozen, late.Relayed = true, true
	p.AddTxs2Pool([]*Transaction{newRoute, ordinary, late})
	work := p.SnapshotGroupMigrationWork(a[:2])
	if work.ContactPending != 2 || work.RecoveryDue != 1 || len(work.Transactions) != 2 || work.Transactions[0].Nonce != 1 || work.Transactions[1].Nonce != 3 {
		t.Fatal("contact count conflated executable inbound debit with held credit", work)
	}
	work.Transactions[0].Value.SetInt64(900)
	again := p.SnapshotGroupMigrationWork(a[:2])
	if again.Transactions[0].Value.Int64() != 1 || newRoute.Value.Int64() != 1 {
		t.Fatal("snapshot mutated live/original transport")
	}
	_, detail, err := p.TryGroupDrainSnapshot(a[:2])
	if err != nil || detail.Status != "pending" || detail.ContactPending != 2 || detail.RecoveryDue != 1 {
		t.Fatal("drain did not expose the actual recovery population", detail, err)
	}
	p.journalGate.Lock()
	_, detail, err = p.TryGroupDrainSnapshot(a[:2])
	p.journalGate.Unlock()
	if err != nil || detail.Status != "journal_busy" || detail.ContactPending != -1 || detail.RecoveryDue != -1 {
		t.Fatal("busy drain claimed an observed empty queue", detail, err)
	}
}
