package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func serialAccounts(n int) []string {
	var result []string
	for i := 0; i < n; i++ {
		result = append(result, fmt.Sprintf("%040x", i+1))
	}
	return result
}

func serialTx(a, b string, nonce uint64) *Transaction {
	return NewTransaction(a, b, big.NewInt(1), nonce, time.Unix(1, 0))
}

func TestSerialJournalExtensionPreservesOldWorkAndOrder(t *testing.T) {
	a := serialAccounts(5)
	dir := t.TempDir()
	p := NewTxPool()
	old := serialTx(a[0], a[1], 1)
	next := serialTx(a[2], a[3], 2)
	ordinary := serialTx(a[4], a[4], 3)
	p.AddTxs2Pool([]*Transaction{old, next, ordinary})
	if err := p.EnableGroupMigrationQueueJournal(dir, a[:2]); err != nil {
		t.Fatal(err)
	}
	if err := p.ExtendGroupMigrationQueueJournal(a[2:4]); err != nil {
		t.Fatal(err)
	}
	if got := p.pendingQueueLocked(); !reflect.DeepEqual(got, []*Transaction{old, next, ordinary}) {
		t.Fatal("extension reordered pending work")
	}
	if packed := p.PackTxs(99); len(packed) != 1 || packed[0] != ordinary {
		t.Fatal("holds lost or unrelated work blocked")
	}
	recovered := NewTxPool()
	if err := recovered.RestoreGroupMigrationQueueJournal(dir, a[:4]); err != nil {
		t.Fatal(err)
	}
	if len(recovered.pendingQueueLocked()) != 2 || len(recovered.PackTxs(99)) != 0 {
		t.Fatal("both groups must survive recovery with holds")
	}
	if err := NewTxPool().RestoreGroupMigrationQueueJournal(dir, a[:2]); err == nil {
		t.Fatal("uncommitted extension accepted")
	}
	if err := p.ExtendGroupMigrationQueueJournal(a[1:3]); err == nil {
		t.Fatal("overlap accepted")
	}
}

func TestSerialGroupDrainDoesNotEraseOtherGroup(t *testing.T) {
	a := serialAccounts(4)
	dir := t.TempDir()
	p := NewTxPool()
	p.AddTx2Pool(serialTx(a[0], a[1], 1))
	if err := p.EnableGroupMigrationQueueJournal(dir, a[:2]); err != nil {
		t.Fatal(err)
	}
	if err := p.ExtendGroupMigrationQueueJournal(a[2:]); err != nil {
		t.Fatal(err)
	}
	raw, err := p.GroupDrainSnapshot(a[2:])
	if err != nil || len(raw) == 0 {
		t.Fatalf("empty new group did not drain: %s %v", raw, err)
	}
	var proof struct {
		Accounts, JournalAccounts []string
		Transactions              [][]byte
		JournalSHA256             string
	}
	if err = json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	if len(proof.Transactions) != 0 || !reflect.DeepEqual(proof.Accounts, a[2:]) || !reflect.DeepEqual(proof.JournalAccounts, a) || len(proof.JournalSHA256) != 64 {
		t.Fatal("projection missing union binding")
	}
	recovered := NewTxPool()
	if err = recovered.RestoreGroupMigrationQueueJournal(dir, a); err != nil {
		t.Fatal(err)
	}
	if len(recovered.pendingQueueLocked()) != 1 {
		t.Fatal("retiring second group erased first group's durable work")
	}
	if pending, err := recovered.GroupDrainSnapshot(a[:2]); err != nil || pending != nil {
		t.Fatal("old group incorrectly drained", err)
	}
	// A late old-route relay must still acquire durability after another group retired.
	relay := serialTx(a[2], a[1], 2)
	relay.Relayed = true
	p.AddTx2Pool(relay)
	again := NewTxPool()
	if err = again.RestoreGroupMigrationQueueJournal(dir, a); err != nil {
		t.Fatal(err)
	}
	if len(again.pendingQueueLocked()) != 2 {
		t.Fatal("late old-group relay lost")
	}
}

func TestSerialExtensionFailureLeavesMemoryUnchanged(t *testing.T) {
	a := serialAccounts(4)
	p := NewTxPool()
	dir := t.TempDir()
	if err := p.EnableGroupMigrationQueueJournal(dir, a[:2]); err != nil {
		t.Fatal(err)
	}
	p.AddTx2Pool(serialTx(a[2], a[3], 1))
	p.migrationJournalPublish = func(string, []byte) error { return errors.New("injected IO failure") }
	if err := p.ExtendGroupMigrationQueueJournal(a[2:]); err == nil {
		t.Fatal("write failure ignored")
	}
	if !reflect.DeepEqual(p.migrationJournal.accounts, a[:2]) || p.migrationJournal.sequence != 1 || p.heldMigrationAccounts[a[2]] {
		t.Fatal("uncommitted memory extension")
	}
	if len(p.PackTxs(10)) != 1 {
		t.Fatal("failed extension held unrelated work")
	}
}

func TestSerialJournalRejectsUndeclaredMembershipChange(t *testing.T) {
	a := serialAccounts(4)
	p := NewTxPool()
	dir := t.TempDir()
	if err := p.EnableGroupMigrationQueueJournal(dir, a[:2]); err != nil {
		t.Fatal(err)
	}
	if err := p.ExtendGroupMigrationQueueJournal(a[2:]); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fmt.Sprintf("%020d.queue.json", 2))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var s migrationQueueSnapshot
	if err = json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	s.Kind = "full"
	s.PreviousAccounts = nil
	raw, _ = json.Marshal(s)
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err = NewTxPool().RestoreGroupMigrationQueueJournal(dir, a); err == nil {
		t.Fatal("membership change disguised as full snapshot")
	}
}

func TestGenerationJournalBeyondThreeAndLateWorkRecovery(t *testing.T) {
	a := serialAccounts(256)
	p := NewTxPool()
	dir := t.TempDir()
	if err := p.EnableGroupMigrationQueueJournal(dir, a[:32]); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 8; i++ {
		if err := p.ExtendGroupMigrationQueueJournal(a[i*32 : (i+1)*32]); err != nil {
			t.Fatal(err)
		}
	}
	late := serialTx(a[0], a[1], 199)
	p.AddTx2Pool(late)
	restored := NewTxPool()
	if err := restored.RestoreGroupMigrationQueueJournal(dir, a); err != nil {
		t.Fatal(err)
	}
	if restored.GetTxQueueLen() != 1 || len(restored.PackTxs(99)) != 0 {
		t.Fatal("first-generation fence or late work lost")
	}
	if err := NewTxPool().RestoreGroupMigrationQueueJournal(dir, a[:96]); err == nil {
		t.Fatal("restored against stale committed union")
	}
	if err := p.ExtendGroupMigrationQueueJournal(a[:2]); err == nil {
		t.Fatal("overlap silently accepted")
	}
	if err := NewTxPool().EnableGroupMigrationQueueJournal(t.TempDir(), a[:33]); err == nil {
		t.Fatal("33-account first group accepted")
	}
}

func TestSerialExtensionConcurrentAdmissions(t *testing.T) {
	a := serialAccounts(5)
	p := NewTxPool()
	dir := t.TempDir()
	if err := p.EnableGroupMigrationQueueJournal(dir, a[:2]); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); p.AddTx2Pool(serialTx(a[2], a[4], uint64(i))) }(i)
	}
	if err := p.ExtendGroupMigrationQueueJournal(a[2:4]); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	recovered := NewTxPool()
	if err := recovered.RestoreGroupMigrationQueueJournal(dir, a[:4]); err != nil {
		t.Fatal(err)
	}
	if len(recovered.pendingQueueLocked()) != 50 {
		t.Fatal("extension/admission race lost durable occurrences", len(recovered.pendingQueueLocked()))
	}
}
