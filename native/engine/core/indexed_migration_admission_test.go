package core

import (
	"bytes"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func indexedAdmissionFixture(t *testing.T, enabled bool) (*TxPool, string, []string, *Transaction) {
	t.Helper()
	accounts := []string{"0000000000000000000000000000000000000010", "0000000000000000000000000000000000000020"}
	p, directory := NewTxPool(), t.TempDir()
	if enabled {
		p.EnableIndexedMigrationAdmission()
	}
	if err := p.EnableGroupMigrationQueueJournal(directory, accounts); err != nil {
		t.Fatal(err)
	}
	tx := NewTransaction("0000000000000000000000000000000000000031", accounts[0], big.NewInt(5), 1, time.Unix(10, 0))
	tx.AdmissionID, tx.IngressSequence = "pending/1", 1
	tx.RoutingFrozen, tx.Relayed = true, true
	tx.RoutedSenderShard, tx.RoutedRecipientShard = 1, 0
	tx.SenderRequiresExistingState, tx.RecipientRequiresExistingState = true, true
	return p, directory, accounts, tx
}

func indexedAdmissionPanic(fn func()) (value interface{}) {
	defer func() { value = recover() }()
	fn()
	return nil
}

func TestIndexedMigrationAdmissionConcurrentPendingOnce(t *testing.T) {
	p, dir, accounts, tx := indexedAdmissionFixture(t, true)
	start, failures := make(chan struct{}), make(chan interface{}, 2)
	var workers sync.WaitGroup
	for i := 0; i < 2; i++ {
		copy := DecodeTx(tx.Encode())
		workers.Add(1)
		go func(copy *Transaction) {
			defer workers.Done()
			<-start
			if failure := indexedAdmissionPanic(func() { p.AddTxs2Pool([]*Transaction{copy}) }); failure != nil {
				failures <- failure
			}
		}(copy)
	}
	close(start)
	workers.Wait()
	close(failures)
	for failure := range failures {
		t.Fatal("identical concurrent pending admission rejected", failure)
	}
	if p.GetTxQueueLen() != 1 || p.migrationJournal.sequence != 2 {
		t.Fatal("duplicate produced additional pending work or journal generation")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "00000000000000000002.queue.json"))
	if err != nil {
		t.Fatal(err)
	}
	var generation migrationQueueSnapshot
	if err = json.Unmarshal(raw, &generation); err != nil || generation.Kind != "append" || len(generation.Transactions) != 1 {
		t.Fatal("expected exactly one durable append", generation, err)
	}
	// Recovery reconstructs pending identity from the existing journal. Both
	// tail and head retries leave it untouched, without a persistent side index.
	restored := NewTxPool()
	restored.EnableIndexedMigrationAdmission()
	if err = restored.RestoreGroupMigrationQueueJournal(dir, accounts); err != nil {
		t.Fatal(err)
	}
	if err = restored.ValidateIndexedMigrationAdmission(); err != nil {
		t.Fatal(err)
	}
	restored.AddTxs2Pool([]*Transaction{DecodeTx(tx.Encode())})
	restored.AddTxs2Pool_Head([]*Transaction{DecodeTx(tx.Encode())})
	if restored.GetTxQueueLen() != 1 || restored.migrationJournal.sequence != 2 {
		t.Fatal("restart retry created another durable obligation")
	}
}

func TestIndexedMigrationAdmissionConflictIsAtomic(t *testing.T) {
	changes := map[string]func(*Transaction){
		"value": func(tx *Transaction) { tx.Value = big.NewInt(6) },
		"route": func(tx *Transaction) { tx.RoutedRecipientShard = 1 },
		"hash":  func(tx *Transaction) { tx.TxHash[0] ^= 1 },
		"time":  func(tx *Transaction) { tx.Time = tx.Time.Add(time.Second) },
	}
	for name, mutate := range changes {
		t.Run(name, func(t *testing.T) {
			p, _, _, tx := indexedAdmissionFixture(t, true)
			p.AddTx2Pool(tx)
			conflict := DecodeTx(tx.Encode())
			mutate(conflict)
			ordinary := NewTransaction("0000000000000000000000000000000000000031", "0000000000000000000000000000000000000031", big.NewInt(1), 2, time.Unix(11, 0))
			if indexedAdmissionPanic(func() { p.AddTxs2Pool([]*Transaction{ordinary, conflict}) }) == nil {
				t.Fatal("same admission/effect with altered transport was accepted")
			}
			if p.GetTxQueueLen() != 1 || p.migrationJournal.sequence != 2 {
				t.Fatal("rejected mixed batch partly changed queue or journal")
			}
		})
	}
}

func TestIndexedMigrationAdmissionPreservesRecoveredDuplicateEvidence(t *testing.T) {
	legacy, dir, accounts, tx := indexedAdmissionFixture(t, false)
	legacy.AddTxs2Pool([]*Transaction{tx, DecodeTx(tx.Encode())})
	path := filepath.Join(dir, "00000000000000000002.queue.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	restored := NewTxPool()
	restored.EnableIndexedMigrationAdmission()
	if err = restored.RestoreGroupMigrationQueueJournal(dir, accounts); err != nil {
		t.Fatal(err)
	}
	if restored.ValidateIndexedMigrationAdmission() == nil {
		t.Fatal("corrupt recovered duplicate was silently coalesced")
	}
	if indexedAdmissionPanic(func() { restored.AddTx2Pool(DecodeTx(tx.Encode())) }) == nil {
		t.Fatal("admission continued after invalid recovered pending state")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) || restored.GetTxQueueLen() != 2 || restored.migrationJournal.sequence != 2 {
		t.Fatal("legacy evidence was overwritten or queue repaired silently", err)
	}
}

func TestIndexedMigrationAdmissionOnlyFiltersRecoveryWork(t *testing.T) {
	p, _, _, tx := indexedAdmissionFixture(t, true)
	p.AddTx2Pool(tx)
	ordinary := NewTransaction("0000000000000000000000000000000000000031", "0000000000000000000000000000000000000031", big.NewInt(1), 2, time.Unix(11, 0))
	// Ordinary work has no index identity here; it remains outside this helper's
	// scope and its two entries keep their original admission order.
	p.AddTxs2Pool([]*Transaction{DecodeTx(tx.Encode()), ordinary, DecodeTx(ordinary.Encode())})
	if p.GetTxQueueLen() != 3 || p.migrationJournal.sequence != 2 || len(p.PackTxs(8)) != 2 {
		t.Fatal("ordinary work was scanned, deduplicated, or journaled")
	}
}
