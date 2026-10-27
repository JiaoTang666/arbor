package core

import (
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sourceCreditFixture(t *testing.T) (*TxPool, string, []string, *Transaction, SourceCreditConfirmation) {
	t.Helper()
	accounts := []string{strings.Repeat("1", 40), strings.Repeat("2", 40)}
	tx := NewTransaction(strings.Repeat("3", 40), accounts[0], big.NewInt(7), 9, time.Unix(1, 0))
	tx.AdmissionID, tx.IngressSequence = "run/9", 9
	tx.Relayed, tx.RoutingFrozen = true, true
	tx.RoutedSenderShard, tx.RoutedRecipientShard = 1, 0
	p := NewTxPool()
	p.AddTx2Pool(tx)
	p.AddTx2Pool(DecodeTx(tx.Encode()))
	dir := t.TempDir()
	if err := p.EnableGroupMigrationQueueJournal(dir, accounts); err != nil {
		t.Fatal(err)
	}
	effect, err := BuildTransactionEffect(tx, TransactionCredit)
	if err != nil {
		t.Fatal(err)
	}
	item := SourceCreditConfirmation{Original: tx.Encode(), Effect: effect, ExecutedBlock: 2, ObservedStateRoot: strings.Repeat("4", 64)}
	return p, dir, accounts, tx, item
}

func TestSourceCreditConfirmationDropsDuplicatesNotOtherAdmissions(t *testing.T) {
	p, dir, accounts, tx, item := sourceCreditFixture(t)
	other := DecodeTx(tx.Encode())
	other.AdmissionID, other.IngressSequence = "run/10", 10
	p.AddTx2Pool(other)
	ordinary := NewTransaction(strings.Repeat("3", 40), strings.Repeat("3", 40), big.NewInt(1), 1, time.Unix(1, 0))
	p.AddTx2Pool(ordinary)
	if removed, err := p.ConfirmSourceCreditBatch([]SourceCreditConfirmation{item, item}); err != nil || removed != 2 {
		t.Fatal("exact duplicates not completed", removed, err)
	}
	if got := p.PackTxs(10); len(got) != 1 || got[0] != ordinary {
		t.Fatal("unrelated work blocked or another admission dropped")
	}
	if raw, err := p.GroupDrainSnapshot(accounts); err != nil || raw != nil {
		t.Fatal("unknown credit was declared drained", err)
	}
	item.Original = other.Encode()
	item.Effect, _ = BuildTransactionEffect(other, TransactionCredit)
	if removed, err := p.ConfirmSourceCreditBatch([]SourceCreditConfirmation{item}); err != nil || removed != 1 {
		t.Fatal(removed, err)
	}
	// Exercise actual drain normalization after the new delta, then recovery.
	raw, err := p.GroupDrainSnapshot(accounts)
	if err != nil || raw == nil {
		t.Fatal("source-credit delta blocked full drain", err)
	}
	var snapshot migrationQueueSnapshot
	if err = json.Unmarshal(raw, &snapshot); err != nil || len(snapshot.Transactions) != 0 || snapshot.Kind != "" && snapshot.Kind != "full" {
		t.Fatal("drain did not certify an empty full snapshot", err)
	}
	recovered := NewTxPool()
	if err = recovered.EnableGroupMigrationQueueJournal(dir, accounts); err != nil || recovered.PendingCountLocked() != 0 {
		t.Fatal("completed duplicate credits resurrected", err)
	}
}

func TestSourceCreditRemovalFailureRetainsQueueAndRecovers(t *testing.T) {
	p, dir, accounts, _, item := sourceCreditFixture(t)
	p.migrationJournalPublish = func(string, []byte) error { return errors.New("forced removal publication failure") }
	if removed, err := p.ConfirmSourceCreditBatch([]SourceCreditConfirmation{item}); err == nil || removed != 0 || len(p.lanes.related) != 2 || p.migrationJournal.sequence != 1 {
		t.Fatal("failed publication changed queue", removed, err)
	}
	recovered := NewTxPool()
	if err := recovered.EnableGroupMigrationQueueJournal(dir, accounts); err != nil || len(recovered.lanes.related) != 2 {
		t.Fatal("pre-removal generation not recoverable", err)
	}
	p.migrationJournalPublish = nil
	if removed, err := p.ConfirmSourceCreditBatch([]SourceCreditConfirmation{item}); err != nil || removed != 2 {
		t.Fatal("durable source receipt could not resume removal", removed, err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "00000000000000000002.queue.json"))
	if err != nil {
		t.Fatal(err)
	}
	var snapshot migrationQueueSnapshot
	if err = json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err = replayMigrationGeneration(nil, snapshot); err == nil {
		t.Fatal("source removal accepted without journal receipt directory")
	}
	receipt, _ := sourceCreditReceiptData(snapshot.SourceCredits)
	if err = os.Remove(sourceCreditReceiptPath(dir, receipt)); err != nil {
		t.Fatal(err)
	}
	if err = NewTxPool().EnableGroupMigrationQueueJournal(dir, accounts); err == nil {
		t.Fatal("missing source proof allowed destructive recovery")
	}
}

func TestSourceCreditConfirmationRejectsDebitAndNoJournal(t *testing.T) {
	p, _, _, _, item := sourceCreditFixture(t)
	item.Effect.Effect = TransactionDebit
	if _, err := p.ConfirmSourceCreditBatch([]SourceCreditConfirmation{item}); err == nil || len(p.lanes.related) != 2 {
		t.Fatal("debit seen used as credit completion", err)
	}
	if _, err := NewTxPool().ConfirmSourceCreditBatch(nil); err == nil {
		t.Fatal("source completion silently bypassed durable journal")
	}
}

func TestOrdinarySourceReceiptRequiresMatchingDebitAndLocalRoute(t *testing.T) {
	_, _, _, tx, item := sourceCreditFixture(t)
	tx.Relayed, tx.RoutedSenderShard = false, tx.RoutedRecipientShard
	item.Original = tx.Encode()
	if _, err := validateSourceCredit(item); err == nil {
		t.Fatal("ordinary completion accepted only a credit")
	}
	debit, _ := BuildTransactionEffect(tx, TransactionDebit)
	item.DebitEffect, item.DebitExecutedBlock = &debit, 2
	if _, err := validateSourceCredit(item); err != nil {
		t.Fatal(err)
	}
	wrong := debit
	wrong.AdmissionID = "different-admission"
	item.DebitEffect = &wrong
	if _, err := validateSourceCredit(item); err == nil {
		t.Fatal("mismatching debit proof accepted")
	}
	item.DebitEffect = &debit
	tx.RoutedSenderShard = 1
	item.Original = tx.Encode()
	if _, err := validateSourceCredit(item); err == nil {
		t.Fatal("source-local completion accepted cross-shard transaction")
	}
}
