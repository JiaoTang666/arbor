package core

import (
	"bytes"
	"encoding/json"
	"math/big"
	"testing"
	"time"
)

func TestGroupDrainDoesNotWaitForOrdinaryDebit(t *testing.T) {
	a := "0000000000000000000000000000000000000010"
	b := "0000000000000000000000000000000000000020"
	sender := "0000000000000000000000000000000000000030"
	dir := t.TempDir()
	p := NewTxPool()
	tx := NewTransaction(sender, a, big.NewInt(1), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	tx.RoutedSenderShard = 6
	tx.RoutedRecipientShard = 2
	p.AddTx2Pool(tx)
	if err := p.EnableGroupMigrationQueueJournal(dir, []string{a, b}); err != nil {
		t.Fatal(err)
	}
	if raw, err := p.GroupDrainSnapshot([]string{a, b}); err != nil || raw == nil {
		t.Fatalf("ordinary debit must not block recovery drain: raw=%q err=%v", raw, err)
	}
	if got := p.PackTxs(100); len(got) != 1 || got[0] != tx {
		t.Fatalf("new route should use ordinary block: %d", len(got))
	}
	// In production this call occurs from the next proposal, after the packed
	// ordinary block has committed. This ordinary debit was never journaled.
	raw, err := p.GroupDrainSnapshot([]string{a, b})
	if err != nil || len(raw) == 0 {
		t.Fatalf("committed empty union was not published: %v", err)
	}
	var drained migrationQueueSnapshot
	if err = json.Unmarshal(raw, &drained); err != nil || drained.Sequence != 1 || len(drained.Transactions) != 0 {
		t.Fatalf("wrong empty generation: %+v %v", drained, err)
	}
	again, err := p.GroupDrainSnapshot([]string{a, b})
	if err != nil || !bytes.Equal(raw, again) || p.migrationJournal.sequence != 1 {
		t.Fatal("empty generation must be stable and idempotent", err)
	}
	recovered := NewTxPool()
	if err = recovered.EnableGroupMigrationQueueJournal(dir, []string{a, b}); err != nil || len(recovered.pendingQueueLocked()) != 0 {
		t.Fatalf("empty generation must recover empty: %d %v", len(recovered.pendingQueueLocked()), err)
	}
}
