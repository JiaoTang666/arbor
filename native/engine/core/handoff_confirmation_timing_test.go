package core

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCompletedHandoffTimingStages(t *testing.T) {
	p, dir, _, items := completedFixture(t, 3)
	timing, err := p.ConfirmCompletedHandoffBatchTimed(dir, items)
	if err != nil {
		t.Fatal(err)
	}
	if timing.Items != len(items) || timing.RelatedLen != len(items) {
		t.Fatalf("wrong timed batch size: %+v", timing)
	}
	stages := map[string]time.Duration{
		"outbox": timing.OutboxRead, "validation": timing.Validation,
		"gate wait": timing.JournalGateWait, "pool lock wait": timing.PoolLockWait,
		"pool scan": timing.PoolScan, "receipt check": timing.ReceiptCheck,
		"joint confirm persist": timing.ConfirmPersist,
		"compact":               timing.Compact,
	}
	var measured time.Duration
	for name, elapsed := range stages {
		if elapsed <= 0 {
			t.Fatalf("missing %s timing: %+v", name, timing)
		}
		measured += elapsed
	}
	if timing.Total < measured || timing.ReceiptPersist != 0 || timing.RemovePersist != 0 || p.migrationJournal.sequence != 2 || len(p.pendingQueueLocked()) != 2000 {
		t.Fatalf("timing or durable dequeue mismatch: %+v", timing)
	}
}

func TestCompletedHandoffTimingOnFailure(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		p, dir, _, items := completedFixture(t, 3)
		items[1].Relay = nil
		timing, err := p.ConfirmCompletedHandoffBatchTimed(dir, items)
		if err == nil || timing.Total <= 0 || timing.OutboxRead <= 0 || timing.Validation <= 0 ||
			timing.JournalGateWait != 0 || timing.ReceiptPersist != 0 || timing.RemovePersist != 0 || timing.ConfirmPersist != 0 ||
			p.migrationJournal.sequence != 1 || len(p.pendingQueueLocked()) != 2003 {
			t.Fatalf("validation failure timing or state mismatch: timing=%+v err=%v", timing, err)
		}
	})
	t.Run("remove publication", func(t *testing.T) {
		p, dir, journal, items := completedFixture(t, 3)
		if err := os.WriteFile(filepath.Join(journal, "00000000000000000002.queue.json"), []byte("conflict"), 0600); err != nil {
			t.Fatal(err)
		}
		timing, err := p.ConfirmCompletedHandoffBatchTimed(dir, items)
		if err == nil || timing.Total <= 0 || timing.ReceiptPersist != 0 || timing.RemovePersist != 0 ||
			timing.Compact != 0 || p.migrationJournal.sequence != 1 || len(p.pendingQueueLocked()) != 2003 {
			t.Fatalf("remove failure timing or state mismatch: timing=%+v err=%v", timing, err)
		}
		receipts, err := loadCompletedBatchReceipts(dir)
		if err != nil || len(receipts) != 0 {
			t.Fatalf("failed joint commit published a separate receipt: receipts=%d err=%v", len(receipts), err)
		}
	})
}
