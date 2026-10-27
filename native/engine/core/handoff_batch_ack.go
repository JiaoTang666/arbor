package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

type HandoffConfirmation struct {
	H   *MigrationHandoff
	Ack HandoffAck
}

// Preserve the final relay receipt and the original pre-credit inbound record.
// Every receipt and permitted transformation is verified before dequeue.
type CompletedHandoffConfirmation struct {
	H               *MigrationHandoff
	Ack             HandoffAck
	Relay           *HandoffRelayAck
	InboundOriginal *Transaction
}

// HandoffConfirmationTiming records wall time in non-overlapping stages.
// RelatedLen is sampled while scanning the migration lane; it is zero when
// there is no isolated lane. Durations remain populated on partial failure.
type HandoffConfirmationTiming struct {
	OutboxRead      time.Duration
	Validation      time.Duration
	JournalGateWait time.Duration
	PoolLockWait    time.Duration
	PoolScan        time.Duration
	ReceiptCheck    time.Duration
	ReceiptPersist  time.Duration
	RemovePersist   time.Duration
	ConfirmPersist  time.Duration // v2: completion proof and dequeue share one publication
	Compact         time.Duration
	Total           time.Duration
	Items           int
	RelatedLen      int
	Removed         int // memory dequeue completed after durable publication; zero on failed publication
	observe         func(string)
}

func (t *HandoffConfirmationTiming) enter(stage string) {
	if t != nil && t.observe != nil {
		t.observe(stage)
	}
}

// Completed journal-backed batches publish one joint proof/dequeue generation.
// Legacy non-journal pools retain their original receipt-first behavior.
func (p *TxPool) ConfirmMigrationHandoffBatch(dir string, items []HandoffConfirmation) error {
	completed := make([]CompletedHandoffConfirmation, 0, len(items))
	for _, item := range items {
		if item.Ack.RelayPending {
			return fmt.Errorf("batch requires completed direct handoff")
		}
		completed = append(completed, CompletedHandoffConfirmation{H: item.H, Ack: item.Ack})
	}
	return p.ConfirmCompletedHandoffBatch(dir, completed)
}

// Scan the related queue once and publish one proof/dequeue generation before
// changing memory. An unsuccessful publication retains the previous queue.
func (p *TxPool) ConfirmCompletedHandoffBatch(dir string, items []CompletedHandoffConfirmation) error {
	_, err := p.ConfirmCompletedHandoffBatchTimed(dir, items)
	return err
}

// ConfirmCompletedHandoffBatchTimed performs the same durable confirmation and
// returns timings without emitting logs. Total includes all stages and waits.
func (p *TxPool) ConfirmCompletedHandoffBatchTimed(dir string, items []CompletedHandoffConfirmation) (timing HandoffConfirmationTiming, err error) {
	return p.ConfirmCompletedHandoffBatchObserved(dir, items, nil)
}

// observe records entry, not success: blocked stages stay visible. It must be
// nonblocking and must not call back into the pool. No extra polling goroutine.
func (p *TxPool) ConfirmCompletedHandoffBatchObserved(dir string, items []CompletedHandoffConfirmation, observe func(string)) (timing HandoffConfirmationTiming, err error) {
	started := time.Now()
	timing.observe = observe
	timing.Items = len(items)
	defer func() { timing.Total = time.Since(started); timing.observe = nil }()
	if len(items) == 0 {
		return timing, nil
	}
	originals := map[string]bool{}
	timing.enter("outbox_read")
	stage := time.Now()
	outbox, err := LoadMigrationArtifacts(dir, ".handoff.json", false)
	timing.OutboxRead = time.Since(stage)
	if err != nil {
		return timing, err
	}
	stage = time.Now()
	timing.enter("validation")
	err = func() error {
		for _, item := range items {
			h := item.H
			if h == nil {
				return fmt.Errorf("batch requires handoff")
			}
			if e := ValidateHandoffAck(h, item.Ack); e != nil {
				return e
			}
			if item.Ack.RelayPending {
				if item.Relay == nil {
					return fmt.Errorf("batch handoff missing final relay ACK")
				}
				if e := ValidateHandoffRelayAck(h, *item.Relay); e != nil {
					return e
				}
			} else if item.Relay != nil {
				return fmt.Errorf("unexpected final relay ACK")
			}
			// Queue entries are locally encoded after restart. Gob type IDs are
			// process-specific; normalize ONLY the local matching key. Original
			// wire bytes, handoff IDs and ACK hashes remain unchanged.
			original, e := completedOriginal(item)
			if e != nil {
				return e
			}
			raw, e := outbox.Read(h.ID)
			if e != nil {
				return e
			}
			var stored MigrationHandoff
			if e = json.Unmarshal(raw, &stored); e != nil {
				return e
			}
			a, _ := json.Marshal(h)
			b, _ := json.Marshal(stored)
			if !bytes.Equal(a, b) {
				return fmt.Errorf("batch outbox mismatch")
			}
			if originals[string(original)] {
				return fmt.Errorf("ambiguous batch originals")
			}
			originals[string(original)] = true
		}
		return nil
	}()
	timing.Validation = time.Since(stage)
	if err != nil {
		return timing, err
	}
	stage = time.Now()
	timing.enter("journal_gate_wait")
	p.journalGate.Lock()
	timing.JournalGateWait = time.Since(stage)
	defer p.journalGate.Unlock()
	if p.lanes != nil {
		if p.migrationJournal == nil {
			return timing, fmt.Errorf("migration lane lacks durable journal")
		}
		err = p.confirmIsolatedTimedLocked(originals, func(removed [][]byte) error {
			timing.enter("receipt_check")
			stage = time.Now()
			existing, err := p.checkedCompletedReceiptStateLocked(dir, items)
			if err != nil {
				timing.ReceiptCheck += time.Since(stage)
				return err
			}
			timing.ReceiptCheck += time.Since(stage)
			stage = time.Now()
			timing.enter("confirm_persist")
			err = p.persistCompletedGenerationLocked(items, removed, existing)
			timing.ConfirmPersist += time.Since(stage)
			return err
		}, &timing)
		return timing, err
	}
	stage = time.Now()
	timing.enter("legacy_pool_lock_wait")
	p.lock.Lock()
	timing.PoolLockWait += time.Since(stage)
	defer p.lock.Unlock()
	stage = time.Now()
	timing.enter("receipt_check")
	if err := checkCompletedReceiptConflicts(dir, items); err != nil {
		timing.ReceiptCheck += time.Since(stage)
		return timing, err
	}
	timing.ReceiptCheck += time.Since(stage)
	stage = time.Now()
	remaining := make([]*Transaction, 0, len(p.TxQueue))
	counts := map[string]int{}
	for _, tx := range p.TxQueue {
		key := string(tx.Encode())
		if originals[key] {
			counts[key]++
			if counts[key] > 1 {
				timing.PoolScan += time.Since(stage)
				return timing, fmt.Errorf("ambiguous duplicate source occurrence")
			}
		} else {
			remaining = append(remaining, tx)
		}
	}
	timing.PoolScan += time.Since(stage)
	stage = time.Now()
	timing.enter("receipt_persist")
	if err := persistCompletedBatchReceipt(dir, items); err != nil {
		timing.ReceiptPersist += time.Since(stage)
		return timing, err
	}
	timing.ReceiptPersist += time.Since(stage)
	if len(remaining) == len(p.TxQueue) {
		return timing, nil
	}
	stage = time.Now()
	timing.enter("remove_persist")
	if e := p.persistMigrationQueueLocked(remaining); e != nil {
		timing.RemovePersist += time.Since(stage)
		return timing, e
	}
	timing.RemovePersist += time.Since(stage)
	stage = time.Now()
	timing.Removed = len(p.TxQueue) - len(remaining)
	p.TxQueue = remaining
	timing.Compact += time.Since(stage)
	return timing, nil
}
