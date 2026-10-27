package core

import (
	"fmt"
	"time"
)

// Journal-enabled pools have two physical lanes. Order is successful admission
// order, shared by both lanes; unrelated work never acquires journalGate.
// The related lane contains contacts, only some of which own recovery duties.
// Transactions must
// not be mutated by callers after admission (the existing TxPool contract).
type queuedMigrationWork struct {
	tx    *Transaction
	order int64
	key   string
}

type migrationLanes struct {
	ordinary, related []queuedMigrationWork
	first, last       int64
}

// Legacy CLPA rewrites the public queue in place, without durable handoff ACKs.
// It cannot own the same pool concurrently with the journal protocol.
func (p *TxPool) RequireLegacyQueueLocked() {
	if p.lanes != nil {
		panic("legacy queue transfer cannot bypass durable migration lanes")
	}
}

func (p *TxPool) appendPendingLocked(txs []*Transaction, head bool) {
	p.appendPendingEncodedLocked(txs, head, nil)
}

// encoded contains originals prepared before journalGate acquisition. Recheck
// current membership here; entries newly covered by an extension are encoded
// locally. An original is reused only for the very same immutable admission.
func (p *TxPool) appendPendingEncodedLocked(txs []*Transaction, head bool, encoded map[*Transaction][]byte) {
	if p.lanes == nil {
		if head {
			p.TxQueue = append(append([]*Transaction(nil), txs...), p.TxQueue...)
		} else {
			p.TxQueue = append(p.TxQueue, txs...)
		}
		return
	}
	l := p.lanes
	start := l.last
	if head {
		l.first -= int64(len(txs))
		start = l.first
	} else {
		l.last += int64(len(txs))
	}
	var ordinary, related []queuedMigrationWork
	for i, tx := range txs {
		e := queuedMigrationWork{tx: tx, order: start + int64(i)}
		if journalTouches(tx, p.migrationJournal.account, p.migrationJournal.accounts) {
			raw, prepared := encoded[tx]
			if !prepared {
				raw = tx.Encode()
			}
			e.key = string(raw)
			related = append(related, e)
		} else {
			ordinary = append(ordinary, e)
		}
	}
	if head {
		l.ordinary = append(ordinary, l.ordinary...)
		l.related = append(related, l.related...)
	} else {
		l.ordinary = append(l.ordinary, ordinary...)
		l.related = append(l.related, related...)
	}
}

// PendingCountLocked includes both lanes; caller already holds GetLocked.
func (p *TxPool) PendingCountLocked() int {
	if p.lanes == nil {
		return len(p.TxQueue)
	}
	return len(p.lanes.ordinary) + len(p.lanes.related)
}

// Audit/compatibility snapshot, never used by ordinary admission or ACK work.
func (p *TxPool) pendingQueueLocked() []*Transaction {
	if p.lanes == nil {
		return p.TxQueue
	}
	l := p.lanes
	out := make([]*Transaction, 0, p.PendingCountLocked())
	i, j := 0, 0
	for i < len(l.ordinary) || j < len(l.related) {
		if j == len(l.related) || (i < len(l.ordinary) && l.ordinary[i].order < l.related[j].order) {
			out = append(out, l.ordinary[i].tx)
			i++
		} else {
			out = append(out, l.related[j].tx)
			j++
		}
	}
	return out
}

func (p *TxPool) migrationPendingLocked() []*Transaction {
	if p.lanes == nil {
		return p.TxQueue
	}
	out := make([]*Transaction, 0, len(p.lanes.related))
	for _, e := range p.lanes.related {
		out = append(out, e.tx)
	}
	return out
}

// Merge only eligible work in admission order. Held related entries are never
// copied into the ordinary lane, including late old-route relay legs.
func (p *TxPool) packLanesLocked(limit uint64) []*Transaction {
	l := p.lanes
	selected := make([]*Transaction, 0)
	i, j := 0, 0
	retained := l.related[:0]
	ordinary := l.ordinary[:0]
	for uint64(len(selected)) < limit && (i < len(l.ordinary) || j < len(l.related)) {
		if j == len(l.related) || (i < len(l.ordinary) && l.ordinary[i].order < l.related[j].order) {
			if p.heldForMigration(l.ordinary[i].tx) {
				ordinary = append(ordinary, l.ordinary[i])
			} else {
				selected = append(selected, l.ordinary[i].tx)
			}
			i++
		} else {
			if p.heldForMigration(l.related[j].tx) {
				retained = append(retained, l.related[j])
			} else {
				selected = append(selected, l.related[j].tx)
			}
			j++
		}
	}
	retained = append(retained, l.related[j:]...)
	for k := len(retained); k < len(l.related); k++ {
		l.related[k] = queuedMigrationWork{}
	}
	l.related = retained
	if len(ordinary) == 0 {
		for k := 0; k < i; k++ {
			l.ordinary[k] = queuedMigrationWork{}
		}
		l.ordinary = l.ordinary[i:]
	} else {
		ordinary = append(ordinary, l.ordinary[i:]...)
		for k := len(ordinary); k < len(l.ordinary); k++ {
			l.ordinary[k] = queuedMigrationWork{}
		}
		l.ordinary = ordinary
	}
	return selected
}

// Called under journalGate, but never holds the ordinary pool lock over disk
// I/O. Related admission is serialized; packing can only remove entries.
func (p *TxPool) confirmIsolatedLocked(originals map[string]bool, persistReceipt func() error) error {
	return p.confirmIsolatedTimedLocked(originals, func(removed [][]byte) error {
		if err := persistReceipt(); err != nil {
			return err
		}
		if len(removed) == 0 {
			return nil
		}
		return p.persistMigrationRemovalLocked(removed)
	}, nil)
}

func (p *TxPool) confirmIsolatedTimedLocked(originals map[string]bool, persist func([][]byte) error, timing *HandoffConfirmationTiming) error {
	timing.enter("pool_lock_wait")
	stage := time.Now()
	p.lock.Lock()
	if timing != nil {
		timing.PoolLockWait += time.Since(stage)
		timing.RelatedLen = len(p.lanes.related)
	}
	stage = time.Now()
	counts := map[string]int{}
	var removed [][]byte
	for _, e := range p.lanes.related {
		if originals[e.key] {
			counts[e.key]++
			if counts[e.key] > 1 {
				if timing != nil {
					timing.PoolScan += time.Since(stage)
				}
				p.lock.Unlock()
				return fmt.Errorf("ambiguous duplicate source occurrence")
			}
			// An ACK must not race native execution of the same stage.
			if !p.heldForMigration(e.tx) {
				if timing != nil {
					timing.PoolScan += time.Since(stage)
				}
				p.lock.Unlock()
				return fmt.Errorf("handoff ACK matches executable work")
			}
			removed = append(removed, []byte(e.key))
		}
	}
	if timing != nil {
		timing.PoolScan += time.Since(stage)
	}
	p.lock.Unlock()
	if err := persist(removed); err != nil {
		return err
	}
	if len(removed) == 0 {
		return nil
	}
	stage = time.Now()
	timing.enter("compact_pool_lock_wait")
	p.lock.Lock()
	if timing != nil {
		timing.PoolLockWait += time.Since(stage)
	}
	defer p.lock.Unlock()
	stage = time.Now()
	kept := p.lanes.related[:0]
	for _, e := range p.lanes.related {
		if !originals[e.key] {
			kept = append(kept, e)
		}
	}
	for k := len(kept); k < len(p.lanes.related); k++ {
		p.lanes.related[k] = queuedMigrationWork{}
	}
	p.lanes.related = kept
	if timing != nil {
		timing.Compact += time.Since(stage)
		timing.Removed = len(removed)
	}
	return nil
}
