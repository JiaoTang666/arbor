package core

import (
	"fmt"
)

// Epoch migration owns the whole paused legacy pool. The serial migration
// journal has a different ownership and recovery contract and cannot coexist.
func (p *TxPool) SnapshotCLPAEpochPending() ([]*Transaction, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.lanes != nil || p.migrationJournal != nil || p.sourcePreparedAccount != "" {
		return nil, fmt.Errorf("CLPA epoch queue cannot share a serial migration pool")
	}
	for _, txs := range p.RelayPool {
		if len(txs) != 0 {
			return nil, fmt.Errorf("CLPA epoch snapshot has unsent relay work")
		}
	}
	out := make([]*Transaction, len(p.TxQueue))
	for i, tx := range p.TxQueue {
		if tx == nil {
			return nil, fmt.Errorf("nil pending epoch transaction")
		}
		out[i] = DecodeTx(tx.Encode())
	}
	return out, nil
}

// SnapshotCLPAEpochPendingReferences is for the paused async cut only. The
// protocol owns these immutable pending objects until install; callers must
// neither mutate them nor resume ordinary packing before that install.
// Retained payload is not serialized or cloned to create its identity manifest.
func (p *TxPool) SnapshotCLPAEpochPendingReferences() ([]*Transaction, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if err := p.validateCLPAEpochPoolLocked(); err != nil {
		return nil, err
	}
	return append([]*Transaction(nil), p.TxQueue...), nil
}

func (p *TxPool) validateCLPAEpochPoolLocked() error {
	if p.lanes != nil || p.migrationJournal != nil || p.sourcePreparedAccount != "" {
		return fmt.Errorf("CLPA epoch queue cannot share a serial migration pool")
	}
	for _, txs := range p.RelayPool {
		if len(txs) != 0 {
			return fmt.Errorf("CLPA epoch snapshot has unsent relay work")
		}
	}
	for _, tx := range p.TxQueue {
		if tx == nil {
			return fmt.Errorf("nil pending epoch transaction")
		}
	}
	return nil
}

// InstallCLPAEpochPendingDelta atomically checks the exact paused source view,
// retains its selected objects in FIFO order, and appends cloned incoming work.
// The caller validates the source manifest and each incoming identity before
// this call. A missing leader payload cannot be reconstructed from a hash.
func (p *TxPool) InstallCLPAEpochPendingDelta(expected, retained, incoming []*Transaction) error {
	clones := make([]*Transaction, len(incoming))
	for i, tx := range incoming {
		if tx == nil {
			return fmt.Errorf("nil installed epoch transaction")
		}
		clones[i] = DecodeTx(tx.Encode())
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	if err := p.validateCLPAEpochPoolLocked(); err != nil {
		return err
	}
	if len(expected) != len(p.TxQueue) {
		return fmt.Errorf("async epoch pending source changed after manifest validation")
	}
	for i, tx := range expected {
		if tx != p.TxQueue[i] {
			return fmt.Errorf("async epoch pending source changed after manifest validation")
		}
	}
	keep := 0
	for _, tx := range expected {
		if keep < len(retained) && retained[keep] == tx {
			keep++
		}
	}
	if keep != len(retained) {
		return fmt.Errorf("async epoch retained work is not a source FIFO subset")
	}
	p.TxQueue = append(append(make([]*Transaction, 0, len(retained)+len(clones)), retained...), clones...)
	return nil
}

// Only called from the committed epoch install, after its input and peer relay
// prefixes are closed. The complete install payload defines every replica's
// pending queue; enqueueing a received transfer before commit is forbidden.
func (p *TxPool) ReplaceCLPAEpochPending(txs []*Transaction) error {
	clones := make([]*Transaction, len(txs))
	for i, tx := range txs {
		if tx == nil {
			return fmt.Errorf("nil installed epoch transaction")
		}
		clones[i] = DecodeTx(tx.Encode())
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.lanes != nil || p.migrationJournal != nil || p.sourcePreparedAccount != "" {
		return fmt.Errorf("CLPA epoch install cannot share a serial migration pool")
	}
	p.TxQueue = clones
	return nil
}

// Followers retain an installed pending queue but do not pack the leader's
// block. Remove only matching admission legs when that block commits. The
// leader already removed the same entries at proposal time.
func (p *TxPool) RemoveCLPAEpochCommitted(txs []*Transaction) {
	keys := make(map[string]bool, len(txs))
	for _, tx := range txs {
		if tx != nil && tx.AdmissionID != "" {
			keys[fmt.Sprintf("%s/%t", tx.AdmissionID, tx.Relayed)] = true
		}
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	out := p.TxQueue[:0]
	for _, tx := range p.TxQueue {
		if !keys[fmt.Sprintf("%s/%t", tx.AdmissionID, tx.Relayed)] {
			out = append(out, tx)
		}
	}
	for i := len(out); i < len(p.TxQueue); i++ {
		p.TxQueue[i] = nil
	}
	p.TxQueue = out
}
