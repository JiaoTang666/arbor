package core

import (
	"fmt"
	"time"
)

// AddAuthorizedTxs is the non-panicking network boundary for the restricted
// authorized FIFO path. Internal legacy APIs keep their existing contracts.
func (p *TxPool) AddAuthorizedTxs(txs []*Transaction) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.transactionValidation == nil {
		return fmt.Errorf("ownership validator not configured")
	}
	for _, tx := range txs {
		if tx == nil {
			return fmt.Errorf("nil authorized transaction")
		}
	}
	if err := p.validateTransactionsLocked(txs); err != nil {
		return err
	}
	for _, tx := range txs {
		if tx.Time.IsZero() {
			tx.Time = time.Now()
		}
	}
	p.appendPendingLocked(txs, false)
	return nil
}

// ConfigureTransactionValidation is a startup-only immutable authorization
// hook. It is not a scheduler, queue bound or transaction-rate limit. Callbacks
// run under the pool lock and must be side-effect free and never reenter it.
func (p *TxPool) ConfigureTransactionValidation(check func(*Transaction) error) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if check == nil || p.transactionValidation != nil || len(p.TxQueue) != 0 || len(p.RelayPool) != 0 {
		return fmt.Errorf("transaction validation requires an empty startup pool")
	}
	p.transactionValidation = check
	return nil
}

func (p *TxPool) validateTransactionsLocked(txs []*Transaction) error {
	if p.transactionValidation == nil {
		return nil
	}
	if p.migrationJournal != nil || p.lanes != nil || p.sourcePreparedAccount != "" || (p.completionScheduling != nil && p.completionScheduling.config.Mode != "fifo") {
		return fmt.Errorf("authorized transaction pool does not support migration or reordered selection")
	}
	for _, tx := range txs {
		if err := p.transactionValidation(tx); err != nil {
			return err
		}
	}
	return nil
}
