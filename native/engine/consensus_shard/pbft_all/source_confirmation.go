package pbft_all

import (
	"blockEmulator/core"
	"encoding/json"
	"fmt"
	"time"
)

func (n *PbftConsensusNode) pollSourceConfirmation() error {
	_, result := n.sourceConfirmationLane().poll()
	if result == nil {
		return nil
	}
	if n.pl != nil && n.pl.Plog != nil {
		n.pl.Plog.Printf("source_confirmation_complete elapsedMs=%d success=%t", result.elapsed.Milliseconds(), result.err == nil)
	}
	return result.err // Failed immutable jobs remain fenced for exact retry.
}

// Snapshot all slices, nested transaction fields and receipts before the
// worker owns them. Only pool+immutable records+directory enter the closure.
func copyCompletedHandoffs(items []core.CompletedHandoffConfirmation) ([]core.CompletedHandoffConfirmation, error) {
	raw, err := json.Marshal(items)
	if err != nil {
		return nil, err
	}
	var owned []core.CompletedHandoffConfirmation
	if err = json.Unmarshal(raw, &owned); err != nil {
		return nil, err
	}
	return owned, nil
}

func (n *PbftConsensusNode) queueCompletedHandoffs(dir string, items []core.CompletedHandoffConfirmation) (bool, error) {
	if len(items) == 0 || n.stopSignal.Load() || n.sourceConfirmationLane().active() {
		return false, nil
	}
	limit := int(n.CurChain.ChainConfig.BlockSize)
	if limit < 1 {
		limit = 1
	}
	if len(items) > limit {
		items = items[:limit]
	}
	owned, err := copyCompletedHandoffs(items)
	if err != nil {
		return false, err
	}
	ids := make([]string, len(owned))
	for i, item := range owned {
		if item.H == nil {
			return false, fmt.Errorf("confirmation requires handoff")
		}
		ids[i] = item.H.ID
	}
	pool, logger := n.CurChain.Txpool, n.pl
	return n.sourceConfirmationLane().start(ids, func() error {
		start := time.Now()
		timing, err := pool.ConfirmCompletedHandoffBatchObserved(dir, owned, func(stage string) {
			if logger != nil && logger.Plog != nil {
				logger.Plog.Printf("source_confirmation_stage first=%s items=%d phase=%s elapsedMs=%d", ids[0], len(owned), stage, time.Since(start).Milliseconds())
			}
		})
		if logger != nil && logger.Plog != nil {
			logger.Plog.Printf("source_confirmation_detail items=%d contact_related=%d durable_removed=%d outboxMs=%d validateMs=%d gateWaitMs=%d poolWaitMs=%d scanMs=%d receiptCheckMs=%d receiptMs=%d removeMs=%d confirm_persist_ms=%d compactMs=%d totalMs=%d success=%t", timing.Items, timing.RelatedLen, timing.Removed, timing.OutboxRead.Milliseconds(), timing.Validation.Milliseconds(), timing.JournalGateWait.Milliseconds(), timing.PoolLockWait.Milliseconds(), timing.PoolScan.Milliseconds(), timing.ReceiptCheck.Milliseconds(), timing.ReceiptPersist.Milliseconds(), timing.RemovePersist.Milliseconds(), timing.ConfirmPersist.Milliseconds(), timing.Compact.Milliseconds(), timing.Total.Milliseconds(), err == nil)
		}
		return err
	}), nil
}

// Read proofs while the caller owns PBFT serialization, not on the I/O worker.
// Limit each owned task to one native block's worth of confirmations.
func (n *PbftConsensusNode) queueSourceCompletedCredits(pending []*core.Transaction) (bool, error) {
	if n.stopSignal.Load() || n.sourceConfirmationLane().active() {
		return false, nil
	}
	limit := int(n.CurChain.ChainConfig.BlockSize)
	if limit < 1 {
		limit = 1
	}
	items, err := n.CurChain.CollectSourceAppliedCredits(pending, limit)
	if err != nil || len(items) == 0 {
		return false, err
	}
	// The collector creates owned byte slices and effect copies at one root.
	pool, logger := n.CurChain.Txpool, n.pl
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = "source-credit/" + item.Effect.ID
	}
	return n.sourceConfirmationLane().start(ids, func() error {
		start := time.Now()
		removed, err := pool.ConfirmSourceCreditBatch(items)
		if logger != nil && logger.Plog != nil {
			logger.Plog.Printf("source_credit_confirmation items=%d removed=%d totalMs=%d success=%t", len(items), removed, time.Since(start).Milliseconds(), err == nil)
		}
		return err
	}), nil
}
