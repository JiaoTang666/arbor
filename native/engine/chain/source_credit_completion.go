package chain

import (
	"blockEmulator/core"
	"encoding/hex"
	"fmt"
)

// CollectSourceAppliedCredits reads only the committed root. It issues local
// source completion evidence, never fabricated target execution ACKs. The caller
// must use the same PBFT state serialization as other committed-root readers.
func (bc *BlockChain) CollectSourceAppliedCredits(txs []*core.Transaction, limit int) ([]core.SourceCreditConfirmation, error) {
	enabled, err := NativeEffectIndexConfiguration()
	if err != nil || !enabled {
		return nil, err
	}
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	var items []core.SourceCreditConfirmation
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		// A source-local original is complete only when this same committed
		// root contains both effects. A debit alone never proves remote credit.
		if !tx.Relayed && (!tx.RoutingFrozen || tx.RoutedSenderShard != bc.ChainConfig.ShardID || tx.RoutedRecipientShard != bc.ChainConfig.ShardID) {
			continue
		}
		status, err := LookupTransactionEffect(st, tx, core.TransactionCredit)
		if err != nil {
			return nil, err
		}
		if status == EffectUnknown {
			return nil, fmt.Errorf("source completion has unknown credit coverage")
		}
		if status != EffectSeen {
			continue
		}
		if !tx.RoutingFrozen || tx.RoutedRecipientShard != bc.ChainConfig.ShardID {
			return nil, fmt.Errorf("source credit completion has wrong local owner")
		}
		receipt, err := ReadTransactionEffectReceipt(st, tx, core.TransactionCredit)
		if err != nil {
			return nil, err
		}
		if receipt == nil || receipt.Block > bc.CurrentBlock.Header.Number {
			return nil, fmt.Errorf("source credit receipt is not committed at observed root")
		}
		item := core.SourceCreditConfirmation{Original: tx.Encode(), Effect: receipt.Effect,
			ExecutedBlock: receipt.Block, ObservedStateRoot: hex.EncodeToString(bc.CurrentBlock.Header.StateRoot)}
		if !tx.Relayed {
			debit, err := ReadTransactionEffectReceipt(st, tx, core.TransactionDebit)
			if err != nil {
				return nil, err
			}
			if debit == nil || debit.Block == 0 || debit.Block > bc.CurrentBlock.Header.Number {
				// A formerly local transaction may now debit at the migration
				// target and Relay its credit back here. This is NOT a local
				// completion. Leave it queued for the full handoff + final ACK.
				if debit == nil && bc.isReturnedHandoffCredit(tx, receipt) {
					continue
				}
				return nil, fmt.Errorf("ordinary source completion lacks committed debit: admission=%s credit_block=%d", tx.AdmissionID, receipt.Block)
			}
			item.DebitEffect, item.DebitExecutedBlock = &debit.Effect, debit.Block
		}
		items = append(items, item)
		if limit > 0 && len(items) >= limit {
			break
		}
	}
	return items, nil
}

// Recognize only the exact permitted transformation of a committed source
// preparation. A matching credit classifies work; it never authorizes dequeue
// or proves the remote debit. That remains the handoff ACK path's obligation.
func (bc *BlockChain) isReturnedHandoffCredit(tx *core.Transaction, receipt *TransactionEffectReceipt) bool {
	if tx.Relayed || receipt == nil || len(receipt.Transaction) == 0 {
		return false
	}
	// Read immutable command metadata only, not cloned account/effect snapshots
	// (which can be megabytes per group). No storage I/O under this read lock.
	bc.sourcePreparationMu.RLock()
	defer bc.sourcePreparationMu.RUnlock()
	groups := bc.groupSourceHistory
	if len(groups) == 0 && bc.groupSourcePreparation != nil {
		groups = []*core.GroupPreparation{bc.groupSourcePreparation}
	}
	for _, p := range groups {
		first := p.Command.Members[0]
		if first.SourceShard != bc.ChainConfig.ShardID || tx.MappingVersion > first.MappingVersion {
			continue
		}
		accounts := make([]string, len(p.Command.Members))
		for i, m := range p.Command.Members {
			accounts[i] = m.Account
		}
		h, err := core.BuildGroupMigrationHandoff(tx, first.ProposalID, accounts, first.SourceShard, first.TargetShard, first.MappingVersion+1)
		if err == nil && core.ValidateForwardedRelay(h, receipt.Transaction) == nil {
			return true
		}
	}
	return false
}

// Compatibility wrapper for synchronous/offline callers. The live PBFT path
// collects committed evidence above, then persists it outside consensus locks.
func (bc *BlockChain) ConfirmSourceAppliedCredits(txs []*core.Transaction) (int, error) {
	items, err := bc.CollectSourceAppliedCredits(txs, 0)
	if err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, nil
	}
	return bc.Txpool.ConfirmSourceCreditBatch(items)
}
