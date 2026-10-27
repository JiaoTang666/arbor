package chain

import (
	"blockEmulator/core"
	"fmt"
	"github.com/ethereum/go-ethereum/trie"
)

// The read is at the current committed root. It does not certify a remote
// debit, and UNKNOWN is deliberately different from an unexecuted effect.
func (bc *BlockChain) TransactionEffectStatus(tx *core.Transaction, effect string) (TransactionEffectStatus, error) {
	statuses, err := bc.TransactionEffectStatuses([]*core.Transaction{tx}, effect)
	if err != nil {
		return EffectUnknown, err
	}
	return statuses[0], nil
}

// Caller serializes with PBFT commit. A whole batch observes one immutable root.
func (bc *BlockChain) TransactionEffectStatuses(txs []*core.Transaction, effect string) ([]TransactionEffectStatus, error) {
	enabled, err := NativeEffectIndexConfiguration()
	if err != nil {
		return nil, err
	}
	statuses := make([]TransactionEffectStatus, len(txs))
	if !enabled {
		// Startup rejects disabling an index already present in the ledger.
		for i := range statuses {
			statuses[i] = EffectIndexDisabled
		}
		return statuses, nil
	}
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	for i, tx := range txs {
		statuses[i], err = LookupTransactionEffect(st, tx, effect)
		if err != nil {
			return nil, err
		}
	}
	return statuses, nil
}

func transactionEffectPending(st EffectIndexTrie, tx *core.Transaction, effect string) (bool, error) {
	status, err := LookupTransactionEffect(st, tx, effect)
	if err != nil {
		return false, err
	}
	switch status {
	case EffectIndexDisabled, EffectUnseen:
		return true, nil
	case EffectSeen:
		return false, nil
	default:
		return false, fmt.Errorf("transaction %s has unknown %s effect coverage", tx.AdmissionID, effect)
	}
}

// Indexed native execution and auxiliary batches use the same balance,
// first-owner and effect-marker rules. All updates stay in the caller's private
// trie; an error must discard that trie. No helper publishes a partial root.
func (bc *BlockChain) applyIndexedRawRelay(st *trie.Trie, tx *core.Transaction, strictBalance bool) (int, error) {
	if err := bc.RefreshOwnershipAuthorization(); err != nil {
		return 0, err
	}
	return bc.applyIndexedRawRelayObserved(st, tx, strictBalance, nil)
}

func (bc *BlockChain) applyIndexedRawRelayObserved(st *trie.Trie, tx *core.Transaction, strictBalance bool, timing *indexedExecutionTiming) (int, error) {
	if err := bc.CheckOwnershipTransaction(tx); err != nil {
		return 0, err
	}
	if timing != nil {
		timing.transactions++
	}
	if tx == nil || tx.Value == nil || tx.Value.Sign() < 0 {
		return 0, fmt.Errorf("invalid indexed transaction")
	}
	// Validate the actual execution route before any balance or marker write.
	// Ordinary transactions execute at the debit owner; only a Relay second
	// leg may execute independently at the credit owner.
	owner := bc.ResolveTxSenderShard(tx)
	if tx.Relayed {
		owner = bc.ResolveTxRecipientShard(tx)
	}
	if owner != bc.ChainConfig.ShardID {
		return 0, fmt.Errorf("indexed transaction stage belongs to shard %d, not %d", owner, bc.ChainConfig.ShardID)
	}
	started := executionTick(timing)
	_, err := core.BuildTransactionEffect(tx, core.TransactionCredit)
	timing.add(execIdentity, started)
	if err != nil {
		return 0, err
	}
	effects := EffectIndexTrie(st)
	if timing != nil {
		effects = observedEffectTrie{EffectIndexTrie: st, timing: timing}
	}
	changed := 0
	// false means an insufficient native debit: the whole transaction must
	// stop before credit. A matching prior debit still permits credit replay.
	apply := func(account, effect string, mustExist bool) (bool, error) {
		account = normalizePartitionMapKey(account)
		started := executionTick(timing)
		pending, err := transactionEffectPending(effects, tx, effect)
		timing.add(execEffectLookup, started)
		if err != nil || !pending {
			return true, err
		}
		state, existed, err := readExecutionAccount(st, account, mustExist, timing)
		if err != nil {
			return false, err
		}
		if effect == core.TransactionDebit {
			if !state.Deduct(tx.Value) {
				if strictBalance {
					return false, fmt.Errorf("indexed debit balance insufficient")
				}
				return false, nil
			}
			state.Nonce++
		} else {
			state.Deposit(tx.Value)
		}
		started = executionTick(timing)
		err = st.Update([]byte(account), state.Encode())
		timing.add(execStateWrite, started)
		if err != nil {
			return false, err
		}
		if !existed {
			role := "recipient"
			if effect == core.TransactionDebit {
				role = "sender"
			}
			started = executionTick(timing)
			err = bc.recordNativeFirstOwner(st, account, tx, role)
			timing.add(execFirstOwner, started)
			if err != nil {
				return false, err
			}
		}
		started = executionTick(timing)
		_, err = RecordTransactionEffect(effects, tx, effect, bc.CurrentBlock.Header.Number+1)
		timing.add(execEffectRecord, started)
		if err != nil {
			return false, err
		}
		changed++
		if timing != nil {
			timing.applied++
		}
		return true, nil
	}
	if !tx.Relayed && bc.ResolveTxSenderShard(tx) == bc.ChainConfig.ShardID {
		proceed, err := apply(tx.Sender, core.TransactionDebit, tx.SenderRequiresExistingState)
		if err != nil || !proceed {
			return 0, err // Native insufficient transfers cannot credit locally.
		}
	}
	if bc.ResolveTxRecipientShard(tx) == bc.ChainConfig.ShardID {
		if _, err := apply(tx.Recipient, core.TransactionCredit, tx.RecipientRequiresExistingState); err != nil {
			return 0, err
		}
	}
	return changed, nil
}

// Only a new envelope marker is written on an effect replay. If the debit
// already exists, its committed next leg cannot be silently retargeted.
func (bc *BlockChain) indexedHandoffRecord(st *trie.Trie, h core.MigrationHandoff) (*HandoffExecution, error) {
	tx := core.DecodeTx(h.Execution)
	record := &HandoffExecution{Handoff: h, Block: bc.CurrentBlock.Header.Number + 1}
	if !tx.Relayed && bc.ResolveTxRecipientShard(tx) != bc.ChainConfig.ShardID {
		receipt, err := ReadTransactionEffectReceipt(st, tx, core.TransactionDebit)
		if err != nil || receipt == nil || len(receipt.Relay) == 0 {
			return nil, fmt.Errorf("indexed handoff debit lacks durable relay: %v", err)
		}
		if err = core.ValidateForwardedRelay(&h, receipt.Relay); err != nil {
			return nil, fmt.Errorf("committed debit requires existing relay redirect: %w", err)
		}
		record.Relay = receipt.Relay
	}
	return record, nil
}
