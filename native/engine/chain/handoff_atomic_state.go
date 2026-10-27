package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"math/big"
)

// Apply every new item to one private trie, then publish one committed root.
// Existing per-item markers make a replay after commit/receipt interruption
// idempotent. No receipt is published by this function before root publication.
func (bc *BlockChain) CommitMigrationHandoffBatch(items []core.MigrationHandoff) error {
	indexed, configErr := NativeEffectIndexConfiguration()
	if configErr != nil {
		return configErr
	}
	if indexed {
		return bc.applyIndexedHandoffBatch(items, true)
	}
	if err := bc.ValidateMigrationHandoffBatch(items); err != nil {
		return err
	}
	st, err := bc.stateTrie()
	if err != nil {
		return err
	}
	changed := false
	load := func(account string, mustExist bool) (*core.AccountState, error) {
		raw, e := st.Get([]byte(account))
		if e != nil {
			return nil, e
		}
		if len(raw) == 0 {
			if mustExist {
				return nil, fmt.Errorf("batch account absent: %s", account)
			}
			return &core.AccountState{Balance: new(big.Int).Set(params.Init_Balance)}, nil
		}
		return core.DecodeAS(raw), nil
	}
	for _, h := range items {
		old, e := st.Get(handoffExecutionKey(h.ID))
		if e != nil {
			return e
		}
		if len(old) > 0 {
			continue
		}
		tx := core.DecodeTx(h.Execution)
		if !tx.Relayed {
			state, e := load(tx.Sender, tx.SenderRequiresExistingState)
			if e != nil {
				return e
			}
			if !state.Deduct(tx.Value) {
				return fmt.Errorf("batch debit insufficient")
			}
			state.Nonce++
			if e = st.Update([]byte(tx.Sender), state.Encode()); e != nil {
				return e
			}
		}
		if bc.ResolveTxRecipientShard(tx) == bc.ChainConfig.ShardID {
			state, e := load(tx.Recipient, tx.RecipientRequiresExistingState)
			if e != nil {
				return e
			}
			state.Deposit(tx.Value)
			if e = st.Update([]byte(tx.Recipient), state.Encode()); e != nil {
				return e
			}
		}
		record := HandoffExecution{Handoff: h, Block: bc.CurrentBlock.Header.Number + 1}
		if !tx.Relayed && bc.ResolveTxRecipientShard(tx) != bc.ChainConfig.ShardID {
			relay := core.DecodeTx(h.Execution)
			relay.Relayed = true
			record.Relay = relay.Encode()
		}
		raw, e := json.Marshal(record)
		if e != nil {
			return e
		}
		if e = st.Update(handoffExecutionKey(h.ID), raw); e != nil {
			return e
		}
		changed = true
	}
	if !changed {
		return nil
	}
	_, err = bc.commitStateTrie(st)
	return err
}
