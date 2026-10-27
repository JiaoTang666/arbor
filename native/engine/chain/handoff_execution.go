package chain

import (
	"blockEmulator/core"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/trie"
)

type HandoffExecution struct {
	Handoff core.MigrationHandoff
	Block   uint64
	Relay   []byte
}

func handoffExecutionKey(id string) []byte { return []byte("__sef_handoff_executed_v1__" + id) }

// Caller must serialize this operation through consensus. The state changes,
// deduplication marker and any subsequent Relay payload share a committed root.
func (bc *BlockChain) CommitMigrationHandoff(h core.MigrationHandoff) (*HandoffExecution, error) {
	return bc.applyMigrationHandoff(h, true)
}
func (bc *BlockChain) ValidateMigrationHandoffExecution(h core.MigrationHandoff) error {
	_, err := bc.applyMigrationHandoff(h, false)
	return err
}
func (bc *BlockChain) applyMigrationHandoff(h core.MigrationHandoff, commit bool) (*HandoffExecution, error) {
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	if old, err := bc.lookupHandoffExecution(st, h); err != nil || old != nil {
		return old, err
	}
	tx := core.DecodeTx(h.Execution)
	if tx.Value.Sign() < 0 {
		return nil, fmt.Errorf("negative handoff value")
	}
	indexed, err := NativeEffectIndexConfiguration()
	if err != nil {
		return nil, err
	}
	if indexed {
		if _, err = bc.applyIndexedRawRelay(st, tx, true); err != nil {
			return nil, err
		}
		if !commit {
			return nil, nil
		}
		record, err := bc.indexedHandoffRecord(st, h)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			return nil, err
		}
		if err = st.Update(handoffExecutionKey(h.ID), encoded); err != nil {
			return nil, err
		}
		if _, err = bc.commitStateTrie(st); err != nil {
			return nil, err
		}
		return record, nil
	}
	if !tx.Relayed {
		sender, err := bc.handoffAccountState(tx.Sender, tx.SenderRequiresExistingState)
		if err != nil {
			return nil, err
		}
		if sender.Balance.Cmp(tx.Value) < 0 {
			return nil, fmt.Errorf("insufficient handoff balance")
		}
	}
	// Migrated endpoints must exist; ordinary counterparts retain native Relay
	// lazy creation semantics, including the configured initial balance.
	if bc.ResolveTxRecipientShard(tx) == bc.ChainConfig.ShardID {
		if _, err = bc.handoffAccountState(tx.Recipient, tx.RecipientRequiresExistingState); err != nil {
			return nil, err
		}
	}
	if !commit {
		return nil, nil
	}
	root := bc.GetUpdateStatusTrie([]*core.Transaction{tx})
	next, err := trie.New(trie.TrieID(root), bc.triedb)
	if err != nil {
		return nil, err
	}
	record := &HandoffExecution{Handoff: h, Block: bc.CurrentBlock.Header.Number + 1}
	if !tx.Relayed && bc.ResolveTxRecipientShard(tx) != bc.ChainConfig.ShardID {
		relay := core.DecodeTx(tx.Encode())
		relay.Relayed = true
		record.Relay = relay.Encode()
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if err = next.Update(handoffExecutionKey(h.ID), encoded); err != nil {
		return nil, err
	}
	if _, err = bc.commitStateTrie(next); err != nil {
		return nil, err
	}
	return record, nil
}
