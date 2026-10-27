package chain

import (
	"blockEmulator/core"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/trie"
)

type InboundDebitItem struct {
	Transaction    *core.Transaction
	MappingVersion uint64
}

func inboundItemKey(tx *core.Transaction, proposal string, version uint64) []byte {
	h := sha256.Sum256(append([]byte(fmt.Sprintf("%s|%d|", proposal, version)), tx.Encode()...))
	return []byte("__sef_inbound_debit_v1__" + hex.EncodeToString(h[:]))
}

func batchAccount(st *trie.Trie, address string, mustExist bool) (*core.AccountState, error) {
	state, _, err := readExecutionAccount(st, address, mustExist, nil)
	return state, err
}

// Both validation and commit evaluate the ordered batch in a private trie.
// No externally visible state or receipt is published until the shared root.
func (bc *BlockChain) applyInboundDebitBatch(items []InboundDebitItem, commit bool) error {
	if len(items) == 0 || uint64(len(items)) > bc.ChainConfig.BlockSize {
		return fmt.Errorf("invalid inbound batch size")
	}
	st, e := bc.stateTrie()
	if e != nil {
		return e
	}
	indexed, e := NativeEffectIndexConfiguration()
	if e != nil {
		return e
	}
	seen := map[string]bool{}
	changed := false
	for _, item := range items {
		record, e := bc.applyInboundDebit(item.Transaction, item.MappingVersion, false)
		if e != nil {
			return e
		}
		tx := item.Transaction
		key := inboundItemKey(tx, record.Credit.ProposalID, item.MappingVersion)
		if seen[string(key)] {
			return fmt.Errorf("duplicate inbound batch item")
		}
		seen[string(key)] = true
		old, e := st.Get(key)
		if e != nil {
			return e
		}
		if len(old) > 0 {
			continue
		}
		if indexed {
			debit := core.DecodeTx(record.Original)
			debit.RoutedRecipientShard = record.Credit.TargetShard
			if _, e = bc.applyIndexedRawRelay(st, debit, true); e != nil {
				return e
			}
		} else {
			sender, e := batchAccount(st, tx.Sender, tx.SenderRequiresExistingState)
			if e != nil {
				return e
			}
			if !sender.Deduct(tx.Value) {
				return fmt.Errorf("cumulative inbound balance insufficient")
			}
			sender.Nonce++
			if e = st.Update([]byte(tx.Sender), sender.Encode()); e != nil {
				return e
			}
		}
		raw, e := json.Marshal(record)
		if e != nil {
			return e
		}
		if e = st.Update(key, raw); e != nil {
			return e
		}
		changed = true
	}
	if !commit || !changed {
		return nil
	}
	_, e = bc.commitStateTrie(st)
	return e
}
func (bc *BlockChain) ValidateInboundDebitBatch(items []InboundDebitItem) error {
	return bc.applyInboundDebitBatch(items, false)
}
func (bc *BlockChain) CommitInboundDebitBatch(items []InboundDebitItem) error {
	return bc.applyInboundDebitBatch(items, true)
}

func (bc *BlockChain) applyHandoffRelayBatch(items []HandoffExecution, commit bool) error {
	if len(items) == 0 || uint64(len(items)) > bc.ChainConfig.BlockSize {
		return fmt.Errorf("invalid relay batch size")
	}
	st, e := bc.stateTrie()
	if e != nil {
		return e
	}
	indexed, e := NativeEffectIndexConfiguration()
	if e != nil {
		return e
	}
	seen := map[string]bool{}
	changed := false
	for _, first := range items {
		if seen[first.Handoff.ID] {
			return fmt.Errorf("duplicate relay batch item")
		}
		seen[first.Handoff.ID] = true
		if e = bc.ValidateHandoffRelay(first); e != nil {
			return e
		}
		key := []byte("__sef_handoff_relay_v1__" + first.Handoff.ID)
		old, e := st.Get(key)
		if e != nil {
			return e
		}
		if len(old) > 0 {
			continue
		}
		tx := core.DecodeTx(first.Relay)
		if indexed {
			if _, e = bc.applyIndexedRawRelay(st, tx, true); e != nil {
				return e
			}
		} else {
			recipient, e := batchAccount(st, tx.Recipient, tx.RecipientRequiresExistingState)
			if e != nil {
				return e
			}
			recipient.Deposit(tx.Value)
			if e = st.Update([]byte(tx.Recipient), recipient.Encode()); e != nil {
				return e
			}
		}
		record := HandoffRelayReceipt{HandoffID: first.Handoff.ID, Block: bc.CurrentBlock.Header.Number + 1, Recipient: tx.Recipient}
		raw, e := json.Marshal(record)
		if e != nil {
			return e
		}
		if e = st.Update(key, raw); e != nil {
			return e
		}
		changed = true
	}
	if !commit || !changed {
		return nil
	}
	_, e = bc.commitStateTrie(st)
	return e
}
func (bc *BlockChain) ValidateHandoffRelayBatch(items []HandoffExecution) error {
	return bc.applyHandoffRelayBatch(items, false)
}
func (bc *BlockChain) CommitHandoffRelayBatch(items []HandoffExecution) error {
	return bc.applyHandoffRelayBatch(items, true)
}
