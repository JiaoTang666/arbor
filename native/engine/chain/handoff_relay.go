package chain

import (
	"blockEmulator/core"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/trie"
)

type HandoffRelayReceipt struct {
	HandoffID string
	Block     uint64
	Recipient string
}

// Trusted experimental delivery of the durable first-leg record. Network
// admission must bind this record to the originating committed execution.
func (bc *BlockChain) CommitHandoffRelay(first HandoffExecution) (*HandoffRelayReceipt, error) {
	return bc.applyHandoffRelay(first, true)
}
func (bc *BlockChain) ValidateHandoffRelay(first HandoffExecution) error {
	_, err := bc.applyHandoffRelay(first, false)
	return err
}
func (bc *BlockChain) applyHandoffRelay(first HandoffExecution, commit bool) (*HandoffRelayReceipt, error) {
	h := first.Handoff
	if err := core.ValidateMigrationHandoff(&h); err != nil {
		return nil, err
	}
	if h.Leg != "debit" || first.Block == 0 || len(first.Relay) == 0 {
		return nil, fmt.Errorf("relay requires executed debit leg")
	}
	expected := core.DecodeTx(h.Execution)
	expected.Relayed = true
	if core.ValidateForwardedRelay(&h, first.Relay) != nil || expected.RoutedRecipientShard != bc.ChainConfig.ShardID || h.TargetShard == bc.ChainConfig.ShardID {
		return nil, fmt.Errorf("relay destination or payload mismatch")
	}
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	redirect, err := st.Get(redirectKey(h.ID))
	if err != nil {
		return nil, err
	}
	if len(redirect) > 0 {
		return nil, fmt.Errorf("relay credit reserved for redirected execution")
	}
	key := []byte("__sef_handoff_relay_v1__" + h.ID)
	raw, err := st.Get(key)
	if err != nil {
		return nil, err
	}
	if len(raw) > 0 {
		var old HandoffRelayReceipt
		err = json.Unmarshal(raw, &old)
		return &old, err
	}
	if err = bc.CheckPreparedTransactions([]*core.Transaction{expected}); err != nil {
		return nil, err
	}
	if _, err = bc.handoffAccountState(expected.Recipient, expected.RecipientRequiresExistingState); err != nil {
		return nil, err
	}
	indexed, err := NativeEffectIndexConfiguration()
	if err != nil {
		return nil, err
	}
	if indexed {
		if _, err = bc.applyIndexedRawRelay(st, expected, true); err != nil {
			return nil, err
		}
	}
	if !commit {
		return nil, nil
	}
	next := st
	if !indexed {
		root := bc.GetUpdateStatusTrie([]*core.Transaction{expected})
		next, err = trie.New(trie.TrieID(root), bc.triedb)
		if err != nil {
			return nil, err
		}
	}
	receipt := &HandoffRelayReceipt{HandoffID: h.ID, Block: bc.CurrentBlock.Header.Number + 1, Recipient: expected.Recipient}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return nil, err
	}
	if err = next.Update(key, encoded); err != nil {
		return nil, err
	}
	if _, err = bc.commitStateTrie(next); err != nil {
		return nil, err
	}
	return receipt, nil
}
