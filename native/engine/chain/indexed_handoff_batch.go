package chain

import (
	"blockEmulator/core"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/trie"
)

func (bc *BlockChain) lookupHandoffExecution(st *trie.Trie, h core.MigrationHandoff) (*HandoffExecution, error) {
	if err := core.ValidateMigrationHandoff(&h); err != nil {
		return nil, err
	}
	if h.TargetShard != bc.ChainConfig.ShardID {
		return nil, fmt.Errorf("wrong handoff destination")
	}
	if err := bc.validateGroupHandoffMembership(h); err != nil {
		return nil, err
	}
	raw, err := st.Get(targetActivationKey(h.Account))
	if err != nil {
		return nil, err
	}
	var activation TargetActivation
	if len(raw) == 0 || json.Unmarshal(raw, &activation) != nil || activation.ProposalID != h.ProposalID || activation.MappingVersion != h.MappingVersion {
		return nil, fmt.Errorf("handoff lacks matching activation")
	}
	old, err := st.Get(handoffExecutionKey(h.ID))
	if err != nil {
		return nil, err
	}
	if len(old) > 0 {
		var record HandoffExecution
		if err = json.Unmarshal(old, &record); err != nil {
			return nil, err
		}
		a, _ := json.Marshal(record.Handoff)
		b, _ := json.Marshal(h)
		if !bytes.Equal(a, b) {
			return nil, fmt.Errorf("conflicting handoff replay")
		}
		return &record, nil
	}
	if err = bc.CheckPreparedTransactions([]*core.Transaction{core.DecodeTx(h.Execution)}); err != nil {
		return nil, err
	}
	return nil, nil
}

// Validation and execution simulate the same cumulative effects, including
// cross-envelope replays, on a private trie. Only execution publishes a root.
func (bc *BlockChain) applyIndexedHandoffBatch(items []core.MigrationHandoff, commit bool) error {
	if len(items) == 0 || uint64(len(items)) > bc.ChainConfig.BlockSize {
		return fmt.Errorf("invalid indexed handoff batch size")
	}
	st, err := bc.stateTrie()
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	changed := false
	for _, h := range items {
		if seen[h.ID] {
			return fmt.Errorf("duplicate handoff in batch")
		}
		seen[h.ID] = true
		old, err := bc.lookupHandoffExecution(st, h)
		if err != nil {
			return err
		}
		if old != nil {
			continue
		}
		if _, err = bc.applyIndexedRawRelay(st, core.DecodeTx(h.Execution), true); err != nil {
			return err
		}
		record, err := bc.indexedHandoffRecord(st, h)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err = st.Update(handoffExecutionKey(h.ID), raw); err != nil {
			return err
		}
		changed = true
	}
	if !commit || !changed {
		return nil
	}
	_, err = bc.commitStateTrie(st)
	return err
}
