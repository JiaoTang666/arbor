package chain

import (
	"blockEmulator/core"
	"bytes"
	"encoding/json"
	"fmt"
)

type RelayRedirect struct {
	First HandoffExecution
	Child core.MigrationHandoff
	Block uint64
}

func redirectKey(id string) []byte { return []byte("__sef_relay_redirect_v1__" + id) }

func (bc *BlockChain) ValidateRelayRedirect(first HandoffExecution) error {
	_, err := bc.applyRelayRedirect(first, false)
	return err
}
func (bc *BlockChain) CommitRelayRedirect(first HandoffExecution) (*RelayRedirect, error) {
	return bc.applyRelayRedirect(first, true)
}

// Serialized by PBFT at the OLD recipient shard. The same committed state
// excludes both ordinary credit and a conflicting redirect for this parent.
func (bc *BlockChain) applyRelayRedirect(first HandoffExecution, commit bool) (*RelayRedirect, error) {
	if first.Block == 0 {
		return nil, fmt.Errorf("missing first debit execution")
	}
	if err := core.ValidateForwardedRelay(&first.Handoff, first.Relay); err != nil {
		return nil, err
	}
	tx := core.DecodeTx(first.Relay)
	if tx.RoutedRecipientShard != bc.ChainConfig.ShardID {
		return nil, fmt.Errorf("wrong redirect source")
	}
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	status, err := LookupTransactionEffect(st, tx, core.TransactionCredit)
	if err != nil {
		return nil, err
	}
	if status == EffectSeen {
		return nil, fmt.Errorf("credit already committed at original recipient")
	}
	if status == EffectUnknown {
		return nil, fmt.Errorf("redirect credit coverage unknown")
	}
	raw, err := st.Get([]byte("__sef_handoff_relay_v1__" + first.Handoff.ID))
	if err != nil {
		return nil, err
	}
	if len(raw) > 0 {
		return nil, fmt.Errorf("credit already committed at original recipient")
	}
	raw, err = st.Get(redirectKey(first.Handoff.ID))
	if err != nil {
		return nil, err
	}
	if len(raw) > 0 {
		var old RelayRedirect
		if err = json.Unmarshal(raw, &old); err != nil {
			return nil, err
		}
		a, _ := json.Marshal(old.First)
		b, _ := json.Marshal(first)
		if !bytes.Equal(a, b) {
			return nil, fmt.Errorf("conflicting relay redirect replay")
		}
		return &old, nil
	}
	var prepared *core.GroupPreparation
	for _, p := range bc.GroupSourcePreparations() {
		for _, m := range p.Members {
			if m.Command.Account == normalizePartitionMapKey(tx.Recipient) {
				prepared = p
				break
			}
		}
	}
	if prepared == nil {
		return nil, fmt.Errorf("recipient has no prepared migration")
	}
	command := prepared.Command.Members[0]
	accounts := make([]string, len(prepared.Members))
	for i, m := range prepared.Members {
		accounts[i] = m.Command.Account
	}
	child, err := core.BuildGroupMigrationHandoff(tx, command.ProposalID, accounts, command.SourceShard, command.TargetShard, command.MappingVersion+1)
	if err != nil {
		return nil, err
	}
	if err = core.ValidateRelayRedirectLink(&first.Handoff, first.Relay, child); err != nil {
		return nil, err
	}
	record := &RelayRedirect{First: first, Child: *child, Block: bc.CurrentBlock.Header.Number + 1}
	if !commit {
		return record, nil
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if err = st.Update(redirectKey(first.Handoff.ID), encoded); err != nil {
		return nil, err
	}
	if _, err = bc.commitStateTrie(st); err != nil {
		return nil, err
	}
	return record, nil
}

// The target still requires its actual group installation and activation.
// A trusted source-block report is checked by the PBFT transport before entry.
func (bc *BlockChain) CommitRedirectCredit(r RelayRedirect) (*HandoffExecution, error) {
	if r.Block == 0 {
		return nil, fmt.Errorf("redirect not committed")
	}
	if err := core.ValidateRelayRedirectLink(&r.First.Handoff, r.First.Relay, &r.Child); err != nil {
		return nil, err
	}
	return bc.CommitMigrationHandoff(r.Child)
}
