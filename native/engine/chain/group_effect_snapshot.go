package chain

import (
	"blockEmulator/core"
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/trie"
)

// Export only this group's account-prefixed effects from the SAME frozen trie
// as its account states. The source is trusted to export all entries; these
// records are not Merkle inclusion/completeness proofs or a BFT certificate.
func exportGroupEffectSnapshot(st *trie.Trie, command core.GroupPrepareCommand, sourceBlock uint64) ([]core.TransactionEffectSnapshot, error) {
	if err := validGroupIdentity(command); err != nil {
		return nil, err
	}
	if err := RequireMigrationEffectIndex(st); err != nil {
		return nil, err
	}
	if hex.EncodeToString(st.Hash().Bytes()) != command.Members[0].ExpectedStateRoot {
		return nil, fmt.Errorf("group effects and account snapshot use different roots")
	}
	var records []core.TransactionEffectSnapshot
	for _, member := range command.Members {
		prefix := []byte("__sef_transaction_effect_v1__/" + member.Account + "/")
		iterator := trie.NewIterator(st.NodeIterator(prefix))
		for iterator.Next() {
			if !bytes.HasPrefix(iterator.Key, prefix) {
				break
			}
			var receipt TransactionEffectReceipt
			if err := json.Unmarshal(iterator.Value, &receipt); err != nil {
				return nil, err
			}
			binding, err := st.Get(transactionAdmissionBindingKey(receipt.Effect.AdmissionID))
			if err != nil {
				return nil, err
			}
			record := core.TransactionEffectSnapshot{Original: append([]byte(nil), receipt.Transaction...),
				Receipt: append([]byte(nil), iterator.Value...), Binding: append([]byte(nil), binding...)}
			tx, decoded, _, err := decodeGroupEffectSnapshot(record)
			if err != nil || decoded.Block > sourceBlock || !bytes.Equal(transactionEffectKey(decoded.Effect), iterator.Key) {
				return nil, fmt.Errorf("invalid frozen group effect: %v", err)
			}
			status, err := LookupTransactionEffect(st, tx, decoded.Effect.Effect)
			if err != nil || status != EffectSeen {
				return nil, fmt.Errorf("exported group effect is not committed SEEN: %v", err)
			}
			records = append(records, record)
		}
		if iterator.Err != nil {
			return nil, iterator.Err
		}
	}
	return records, nil
}

func decodeGroupEffectSnapshot(record core.TransactionEffectSnapshot) (*core.Transaction, *TransactionEffectReceipt, transactionAdmissionBinding, error) {
	var receipt TransactionEffectReceipt
	var binding transactionAdmissionBinding
	var tx core.Transaction
	if err := json.Unmarshal(record.Receipt, &receipt); err != nil {
		return nil, nil, binding, err
	}
	if receipt.Block == 0 || len(record.Original) == 0 || !bytes.Equal(record.Original, receipt.Transaction) {
		return nil, nil, binding, fmt.Errorf("effect snapshot lacks its original transaction")
	}
	if err := gob.NewDecoder(bytes.NewReader(record.Original)).Decode(&tx); err != nil {
		return nil, nil, binding, err
	}
	effect, err := core.BuildTransactionEffect(&tx, receipt.Effect.Effect)
	if err != nil || effect != receipt.Effect {
		return nil, nil, binding, fmt.Errorf("effect snapshot transaction/receipt mismatch")
	}
	if err = json.Unmarshal(record.Binding, &binding); err != nil || binding != effectAdmissionBinding(effect) {
		return nil, nil, binding, fmt.Errorf("effect snapshot admission binding mismatch")
	}
	if len(receipt.Relay) > 0 {
		var relay core.Transaction
		if err = gob.NewDecoder(bytes.NewReader(receipt.Relay)).Decode(&relay); err != nil {
			return nil, nil, binding, err
		}
		bound, err := core.BuildTransactionEffect(&relay, core.TransactionDebit)
		if err != nil || effect.Effect != core.TransactionDebit || bound != effect || !relay.Relayed || !relay.RoutingFrozen || relay.RoutedSenderShard == relay.RoutedRecipientShard {
			return nil, nil, binding, fmt.Errorf("effect snapshot relay obligation mismatch")
		}
	}
	return &tx, &receipt, binding, nil
}

// Validate the entire batch before changing the caller's PRIVATE working trie.
// The caller commits it once together with account states and installation.
// Any trie update failure requires discarding that working trie, not a rollback.
func importGroupEffectSnapshot(st *trie.Trie, preparation core.GroupPreparation, installBlock uint64) error {
	if err := validGroupIdentity(preparation.Command); err != nil {
		return err
	}
	if preparation.EffectIndexVersion != 1 || preparation.PrepareBlock == 0 || installBlock == 0 {
		return fmt.Errorf("group effect snapshot lacks source/target coverage context")
	}
	if err := RequireMigrationEffectIndex(st); err != nil {
		return err
	}
	first := preparation.Command.Members[0]
	root, err := hex.DecodeString(first.ExpectedStateRoot)
	if err != nil || len(root) != 32 {
		return fmt.Errorf("group effect snapshot source root missing")
	}
	members := make(map[string]bool, len(preparation.Command.Members))
	for _, member := range preparation.Command.Members {
		members[member.Account] = true
	}
	type effectWrite struct{ key, receipt, bindingKey, binding []byte }
	writes := make([]effectWrite, 0, len(preparation.EffectRecords))
	bindings := map[string]transactionAdmissionBinding{}
	previousKey := ""
	for _, record := range preparation.EffectRecords {
		tx, receipt, binding, err := decodeGroupEffectSnapshot(record)
		if err != nil {
			return err
		}
		key := transactionEffectKey(receipt.Effect)
		if !members[receipt.Effect.Account] || receipt.Block >= preparation.PrepareBlock || string(key) <= previousKey {
			return fmt.Errorf("group effects must be account-bound, committed, unique and key-sorted")
		}
		previousKey = string(key)
		if old, exists := bindings[binding.AdmissionID]; exists && old != binding {
			return fmt.Errorf("conflicting admission within group effect snapshot")
		}
		bindings[binding.AdmissionID] = binding
		status, err := LookupTransactionEffect(st, tx, receipt.Effect.Effect)
		if err != nil {
			return err
		}
		if status == EffectSeen {
			continue // Exact same bound effect already exists; never overwrite it.
		}
		if status != EffectUnseen {
			return fmt.Errorf("target group effect has unknown coverage")
		}
		oldBlock := receipt.Block
		receipt.Block = installBlock
		receipt.ImportedSourceRoot, receipt.ImportedSourceBlock, receipt.ImportedSourceShard = first.ExpectedStateRoot, oldBlock, first.SourceShard
		encoded, err := json.Marshal(receipt)
		if err != nil {
			return err
		}
		writes = append(writes, effectWrite{key, encoded, transactionAdmissionBindingKey(binding.AdmissionID), append([]byte(nil), record.Binding...)})
	}
	for _, write := range writes {
		if err := st.Update(write.bindingKey, write.binding); err != nil {
			return err
		}
		if err := st.Update(write.key, write.receipt); err != nil {
			return err
		}
	}
	return nil
}
