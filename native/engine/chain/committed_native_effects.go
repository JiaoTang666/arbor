package chain

import (
	"blockEmulator/core"
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// ExecutedNativeTransactions distinguishes block occupancy from successfully
// applied effects. Read this block's committed root, not the newest chain root.
// Returned copies may be relayed without mutating the committed block record.
// This does not retry a debit's outstanding next leg after a network failure.
func (bc *BlockChain) ExecutedNativeTransactions(block *core.Block) ([]*core.Transaction, error) {
	if block == nil || block.Header == nil {
		return nil, fmt.Errorf("native execution report requires block")
	}
	applied, err := bc.committedBlockOutcomes(block)
	if err != nil {
		return nil, err
	}
	indexed, err := NativeEffectIndexConfiguration()
	if err != nil {
		return nil, err
	}
	if !indexed {
		result := make([]*core.Transaction, 0, len(block.Body))
		for i, tx := range block.Body {
			if applied[i] {
				result = append(result, core.DecodeTx(tx.Encode()))
			}
		}
		return result, nil
	}
	st, err := openStateTrieAtRoot(common.BytesToHash(block.Header.StateRoot), bc.triedb)
	if err != nil {
		return nil, err
	}
	result := make([]*core.Transaction, 0, len(block.Body))
	seen := map[string]bool{}
	for _, tx := range block.Body {
		if tx == nil {
			return nil, fmt.Errorf("nil transaction in committed block")
		}
		effect := core.TransactionDebit
		owner := bc.ResolveTxSenderShard(tx)
		if tx.Relayed {
			effect, owner = core.TransactionCredit, bc.ResolveTxRecipientShard(tx)
		}
		if owner != bc.ChainConfig.ShardID {
			return nil, fmt.Errorf("committed transaction stage belongs to another shard")
		}
		status, err := LookupTransactionEffect(st, tx, effect)
		if err != nil {
			return nil, err
		}
		if status == EffectUnseen {
			continue // E.g. native debit rejected for insufficient balance.
		}
		if status != EffectSeen {
			return nil, fmt.Errorf("committed transaction execution coverage unknown")
		}
		identity, err := core.BuildTransactionEffect(tx, effect)
		if err != nil {
			return nil, err
		}
		raw, err := st.Get(transactionEffectKey(identity))
		if err != nil {
			return nil, err
		}
		var receipt TransactionEffectReceipt
		if err = json.Unmarshal(raw, &receipt); err != nil {
			return nil, err
		}
		if receipt.Block > block.Header.Number {
			return nil, fmt.Errorf("effect receipt is later than its reported block")
		}
		if receipt.Block != block.Header.Number || seen[identity.ID] {
			continue // A prior-block replay is not new service or a fresh Relay1.
		}
		seen[identity.ID] = true
		result = append(result, core.DecodeTx(tx.Encode()))
	}
	return result, nil
}

// The outcome field is not independently signed. Only this node's verified
// commit or its atomically stored block is authoritative, never caller bits.
func (bc *BlockChain) committedBlockOutcomes(block *core.Block) ([]bool, error) {
	if block == nil || block.Header == nil || len(block.Hash) != 32 {
		return nil, fmt.Errorf("execution report requires a valid committed header")
	}
	body, err := transactionBodyDigest(block.Body)
	if err != nil {
		return nil, err
	}
	identity, err := committedExecutionIdentity(block, body)
	if err != nil {
		return nil, err
	}
	if cached := bc.committedExecution; cached != nil && cached.identity == identity {
		return cached.applied, nil
	}
	stored, err := bc.Storage.GetBlock(block.Hash)
	if err != nil {
		return nil, fmt.Errorf("execution result is not committed: %w", err)
	}
	storedBody, err := transactionBodyDigest(stored.Body)
	if err != nil {
		return nil, err
	}
	storedIdentity, err := committedExecutionIdentity(stored, storedBody)
	if err != nil || storedIdentity != identity {
		return nil, fmt.Errorf("execution report body differs from committed block")
	}
	if len(stored.ExecutionResults) != len(stored.Body) {
		return nil, fmt.Errorf("legacy block lacks locally verified execution outcomes")
	}
	return stored.ExecutionResults, nil
}
