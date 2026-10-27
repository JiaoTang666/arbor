package chain

import (
	"blockEmulator/core"
	"fmt"
	"math/big"
)

// Prevalidation includes cumulative balance effects. Commit still uses each
// existing durable identity: an I/O failure leaves a recoverable prefix, not
// a fictitious all-or-nothing batch or a new rollback protocol.
func (bc *BlockChain) ValidateMigrationHandoffBatch(items []core.MigrationHandoff) error {
	indexed, err := NativeEffectIndexConfiguration()
	if err != nil {
		return err
	}
	if indexed {
		return bc.applyIndexedHandoffBatch(items, false)
	}
	if len(items) == 0 || uint64(len(items)) > bc.ChainConfig.BlockSize {
		return fmt.Errorf("invalid handoff batch size")
	}
	seen := map[string]bool{}
	balances := map[string]*big.Int{}
	balance := func(a string, mustExist bool) (*big.Int, error) {
		if b, ok := balances[a]; ok {
			return b, nil
		}
		s, e := bc.handoffAccountState(a, mustExist)
		if e != nil {
			return nil, e
		}
		b := new(big.Int).Set(s.Balance)
		balances[a] = b
		return b, nil
	}
	for _, h := range items {
		if seen[h.ID] {
			return fmt.Errorf("duplicate handoff in batch")
		}
		seen[h.ID] = true
		old, e := bc.applyMigrationHandoff(h, false)
		if e != nil {
			return e
		}
		if old != nil {
			continue
		}
		tx := core.DecodeTx(h.Execution)
		if !tx.Relayed {
			b, e := balance(tx.Sender, tx.SenderRequiresExistingState)
			if e != nil {
				return e
			}
			if b.Cmp(tx.Value) < 0 {
				return fmt.Errorf("cumulative handoff balance insufficient")
			}
			b.Sub(b, tx.Value)
		}
		if bc.ResolveTxRecipientShard(tx) == bc.ChainConfig.ShardID {
			b, e := balance(tx.Recipient, tx.RecipientRequiresExistingState)
			if e != nil {
				return e
			}
			b.Add(b, tx.Value)
		}
	}
	return nil
}
