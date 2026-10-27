package chain

import (
	"blockEmulator/core"
	"fmt"
	"time"
)

// Called immediately after AddBlock, while commit is serialized. No synthetic
// receipts are emitted for inclusion without a real absent -> present change.
func (bc *BlockChain) FirstCreationReceipts(block *core.Block, at time.Time) ([]core.FirstCreationReceipt, error) {
	out := []core.FirstCreationReceipt{}
	applied, err := bc.committedBlockOutcomes(block)
	if err != nil {
		return nil, err
	}
	for i, tx := range block.Body {
		if !applied[i] {
			continue
		}
		b := tx.FirstPlacement
		if b == nil {
			continue
		}
		if e := b.Validate(tx, b.Session); e != nil {
			return nil, e
		}
		for account, shard := range b.Claims {
			if shard != bc.ChainConfig.ShardID {
				continue
			}
			o, e := bc.ObserveFirstCreation(block.Hash, CreatorExpectation{AdmissionID: b.AdmissionID, TxHash: b.TxHash, Account: account, Shard: shard, MappingVersion: b.MappingVersion}, at)
			if e != nil {
				return nil, fmt.Errorf("creator commit observation: %w", e)
			}
			out = append(out, core.FirstCreationReceipt{Binding: *b, Account: account, Shard: shard, BlockHeight: o.BlockHeight, BlockHash: o.BlockHash, ParentStateRoot: o.ParentStateRoot, StateRoot: o.StateRoot, StateHash: o.StateHash, ObservedAt: o.ObservedAt})
		}
	}
	return out, nil
}
