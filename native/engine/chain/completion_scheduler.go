package chain

import (
	"blockEmulator/core"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

func (bc *BlockChain) configureCompletionScheduling() error {
	cfg, err := core.CompletionSchedulingFromEnv()
	if err != nil || cfg == nil {
		return err
	}
	return bc.Txpool.EnableCompletionScheduling(*cfg, bc.ChainConfig.ShardID, bc.ChainConfig.ShardNums)
}

// Local proposal diagnostics only; actual completion remains established by
// native commit evidence, not by this selector's stage count.
func (bc *BlockChain) reportCompletionScheduling(block *core.Block) {
	a := bc.Txpool.CompletionSchedulingAudit()
	if a.Mode == "off" {
		return
	}
	row := struct {
		core.CompletionScheduleAudit
		Shard        uint64 `json:"shard"`
		Height       uint64 `json:"proposal_height"`
		ProposalHash string `json:"proposal_hash"`
	}{a, bc.ChainConfig.ShardID, block.Header.Number, hex.EncodeToString(block.Hash)}
	if raw, err := json.Marshal(row); err == nil {
		fmt.Printf("completion_schedule %s\n", raw)
	}
}
