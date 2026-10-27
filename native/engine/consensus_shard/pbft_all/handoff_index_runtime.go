package pbft_all

import (
	"blockEmulator/core"
	"encoding/json"
	"os"
)

func (n *PbftConsensusNode) handoffIndex(dir string) *core.HandoffIndex {
	key := "handoff-index-v1:" + dir
	value, ok := n.sourcePrepareSeen.Load(key)
	if !ok {
		value, _ = n.sourcePrepareSeen.LoadOrStore(key, core.NewHandoffIndex(dir, 32<<20))
	}
	return value.(*core.HandoffIndex)
}

func (n *PbftConsensusNode) indexedHandoffs(dir string, accept func(*core.MigrationHandoff) bool) ([]*core.MigrationHandoff, error) {
	records, observation, err := n.handoffIndex(dir).Load(accept)
	// One optional aggregate event per scan, no per-record synchronous audit.
	if os.Getenv("SAA_MIGRATION_OBSERVATION_V1") == "true" && n.pl != nil {
		var height interface{}
		var root interface{}
		if n.CurChain != nil && n.CurChain.CurrentBlock != nil {
			height = n.CurChain.CurrentBlock.Header.Number
			root = n.CurChain.CurrentBlock.Header.StateRoot
		}
		row := map[string]interface{}{"event": "handoff_index_observation_v1", "shard": n.ShardID, "node": n.NodeID, "sequence": n.sequenceID, "applied_height": height, "state_root_bytes": root, "mapping_version": nil, "observation_cutoff": nil, "stats": observation, "success": err == nil}
		raw, _ := json.Marshal(row)
		n.pl.Plog.Printf("%s", raw)
	}
	return records, err
}
