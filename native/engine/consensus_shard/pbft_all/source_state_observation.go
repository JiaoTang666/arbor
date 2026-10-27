package pbft_all

import (
	"blockEmulator/core"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type sourceStateObservation struct {
	Account   string `json:"account"`
	Shard     uint64 `json:"shard"`
	Node      uint64 `json:"node"`
	Block     uint64 `json:"block"`
	BlockHash string `json:"block_hash"`
	StateRoot string `json:"state_root"`
	StateHash string `json:"state_hash"`
	State     []byte `json:"state"`
}

// Invoked under the proposal's consensus lock. This is local state evidence,
// not a quorum certificate, reservation, or authority to migrate the account.
func (n *PbftConsensusNode) exportSourceStateObservation() error {
	dir, account := os.Getenv("SAA_NATIVE_SOURCE_STATE_DIR"), os.Getenv("SAA_NATIVE_SOURCE_STATE_ACCOUNT")
	if dir == "" || account == "" {
		return nil
	}
	decoded, err := hex.DecodeString(account)
	if err != nil || len(decoded) != 20 || hex.EncodeToString(decoded) != account {
		return fmt.Errorf("source observation requires canonical account")
	}
	if n.NodeID != uint64(n.view.Load()) || n.CurChain.Get_PartitionMap(account) != n.ShardID {
		return nil
	}
	state, err := n.CurChain.ExportAccountState(account)
	if err != nil {
		return err
	}
	head := n.CurChain.CurrentBlock
	observation := sourceStateObservation{account, n.ShardID, n.NodeID, head.Header.Number, hex.EncodeToString(head.Hash), hex.EncodeToString(state.SourceRoot), hex.EncodeToString(state.StateHash), state.State.Encode()}
	raw, err := json.Marshal(observation)
	if err != nil {
		return err
	}
	return core.PublishSourcePrepareFile(filepath.Join(dir, fmt.Sprintf("s%d-n%d-%s.json", n.ShardID, n.NodeID, observation.BlockHash)), raw)
}
