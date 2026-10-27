package pbft_all

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Final-state audit requested by the finite baseline runner. It is read-only,
// after timed transaction completion, under pbftLock immediately before close.
func (n *PbftConsensusNode) writeCLPAEpochFinalState() error {
	input := os.Getenv("BLOCK_EMULATOR_CLPA_STATE_AUDIT_ACCOUNTS")
	if input == "" || (os.Getenv("BLOCK_EMULATOR_CLPA_EPOCH_V1") != "1" && os.Getenv("BLOCK_EMULATOR_FINAL_STATE_AUDIT_V1") != "1") {
		return nil
	}
	dir := os.Getenv("BLOCK_EMULATOR_CLPA_FINAL_STATE_DIR")
	if dir == "" {
		return fmt.Errorf("CLPA final state audit directory required")
	}
	raw, err := os.ReadFile(input)
	if err != nil {
		return err
	}
	var addresses []string
	if err = json.Unmarshal(raw, &addresses); err != nil {
		return err
	}
	sort.Strings(addresses)
	owned := make([]string, 0, len(addresses))
	for i, address := range addresses {
		b, err := hex.DecodeString(address)
		if err != nil || len(b) != 20 || hex.EncodeToString(b) != address || (i > 0 && addresses[i-1] == address) {
			return fmt.Errorf("invalid CLPA final state audit member")
		}
		if n.CurChain.Get_PartitionMap(address) == n.ShardID {
			owned = append(owned, address)
		}
	}
	states, err := n.CurChain.ExportCLPAEpochAccounts(owned)
	if err != nil {
		return err
	}
	type account struct {
		Address string `json:"address"`
		Owner   uint64 `json:"owner"`
		Balance string `json:"balance"`
		Nonce   uint64 `json:"nonce"`
	}
	accounts := make([]account, 0, len(states))
	for i, state := range states {
		accounts = append(accounts, account{owned[i], n.ShardID, state.Balance.String(), state.Nonce})
	}
	report := struct {
		Shard     uint64    `json:"shard"`
		Node      uint64    `json:"node"`
		Height    uint64    `json:"height"`
		StateRoot string    `json:"state_root"`
		Accounts  []account `json:"accounts"`
	}{n.ShardID, n.NodeID, n.CurChain.CurrentBlock.Header.Number, hex.EncodeToString(n.CurChain.CurrentBlock.Header.StateRoot), accounts}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("S%d_N%d.json", n.ShardID, n.NodeID)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = json.NewEncoder(f).Encode(report); err != nil {
		return err
	}
	return f.Sync()
}
