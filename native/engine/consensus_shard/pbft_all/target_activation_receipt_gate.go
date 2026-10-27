package pbft_all

import (
	"blockEmulator/chain"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
)

// Shared-file trust boundary only: exact binding, not cryptographic proof.
func targetActivationConfirmed(c chain.TargetActivation) (bool, error) {
	path := os.Getenv("SAA_NATIVE_TARGET_ACTIVATED_PATH")
	if path == "" {
		return true, nil
	} // legacy test wiring; strict runner configures it
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var receipt struct {
		Activation chain.TargetActivation `json:"activation"`
		Block      uint64                 `json:"block"`
		BlockHash  string                 `json:"block_hash"`
		StateRoot  string                 `json:"state_root"`
	}
	if err = json.Unmarshal(raw, &receipt); err != nil {
		return false, err
	}
	hash, err := hex.DecodeString(receipt.BlockHash)
	if err != nil || len(hash) != 32 {
		return false, fmt.Errorf("invalid activation block hash")
	}
	root, err := hex.DecodeString(receipt.StateRoot)
	if err != nil || len(root) != 32 {
		return false, fmt.Errorf("invalid activation root")
	}
	if receipt.Activation != c || receipt.Block == 0 {
		return false, fmt.Errorf("target activation receipt mismatch")
	}
	return true, nil
}
