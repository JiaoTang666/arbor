package core

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Trusted-local execution evidence, not a standalone cryptographic proof.
// Executed is distinct from completion of a subsequent relay leg.
type HandoffAck struct {
	ID            string
	ProposalID    string
	TargetShard   uint64
	ExecutionHash string
	Block         uint64
	BlockHash     string
	StateRoot     string
	Executed      bool
	RelayPending  bool
}

func ValidateHandoffAck(h *MigrationHandoff, a HandoffAck) error {
	if err := ValidateMigrationHandoff(h); err != nil {
		return err
	}
	if len(h.GroupAccounts) > 0 {
		tx := DecodeTx(h.Execution)
		expectsRelay := !tx.Relayed && tx.RoutedRecipientShard != h.TargetShard
		if a.RelayPending != expectsRelay {
			return fmt.Errorf("group acknowledgement relay stage mismatch")
		}
	}
	hash := sha256.Sum256(h.Execution)
	valid := func(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == 32 }
	if a.ID != h.ID || a.ProposalID != h.ProposalID || a.TargetShard != h.TargetShard || a.ExecutionHash != hex.EncodeToString(hash[:]) || !a.Executed || a.Block == 0 || !valid(a.BlockHash) || !valid(a.StateRoot) {
		return fmt.Errorf("handoff acknowledgement mismatch")
	}
	return nil
}
