package committee

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// NativeStateTransferEnvelope binds a state payload to the authority decision.
// It is transport-neutral; consensus handlers must verify it before installing.
type NativeStateTransferEnvelope struct {
	ProposalID      string `json:"proposal_id"`
	Account         string `json:"account"`
	SourceShard     uint64 `json:"source_shard"`
	TargetShard     uint64 `json:"target_shard"`
	MappingVersion  uint64 `json:"mapping_version"`
	AnchorVersion   uint64 `json:"anchor_version"`
	SourceStateRoot string `json:"source_state_root"`
	StateHash       string `json:"state_hash"`
}

func (e NativeStateTransferEnvelope) Validate(decision mappingAuthorityDecision, installed bool) error {
	if e.ProposalID == "" || e.ProposalID != decision.ProposalID {
		return fmt.Errorf("proposal mismatch")
	}
	// Installation must be valid while activation is still pending; requiring
	// applied here creates an installation <-> activation circular dependency.
	if decision.Status != mappingAccepted && decision.Status != mappingApplied {
		return fmt.Errorf("authority decision not accepted")
	}
	if e.SourceShard == e.TargetShard {
		return fmt.Errorf("non-migration transfer")
	}
	for name, value := range map[string]string{"source_root": e.SourceStateRoot, "state_hash": e.StateHash} {
		b, err := hex.DecodeString(value)
		if err != nil || len(b) != 32 {
			return fmt.Errorf("invalid %s", name)
		}
	}
	if installed {
		return nil
	}
	return fmt.Errorf("target installation not acknowledged")
}

func StateTransferReceiptHash(proposalID, account, stateHash string) string {
	h := sha256.Sum256([]byte(proposalID + "|" + account + "|" + stateHash))
	return hex.EncodeToString(h[:])
}
