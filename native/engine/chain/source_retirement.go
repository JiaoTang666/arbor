package chain

import (
	"blockEmulator/core"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Retirement ends execution ownership, not the responsibility to forward
// old-route arrivals. The frozen source bytes remain archival evidence.
type SourceRetirementCommand struct {
	Activation            TargetActivation            `json:"activation"`
	ActivationReceiptHash string                      `json:"activation_receipt_hash"`
	Drain                 core.MigrationDrainBoundary `json:"drain"`
}
type SourceRetirement struct {
	Command  SourceRetirementCommand `json:"command"`
	Block    uint64                  `json:"block"`
	Sequence uint64                  `json:"sequence"`
	Digest   string                  `json:"digest"`
}

var sourceRetirementKey = []byte("__sef_source_retirement_v1__")

func (bc *BlockChain) SourceRetirement() (*SourceRetirement, error) {
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	raw, err := st.Get(sourceRetirementKey)
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	var result SourceRetirement
	if err = json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// The PBFT adapter must additionally validate receipt bytes and the local
// drain boundary before proposing. Hash shape alone is not that validation.
func (bc *BlockChain) ValidateSourceRetirement(c SourceRetirementCommand) error {
	p := bc.SourcePreparation()
	if p == nil {
		return fmt.Errorf("source not prepared")
	}
	a := c.Activation
	if a.ProposalID != p.Command.ProposalID || a.Account != p.Command.Account || a.TargetShard != p.Command.TargetShard || a.StateHash != p.StateHash || a.MappingVersion <= p.Command.MappingVersion || c.Drain.Account != a.Account || c.Drain.Generation == 0 {
		return fmt.Errorf("retirement binding mismatch")
	}
	for _, s := range []string{a.InstallDigest, c.ActivationReceiptHash, c.Drain.SnapshotHash} {
		raw, err := hex.DecodeString(s)
		if err != nil || len(raw) != 32 {
			return fmt.Errorf("retirement evidence digest invalid")
		}
	}
	old, err := bc.SourceRetirement()
	if err != nil {
		return err
	}
	if old != nil && old.Command != c {
		return fmt.Errorf("conflicting source retirement")
	}
	return bc.checkPreparedRoot(bc.CurrentBlock.Header.StateRoot)
}

func (bc *BlockChain) CommitSourceRetirement(c SourceRetirementCommand, sequence uint64, digest []byte) (*SourceRetirement, error) {
	if sequence == 0 || len(digest) != 32 {
		return nil, fmt.Errorf("retirement commit identity required")
	}
	if err := bc.ValidateSourceRetirement(c); err != nil {
		return nil, err
	}
	old, err := bc.SourceRetirement()
	if err != nil {
		return nil, err
	}
	if old != nil {
		if old.Sequence != sequence || old.Digest != hex.EncodeToString(digest) {
			return nil, fmt.Errorf("retirement replay identity mismatch")
		}
		return old, nil
	}
	record := SourceRetirement{c, bc.CurrentBlock.Header.Number + 1, sequence, hex.EncodeToString(digest)}
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	if err = st.Update(sourceRetirementKey, raw); err != nil {
		return nil, err
	}
	if _, err = bc.commitStateTrie(st); err != nil {
		return nil, err
	}
	// Intentionally keep preparation, source fence and forwarding journal.
	return &record, nil
}
