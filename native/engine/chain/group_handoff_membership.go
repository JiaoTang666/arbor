package chain

import (
	"blockEmulator/core"
	"encoding/json"
	"fmt"
)

func (bc *BlockChain) validateGroupHandoffMembership(h core.MigrationHandoff) error {
	installed, err := bc.TargetInstallation(h.Account)
	if err != nil {
		return err
	}
	if len(h.GroupAccounts) == 0 {
		if installed != nil && installed.GroupID != "" {
			return fmt.Errorf("group member requires group-bound handoff")
		}
		return nil
	}
	p, err := bc.GroupTargetInstallation(h.ProposalID)
	if err != nil {
		return err
	}
	if p == nil || len(p.Prepared.Members) != len(h.GroupAccounts) {
		return fmt.Errorf("handoff group not installed")
	}
	st, err := bc.stateTrie()
	if err != nil {
		return err
	}
	for i, m := range p.Prepared.Members {
		if m.Command.Account != h.GroupAccounts[i] || m.Command.ProposalID != h.ProposalID || m.Command.SourceShard != h.SourceShard || m.Command.TargetShard != h.TargetShard {
			return fmt.Errorf("handoff group membership mismatch")
		}
		raw, e := st.Get(targetActivationKey(m.Command.Account))
		if e != nil {
			return e
		}
		var a TargetActivation
		if json.Unmarshal(raw, &a) != nil || a.ProposalID != h.ProposalID || a.MappingVersion != h.MappingVersion || a.InstallDigest != p.CommitDigest || a.StateHash != m.StateHash {
			return fmt.Errorf("handoff group not fully activated")
		}
	}
	return nil
}
