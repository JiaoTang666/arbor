package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
)

// Trusted-local receipt validation, not a transferable Byzantine certificate.
func (n *PbftConsensusNode) confirmedGroupActivation(id ...string) (*chain.GroupTargetActivation, error) {
	a, _, err := n.readConfirmedGroupActivation(id...)
	return a, err
}

// Return the exact validated bytes so retirement cannot reread a different
// receipt between validation and proposal construction.
func (n *PbftConsensusNode) readConfirmedGroupActivation(id ...string) (*chain.GroupTargetActivation, []byte, error) {
	p := n.CurChain.GroupSourcePreparation(id...)
	if p == nil {
		return nil, nil, nil
	}
	raw, err := os.ReadFile(core.GroupTransportPath(os.Getenv("SAA_GROUP_TARGET_ACTIVATED_PATH"), p.Command.Members[0].ProposalID))
	if os.IsNotExist(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var r struct {
		Record    chain.GroupTargetActivation `json:"record"`
		Committed bool                        `json:"committed"`
		Block     uint64                      `json:"block"`
		BlockHash string                      `json:"block_hash"`
		StateRoot string                      `json:"state_root"`
		Shard     uint64                      `json:"shard"`
	}
	if err = json.Unmarshal(raw, &r); err != nil {
		return nil, nil, err
	}
	valid := func(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == 32 }
	if !r.Committed || r.Block == 0 || !valid(r.BlockHash) || !valid(r.StateRoot) || len(r.Record.Members) != len(p.Members) {
		return nil, nil, fmt.Errorf("incomplete group activation receipt")
	}
	for i, m := range p.Members {
		a := r.Record.Members[i]
		if a.Account != m.Command.Account || a.ProposalID != m.Command.ProposalID || a.TargetShard != m.Command.TargetShard || r.Shard != a.TargetShard || a.StateHash != m.StateHash || a.MappingVersion != m.Command.MappingVersion+1 || a.MappingVersion == 0 || !valid(a.InstallDigest) || a.InstallDigest != r.Record.Members[0].InstallDigest {
			return nil, nil, fmt.Errorf("group activation binding mismatch")
		}
	}
	return &r.Record, raw, nil
}
