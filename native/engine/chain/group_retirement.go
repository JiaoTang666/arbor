package chain

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type GroupRetirementCommand struct {
	ActivationReceipt []byte `json:"activation_receipt"`
	DrainSnapshot     []byte `json:"drain_snapshot"`
}
type GroupSourceRetirement struct {
	Command  GroupRetirementCommand `json:"command"`
	Block    uint64                 `json:"block"`
	Sequence uint64                 `json:"sequence"`
	Digest   string                 `json:"digest"`
}

var groupRetirementKey = []byte("__sef_group_retirement_v1__")

func (bc *BlockChain) GroupSourceRetirement(id ...string) (*GroupSourceRetirement, error) {
	if len(id) == 0 {
		if p := bc.GroupSourcePreparation(); p != nil {
			id = []string{p.Command.Members[0].ProposalID}
		}
	}
	st, e := bc.stateTrie()
	if e != nil {
		return nil, e
	}
	raw, e := st.Get(groupRecordKey(groupRetirementKey, id...))
	if e == nil && len(raw) == 0 && len(id) > 0 {
		raw, e = st.Get(groupRetirementKey)
	}
	if e != nil || len(raw) == 0 {
		return nil, e
	}
	var r GroupSourceRetirement
	e = json.Unmarshal(raw, &r)
	if e == nil && len(id) > 0 {
		got, err := retirementProposalID(r.Command)
		if err != nil {
			return nil, err
		}
		if got != id[0] {
			return nil, nil
		}
	}
	return &r, e
}
func (bc *BlockChain) ValidateGroupRetirement(c GroupRetirementCommand) error {
	id, err := retirementProposalID(c)
	if err != nil {
		return err
	}
	p := bc.GroupSourcePreparation(id)
	if p == nil {
		return fmt.Errorf("group not prepared")
	}
	var r struct {
		Record    GroupTargetActivation `json:"record"`
		Committed bool                  `json:"committed"`
		Block     uint64                `json:"block"`
		BlockHash string                `json:"block_hash"`
		StateRoot string                `json:"state_root"`
		Shard     uint64                `json:"shard"`
	}
	if e := json.Unmarshal(c.ActivationReceipt, &r); e != nil {
		return e
	}
	valid := func(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == 32 }
	if !r.Committed || r.Block == 0 || !valid(r.BlockHash) || !valid(r.StateRoot) || len(r.Record.Members) != len(p.Members) {
		return fmt.Errorf("group retirement activation proof invalid")
	}
	var snapshot struct {
		Kind         string
		Account      string
		Accounts     []string
		Sequence     uint64
		Transactions [][]byte
	}
	if e := json.Unmarshal(c.DrainSnapshot, &snapshot); e != nil {
		return e
	}
	// Missing Kind is the original full-snapshot format. A delta, even empty,
	// cannot certify that all previously admitted work has drained.
	if (snapshot.Kind != "" && snapshot.Kind != "full") || snapshot.Account != "" || snapshot.Sequence == 0 || len(snapshot.Transactions) != 0 || len(snapshot.Accounts) != len(p.Members) {
		return fmt.Errorf("group retirement needs empty whole-group generation")
	}
	for i, m := range p.Members {
		a := r.Record.Members[i]
		if snapshot.Accounts[i] != m.Command.Account || a.Account != m.Command.Account || a.ProposalID != m.Command.ProposalID || a.TargetShard != m.Command.TargetShard || r.Shard != a.TargetShard || a.StateHash != m.StateHash || a.MappingVersion != m.Command.MappingVersion+1 || a.MappingVersion == 0 || !valid(a.InstallDigest) || a.InstallDigest != r.Record.Members[0].InstallDigest {
			return fmt.Errorf("group retirement member mismatch")
		}
	}
	old, e := bc.GroupSourceRetirement(id)
	if e != nil {
		return e
	}
	if old != nil && (!bytes.Equal(old.Command.ActivationReceipt, c.ActivationReceipt) || !bytes.Equal(old.Command.DrainSnapshot, c.DrainSnapshot)) {
		return fmt.Errorf("conflicting group retirement")
	}
	return bc.checkPreparedRoot(bc.CurrentBlock.Header.StateRoot)
}
func (bc *BlockChain) CommitGroupRetirement(c GroupRetirementCommand, seq uint64, digest []byte) (*GroupSourceRetirement, error) {
	if seq == 0 || len(digest) != 32 {
		return nil, fmt.Errorf("group retirement commit identity missing")
	}
	if e := bc.ValidateGroupRetirement(c); e != nil {
		return nil, e
	}
	id, e := retirementProposalID(c)
	if e != nil {
		return nil, e
	}
	old, e := bc.GroupSourceRetirement(id)
	if e != nil {
		return nil, e
	}
	if old != nil {
		if old.Sequence != seq || old.Digest != hex.EncodeToString(digest) {
			return nil, fmt.Errorf("group retirement replay identity mismatch")
		}
		return old, nil
	}
	r := &GroupSourceRetirement{Command: c, Block: bc.CurrentBlock.Header.Number + 1, Sequence: seq, Digest: hex.EncodeToString(digest)}
	raw, e := json.Marshal(r)
	if e != nil {
		return nil, e
	}
	st, e := bc.stateTrie()
	if e != nil {
		return nil, e
	}
	if e = retireSourceGeneration(st, bc.GroupSourcePreparations(), id); e != nil {
		return nil, e
	}
	if e = st.Update(groupRetirementKey, raw); e != nil {
		return nil, e
	}
	if e = st.Update(groupRecordKey(groupRetirementKey, id), raw); e != nil {
		return nil, e
	}
	if _, e = bc.commitStateTrie(st); e != nil {
		return nil, e
	}
	// Keep ALL source fences, archival states and the forwarding journal.
	return r, nil
}

func retirementProposalID(c GroupRetirementCommand) (string, error) {
	var receipt struct {
		Record GroupTargetActivation `json:"record"`
	}
	if err := json.Unmarshal(c.ActivationReceipt, &receipt); err != nil {
		return "", err
	}
	if len(receipt.Record.Members) == 0 || receipt.Record.Members[0].ProposalID == "" {
		return "", fmt.Errorf("retirement missing proposal identity")
	}
	return receipt.Record.Members[0].ProposalID, nil
}
