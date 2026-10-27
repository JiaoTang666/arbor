package chain

import (
	"blockEmulator/core"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

var groupTargetKey = []byte("__sef_group_target_install_v1__")
var groupActivatedKey = []byte("__sef_group_target_activation_v1__")
var groupActivationBlockKey = []byte("__sef_group_target_activation_block_v1__")

func (bc *BlockChain) GroupTargetActivationBlock(id ...string) (uint64, error) {
	st, err := bc.stateTrie()
	if err != nil {
		return 0, err
	}
	raw, err := st.Get(groupRecordKey(groupActivationBlockKey, id...))
	if err != nil || len(raw) == 0 {
		return 0, err
	}
	var block uint64
	err = json.Unmarshal(raw, &block)
	return block, err
}

type GroupTargetInstallation struct {
	Prepared        core.GroupPreparation `json:"prepared"`
	InstallBlock    uint64                `json:"install_block"`
	InstallSequence uint64                `json:"install_sequence"`
	CommitDigest    string                `json:"commit_digest"`
}

type GroupTargetActivation struct {
	Members []TargetActivation `json:"members"`
}

func (bc *BlockChain) GroupTargetInstallation(id ...string) (*GroupTargetInstallation, error) {
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	raw, err := st.Get(groupRecordKey(groupTargetKey, id...))
	if err == nil && len(raw) == 0 && len(id) > 0 {
		raw, err = st.Get(groupTargetKey)
	}
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	var p GroupTargetInstallation
	if err = json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	if len(id) > 0 && (len(p.Prepared.Members) == 0 || p.Prepared.Members[0].Command.ProposalID != id[0]) {
		return nil, nil
	}
	return &p, nil
}

func (bc *BlockChain) ValidateGroupTargetInstallation(p core.GroupPreparation, sequence uint64, digest []byte) error {
	_, err := bc.prepareGroupTarget(p, sequence, digest, false)
	return err
}

func (bc *BlockChain) CommitGroupTargetInstallation(p core.GroupPreparation, sequence uint64, digest []byte) (*GroupTargetInstallation, error) {
	return bc.prepareGroupTarget(p, sequence, digest, true)
}

func (bc *BlockChain) prepareGroupTarget(p core.GroupPreparation, sequence uint64, digest []byte, commit bool) (*GroupTargetInstallation, error) {
	if err := validGroupIdentity(p.Command); err != nil {
		return nil, err
	}
	if err := bc.validateGroupEffectCoverage(p.EffectIndexVersion, true); err != nil {
		return nil, err
	}
	if len(p.Members) != len(p.Command.Members) || sequence == 0 || len(digest) != 32 || p.PrepareBlock == 0 || p.PrepareSequence == 0 {
		return nil, fmt.Errorf("invalid group installation context")
	}
	if old, err := bc.GroupTargetInstallation(p.Command.Members[0].ProposalID); err != nil {
		return nil, err
	} else if old != nil {
		a, _ := json.Marshal(old.Prepared)
		b, _ := json.Marshal(p)
		if !bytes.Equal(a, b) || old.InstallSequence != sequence || old.CommitDigest != hex.EncodeToString(digest) {
			return nil, fmt.Errorf("conflicting group installation")
		}
		return old, nil
	}
	for i, m := range p.Members {
		if m.Command != p.Command.Members[i] || m.PrepareBlock != p.PrepareBlock || m.PrepareSequence != p.PrepareSequence || m.CommitDigest != p.CommitDigest {
			return nil, fmt.Errorf("source group member binding mismatch")
		}
		if old, err := bc.TargetInstallation(m.Command.Account); err != nil {
			return nil, err
		} else if old != nil {
			return nil, fmt.Errorf("group member already installed outside group")
		}
		if err := bc.ValidatePreparedTarget(m, sequence, digest); err != nil {
			return nil, err
		}
	}
	record := &GroupTargetInstallation{Prepared: *cloneGroup(&p), InstallBlock: bc.CurrentBlock.Header.Number + 1, InstallSequence: sequence, CommitDigest: hex.EncodeToString(digest)}
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	if p.EffectIndexVersion == 1 {
		if err = importGroupEffectSnapshot(st, p, record.InstallBlock); err != nil {
			return nil, err
		}
	} else if len(p.EffectRecords) != 0 {
		return nil, fmt.Errorf("legacy group cannot declare indexed effects")
	}
	if !commit {
		return record, nil
	}
	accounts, err := bc.targetHeldAccounts()
	if err != nil {
		return nil, err
	}
	for _, m := range p.Members {
		member := TargetInstallation{GroupID: m.Command.ProposalID, Prepared: m, InstallBlock: record.InstallBlock, InstallSequence: sequence, CommitDigest: record.CommitDigest}
		encoded, err := json.Marshal(member)
		if err != nil {
			return nil, err
		}
		if err = st.Update([]byte(m.Command.Account), m.State); err != nil {
			return nil, err
		}
		if err = st.Update(targetInstallationKey(m.Command.Account), encoded); err != nil {
			return nil, err
		}
		accounts = append(accounts, m.Command.Account)
	}
	index, err := json.Marshal(accounts)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if err = st.Update(targetHoldIndexKey, index); err != nil {
		return nil, err
	}
	if err = st.Update(groupTargetKey, encoded); err != nil {
		return nil, err
	}
	if err = st.Update(groupRecordKey(groupTargetKey, p.Command.Members[0].ProposalID), encoded); err != nil {
		return nil, err
	}
	if _, err = bc.commitStateTrie(st); err != nil {
		return nil, err
	}
	held := make([]string, len(p.Members))
	for i, m := range p.Members {
		held[i] = m.Command.Account
	}
	bc.Txpool.HoldSourceAccounts(held)
	return record, nil
}

func (bc *BlockChain) ValidateGroupTargetActivation(c GroupTargetActivation) error {
	return bc.applyGroupTargetActivation(c, false)
}
func (bc *BlockChain) CommitGroupTargetActivation(c GroupTargetActivation) error {
	return bc.applyGroupTargetActivation(c, true)
}
func (bc *BlockChain) applyGroupTargetActivation(c GroupTargetActivation, commit bool) error {
	if len(c.Members) == 0 {
		return fmt.Errorf("empty group activation")
	}
	id := c.Members[0].ProposalID
	p, err := bc.GroupTargetInstallation(id)
	if err != nil {
		return err
	}
	if p == nil || len(c.Members) != len(p.Prepared.Members) {
		return fmt.Errorf("group activation requires complete installation")
	}
	for i, m := range c.Members {
		source := p.Prepared.Members[i]
		if m.Account != source.Command.Account || m.ProposalID != source.Command.ProposalID || m.TargetShard != bc.ChainConfig.ShardID ||
			m.StateHash != source.StateHash || m.InstallDigest != p.CommitDigest || m.MappingVersion != source.Command.MappingVersion+1 || m.MappingVersion == 0 {
			return fmt.Errorf("group activation member mismatch")
		}
	}
	st, err := bc.stateTrie()
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(c)
	if err != nil {
		return err
	}
	old, err := st.Get(groupRecordKey(groupActivatedKey, id))
	if err != nil {
		return err
	}
	if len(old) > 0 {
		if bytes.Equal(old, encoded) {
			return nil
		}
		return fmt.Errorf("conflicting group activation")
	}
	holds, err := bc.targetHeldAccounts()
	if err != nil {
		return err
	}
	members := make(map[string]bool)
	for _, m := range c.Members {
		members[m.Account] = true
	}
	remaining := []string{}
	found := 0
	for _, a := range holds {
		if members[a] {
			found++
		} else {
			remaining = append(remaining, a)
		}
	}
	if found != len(c.Members) {
		return fmt.Errorf("incomplete group holds")
	}
	if !commit {
		return nil
	}
	raw, err := st.Get(targetActivatedIndex)
	if err != nil {
		return err
	}
	var active []string
	if len(raw) > 0 {
		if err = json.Unmarshal(raw, &active); err != nil {
			return err
		}
	}
	for _, m := range c.Members {
		member, err := json.Marshal(m)
		if err != nil {
			return err
		}
		if err = st.Update(targetActivationKey(m.Account), member); err != nil {
			return err
		}
		active = append(active, m.Account)
	}
	holdRaw, err := json.Marshal(remaining)
	if err != nil {
		return err
	}
	activeRaw, err := json.Marshal(active)
	if err != nil {
		return err
	}
	if err = st.Update(targetHoldIndexKey, holdRaw); err != nil {
		return err
	}
	if err = st.Update(targetActivatedIndex, activeRaw); err != nil {
		return err
	}
	if err = st.Update(groupActivatedKey, encoded); err != nil {
		return err
	}
	if err = st.Update(groupRecordKey(groupActivatedKey, id), encoded); err != nil {
		return err
	}
	blockRaw, err := json.Marshal(bc.CurrentBlock.Header.Number + 1)
	if err != nil {
		return err
	}
	if err = st.Update(groupActivationBlockKey, blockRaw); err != nil {
		return err
	}
	if err = st.Update(groupRecordKey(groupActivationBlockKey, id), blockRaw); err != nil {
		return err
	}
	if _, err = bc.commitStateTrie(st); err != nil {
		return err
	}
	for _, m := range c.Members {
		bc.Update_PartitionMap(m.Account, bc.ChainConfig.ShardID)
		bc.Txpool.ReleaseMigrationAccount(m.Account)
	}
	return nil
}
