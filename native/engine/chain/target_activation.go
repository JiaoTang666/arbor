package chain

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// SEF-authorized local transport command, not a cryptographic certificate.
type TargetActivation struct {
	TargetShard    uint64 `json:"target_shard"`
	ProposalID     string `json:"proposal_id"`
	Account        string `json:"account"`
	MappingVersion uint64 `json:"mapping_version"`
	InstallDigest  string `json:"install_digest"`
	StateHash      string `json:"state_hash"`
}

var targetActivatedIndex = []byte("__sef_target_activated_v1__")

func targetActivationKey(account string) []byte {
	return []byte("__sef_target_activation_v1__" + account)
}
func (bc *BlockChain) TargetActivated(account string) (bool, error) {
	st, err := bc.stateTrie()
	if err != nil {
		return false, err
	}
	raw, err := st.Get(targetActivationKey(account))
	return len(raw) > 0, err
}
func (bc *BlockChain) CommitTargetActivation(c TargetActivation) error {
	return bc.applyTargetActivation(c, true)
}
func (bc *BlockChain) ValidateTargetActivation(c TargetActivation) error {
	return bc.applyTargetActivation(c, false)
}
func (bc *BlockChain) applyTargetActivation(c TargetActivation, commit bool) error {
	p, err := bc.TargetInstallation(c.Account)
	if err != nil {
		return err
	}
	if p == nil || c.ProposalID != p.Prepared.Command.ProposalID || c.MappingVersion <= p.Prepared.Command.MappingVersion || c.InstallDigest != p.CommitDigest || c.StateHash != p.Prepared.StateHash {
		return fmt.Errorf("activation does not match target installation")
	}
	if p.GroupID != "" {
		return fmt.Errorf("group member requires whole-group activation")
	}
	st, err := bc.stateTrie()
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(c)
	if err != nil {
		return err
	}
	old, err := st.Get(targetActivationKey(c.Account))
	if err != nil {
		return err
	}
	if len(old) > 0 {
		if bytes.Equal(old, encoded) {
			return nil
		}
		return fmt.Errorf("conflicting target activation")
	}
	accounts, err := bc.targetHeldAccounts()
	if err != nil {
		return err
	}
	remaining := make([]string, 0, len(accounts))
	found := false
	for _, a := range accounts {
		if a == c.Account {
			found = true
		} else {
			remaining = append(remaining, a)
		}
	}
	if !found {
		return fmt.Errorf("target activation lacks hold")
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
	active = append(active, c.Account)
	holds, _ := json.Marshal(remaining)
	activated, _ := json.Marshal(active)
	if err = st.Update(targetHoldIndexKey, holds); err != nil {
		return err
	}
	if err = st.Update(targetActivatedIndex, activated); err != nil {
		return err
	}
	if err = st.Update(targetActivationKey(c.Account), encoded); err != nil {
		return err
	}
	if _, err = bc.commitStateTrie(st); err != nil {
		return err
	}
	bc.Update_PartitionMap(c.Account, bc.ChainConfig.ShardID)
	bc.Txpool.ReleaseMigrationAccount(c.Account)
	return nil
}
func (bc *BlockChain) restoreTargetActivationRoutes() error {
	st, err := bc.stateTrie()
	if err != nil {
		return err
	}
	raw, err := st.Get(targetActivatedIndex)
	if err != nil || len(raw) == 0 {
		return err
	}
	var active []string
	if err = json.Unmarshal(raw, &active); err != nil {
		return err
	}
	for _, a := range active {
		ok, err := bc.TargetActivated(a)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("missing target activation marker")
		}
		bc.Update_PartitionMap(a, bc.ChainConfig.ShardID)
	}
	return nil
}
