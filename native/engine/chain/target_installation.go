package chain

import (
	"blockEmulator/core"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Installation identity and account bytes share one trie commit. This does
// not grant execution authority or change the routing map.
type TargetInstallation struct {
	GroupID         string                 `json:"group_id,omitempty"`
	Prepared        core.SourcePreparation `json:"prepared"`
	InstallBlock    uint64                 `json:"install_block"`
	InstallSequence uint64                 `json:"install_sequence"`
	CommitDigest    string                 `json:"commit_digest"`
}

func targetInstallationKey(account string) []byte {
	return []byte("__sef_target_install_v1__" + account)
}

func (bc *BlockChain) TargetInstallation(account string) (*TargetInstallation, error) {
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	data, err := st.Get(targetInstallationKey(account))
	if err != nil || len(data) == 0 {
		return nil, err
	}
	var record TargetInstallation
	if err = json.Unmarshal(data, &record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (bc *BlockChain) CommitPreparedTarget(p core.SourcePreparation, sequence uint64, digest []byte) (*TargetInstallation, error) {
	indexed, err := NativeEffectIndexConfiguration()
	if err != nil {
		return nil, err
	}
	if indexed {
		return nil, fmt.Errorf("indexed migration requires whole-group installation")
	}
	return bc.prepareTarget(p, sequence, digest, true)
}
func (bc *BlockChain) ValidatePreparedTarget(p core.SourcePreparation, sequence uint64, digest []byte) error {
	_, err := bc.prepareTarget(p, sequence, digest, false)
	return err
}
func (bc *BlockChain) prepareTarget(p core.SourcePreparation, sequence uint64, digest []byte, commit bool) (*TargetInstallation, error) {
	c := p.Command
	address, err := hex.DecodeString(c.Account)
	validDigest := func(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == 32 }
	if err != nil || len(address) != 20 || normalizePartitionMapKey(c.Account) != c.Account || c.ProposalID == "" || c.TargetShard != bc.ChainConfig.ShardID || c.SourceShard == c.TargetShard || c.SourceShard >= bc.ChainConfig.ShardNums || sequence == 0 || len(digest) != 32 || p.PrepareBlock == 0 || p.PrepareSequence == 0 || !validDigest(p.CommitDigest) || !validDigest(p.SourceStateRoot) || p.SourceStateRoot != c.ExpectedStateRoot || p.StateHash != c.ExpectedStateHash {
		return nil, fmt.Errorf("invalid target preparation identity")
	}
	hash := sha256.Sum256(p.State)
	if hex.EncodeToString(hash[:]) != p.StateHash {
		return nil, fmt.Errorf("prepared bytes hash mismatch")
	}
	var state core.AccountState
	if err = gob.NewDecoder(bytes.NewReader(p.State)).Decode(&state); err != nil || state.Balance == nil || state.Balance.Sign() < 0 {
		return nil, fmt.Errorf("invalid prepared account encoding")
	}
	if old, err := bc.TargetInstallation(c.Account); err != nil {
		return nil, err
	} else if old != nil {
		before, _ := json.Marshal(old.Prepared)
		after, _ := json.Marshal(p)
		if !bytes.Equal(before, after) || old.InstallSequence != sequence || old.CommitDigest != hex.EncodeToString(digest) {
			return nil, fmt.Errorf("conflicting target installation")
		}
		return old, nil
	}
	if err = bc.rejectPreparedWrite(c.Account); err != nil {
		return nil, err
	}
	st, err := bc.stateTrie()
	if err != nil {
		return nil, err
	}
	current, err := st.Get([]byte(c.Account))
	if err != nil {
		return nil, err
	}
	if len(current) > 0 {
		return nil, fmt.Errorf("target account already exists without installation identity")
	}
	record := &TargetInstallation{Prepared: p, InstallBlock: bc.CurrentBlock.Header.Number + 1, InstallSequence: sequence, CommitDigest: hex.EncodeToString(digest)}
	if !commit {
		return record, nil
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if err = st.Update([]byte(c.Account), p.State); err != nil {
		return nil, err
	}
	if err = st.Update(targetInstallationKey(c.Account), encoded); err != nil {
		return nil, err
	}
	accounts, err := bc.targetHeldAccounts()
	if err != nil {
		return nil, err
	}
	accounts = append(accounts, c.Account)
	index, err := json.Marshal(accounts)
	if err != nil {
		return nil, err
	}
	if err = st.Update(targetHoldIndexKey, index); err != nil {
		return nil, err
	}
	if _, err = bc.commitStateTrie(st); err != nil {
		return nil, err
	}
	bc.Txpool.HoldSourceAccount(c.Account)
	return record, nil
}
