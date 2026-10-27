package committee

import (
	"blockEmulator/chain"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Validate a trusted-local retirement witness against this authority, without
// treating an arbitrary completion file as permission to stop the network.
func (m *RuntimeUnknownAdmissionManager) validateNativeRetirementLocked(id, account string) error {
	raw, err := os.ReadFile(os.Getenv("SAA_NATIVE_SOURCE_RETIRED_PATH"))
	if err != nil {
		return err
	}
	var witness struct {
		Retirement *chain.SourceRetirement `json:"retirement"`
		BlockHash  string                  `json:"block_hash"`
		StateRoot  string                  `json:"state_root"`
	}
	if err = json.Unmarshal(raw, &witness); err != nil {
		return err
	}
	r := witness.Retirement
	if r == nil || r.Block == 0 || r.Sequence == 0 {
		return fmt.Errorf("retirement commit identity missing")
	}
	for _, digest := range []string{r.Digest, witness.BlockHash, witness.StateRoot, r.Command.Drain.SnapshotHash} {
		if !validTransferDigest(digest) {
			return fmt.Errorf("retirement digest invalid")
		}
	}
	a := m.integratedStateLocked().authority
	install, ok := a.installationReceiptMeta[id][account]
	if !ok || !a.canFinalizeMigration(id, account, install.StateHash) {
		return fmt.Errorf("retirement requires applied matching installation")
	}
	expected := chain.TargetActivation{ProposalID: id, Account: strings.TrimPrefix(account, "0x"), TargetShard: install.TargetShard, MappingVersion: a.decisionByID[id].NewRouteVersion, InstallDigest: install.CommitDigest, StateHash: install.StateHash}
	if r.Command.Activation != expected || r.Command.Drain.Account != expected.Account || r.Command.Drain.Generation == 0 {
		return fmt.Errorf("retirement authority binding mismatch")
	}
	activated, err := os.ReadFile(os.Getenv("SAA_NATIVE_TARGET_ACTIVATED_PATH"))
	if err != nil {
		return err
	}
	hash := sha256.Sum256(activated)
	if r.Command.ActivationReceiptHash != hex.EncodeToString(hash[:]) {
		return fmt.Errorf("retirement activation receipt hash mismatch")
	}
	var target struct {
		Activation chain.TargetActivation `json:"activation"`
		Block      uint64                 `json:"block"`
		BlockHash  string                 `json:"block_hash"`
		StateRoot  string                 `json:"state_root"`
	}
	if err = json.Unmarshal(activated, &target); err != nil {
		return err
	}
	if target.Activation != expected || target.Block == 0 || !validTransferDigest(target.BlockHash) || !validTransferDigest(target.StateRoot) {
		return fmt.Errorf("retirement target activation invalid")
	}
	return nil
}
