package committee

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func (m *RuntimeUnknownAdmissionManager) publishTargetActivationLocked(id, account, path string) error {
	a := m.integratedStateLocked().authority
	r, ok := a.installationReceiptMeta[id][account]
	if !ok || !a.requireInstallationReceipt || !a.canFinalizeMigration(id, account, r.StateHash) {
		return fmt.Errorf("activation requires applied proposal and matched committed installation")
	}
	c := chain.TargetActivation{TargetShard: r.TargetShard, ProposalID: id, Account: strings.TrimPrefix(account, "0x"), MappingVersion: a.decisionByID[id].NewRouteVersion, InstallDigest: r.CommitDigest, StateHash: r.StateHash}
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return core.PublishSourcePrepareFile(path, append(data, '\n'))
}

// Single-command experimental transport. Refuse ambiguous multiple candidates
// instead of silently choosing one or overwriting another proposal's command.
func (m *RuntimeUnknownAdmissionManager) publishDueTargetActivationLocked() error {
	path := os.Getenv("SAA_NATIVE_TARGET_ACTIVATION_PATH")
	if path == "" {
		return nil
	}
	a := m.integratedStateLocked().authority
	var id, account string
	for candidate, records := range a.preparedTransfers {
		if a.decisionByID[candidate].Status != mappingApplied {
			continue
		}
		for addr := range records {
			if id != "" {
				return fmt.Errorf("single activation inbox has multiple candidates")
			}
			id = candidate
			account = addr
		}
	}
	if id == "" {
		return nil
	}
	return m.publishTargetActivationLocked(id, account, path)
}
