package committee

import (
	"blockEmulator/core"
	"encoding/json"
	"fmt"
)

// Trusted local experimental transport: the command file is not a signed
// cross-shard certificate. Only an accepted, source-bound proposal is published.
func (m *RuntimeUnknownAdmissionManager) PublishNativeTargetInstallation(p core.SourcePreparation, path string) error {
	if m == nil {
		return fmt.Errorf("manager required")
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.publishNativeTargetInstallationLocked(p, path)
}
func (m *RuntimeUnknownAdmissionManager) publishNativeTargetInstallationLocked(p core.SourcePreparation, path string) error {
	a := m.integratedStateLocked().authority
	c := p.Command
	e, ok := a.preparedTransfers[c.ProposalID]["0x"+c.Account]
	if !ok || !a.requireInstallationReceipt || a.decisionByID[c.ProposalID].Status != mappingAccepted || e.SourceShard != c.SourceShard || e.TargetShard != c.TargetShard || e.MappingVersion != c.MappingVersion || e.AnchorVersion != c.AnchorVersion || e.StateHash != p.StateHash || e.SourceStateRoot != p.SourceStateRoot || hashSourcePreparedState(p.State) != e.StateHash {
		return fmt.Errorf("target installation requires matching accepted source evidence")
	}
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return core.PublishSourcePrepareFile(path, append(data, '\n'))
}
