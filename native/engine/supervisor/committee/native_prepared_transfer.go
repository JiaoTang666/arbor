package committee

import (
	"encoding/hex"
	"fmt"
)

func validTransferDigest(value string) bool {
	b, err := hex.DecodeString(value)
	return err == nil && len(b) == 32
}

// registerPreparedTransfer binds source-export evidence to an ACCEPTED proposal,
// before installation and before activation. It never changes the route map.
func (a *mappingAuthority) registerPreparedTransfer(e NativeStateTransferEnvelope) error {
	p, ok := a.pending[e.ProposalID]
	if !ok || a.decisionByID[e.ProposalID].Status != mappingAccepted {
		return fmt.Errorf("prepared transfer requires pending accepted proposal")
	}
	member := false
	for _, account := range p.Addresses {
		member = member || account == e.Account
	}
	if !member || e.SourceShard != p.SourceShard || e.TargetShard != p.TargetShard ||
		e.MappingVersion != p.BaseRouteVersion || e.SourceShard == e.TargetShard ||
		!validTransferDigest(e.SourceStateRoot) || !validTransferDigest(e.StateHash) {
		return fmt.Errorf("prepared transfer does not match proposal/state contract")
	}
	if a.preparedTransfers[e.ProposalID] == nil {
		a.preparedTransfers[e.ProposalID] = make(map[string]NativeStateTransferEnvelope)
	}
	if previous, exists := a.preparedTransfers[e.ProposalID][e.Account]; exists && previous != e {
		return fmt.Errorf("conflicting prepared transfer")
	}
	a.preparedTransfers[e.ProposalID][e.Account] = e
	return nil
}

func (a *mappingAuthority) matchesPreparedReceipt(r nativeInstallationReceiptMeta) bool {
	e, ok := a.preparedTransfers[r.ProposalID][r.Account]
	return ok && r.Installed && r.Committed && r.InstallBlock > 0 &&
		validTransferDigest(r.TargetStateRoot) && validTransferDigest(r.CommitDigest) &&
		r.SourceShard == e.SourceShard && r.TargetShard == e.TargetShard &&
		r.MappingVersion == e.MappingVersion && r.AnchorVersion == e.AnchorVersion &&
		r.SourceStateRoot == e.SourceStateRoot && r.StateHash == e.StateHash
}

// RegisterNativePreparedTransfer is for the source-export adapter, not a policy
// prediction. Callers must supply the hash/root of the actual exported state.
func (m *RuntimeUnknownAdmissionManager) RegisterNativePreparedTransfer(e NativeStateTransferEnvelope) error {
	if m == nil {
		return fmt.Errorf("manager required")
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.integratedStateLocked().authority.registerPreparedTransfer(e)
}
