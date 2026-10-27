package committee

import (
	"blockEmulator/core"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
)

// SEF is the only producer of the accepted preparation inbox. Native validates
// the committed source snapshot; SEF validates the global mapping version here.
func (m *RuntimeUnknownAdmissionManager) PublishNativeSourcePreparation(c core.SourcePrepareCommand, path string) error {
	if m == nil {
		return fmt.Errorf("manager required")
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.publishNativeSourcePreparationLocked(c, path)
}

func (m *RuntimeUnknownAdmissionManager) publishNativeSourcePreparationLocked(c core.SourcePrepareCommand, path string) error {
	a := m.integratedStateLocked().authority
	p, ok := a.pending[c.ProposalID]
	account := normalizeMappingAuthorityAccount("0x" + c.Account)
	member := false
	for _, address := range p.Addresses {
		member = member || address == account
	}
	snapshotValid := validTransferDigest(c.ExpectedBlockHash) && validTransferDigest(c.ExpectedStateRoot) && validTransferDigest(c.ExpectedStateHash)
	if c.BindAtProposal {
		snapshotValid = c.ExpectedBlockHash == "" && c.ExpectedStateRoot == "" && c.ExpectedStateHash == ""
	}
	if !ok || !member || a.decisionByID[c.ProposalID].Status != mappingAccepted ||
		p.SourceShard != c.SourceShard || p.TargetShard != c.TargetShard || p.BaseRouteVersion != c.MappingVersion ||
		!a.requireInstallationReceipt || c.SourceShard == c.TargetShard ||
		!snapshotValid {
		return fmt.Errorf("source preparation requires matching accepted gated proposal and snapshot")
	}
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return core.PublishSourcePrepareFile(path, append(data, '\n'))
}

// A candidate is untrusted input, not an authority decision. Reuse the same
// accepted-proposal gate as the public API before publishing the native inbox.
func (m *RuntimeUnknownAdmissionManager) publishSourceCandidateIfEnabledLocked() error {
	path := os.Getenv("SAA_NATIVE_SOURCE_CANDIDATE_PATH")
	if path == "" {
		return nil
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var c core.SourcePrepareCommand
	if err = json.Unmarshal(raw, &c); err != nil {
		return err
	}
	if m.integratedStateLocked().authority.decisionByID[c.ProposalID].Status == mappingApplied {
		return nil
	}
	return m.publishNativeSourcePreparationLocked(c, os.Getenv("SAA_NATIVE_SOURCE_PREPARE_PATH"))
}

func (m *RuntimeUnknownAdmissionManager) consumeNativeSourcePreparedLocked(path string) error {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var p core.SourcePreparation
	if err = json.Unmarshal(data, &p); err != nil {
		return err
	}
	// Check byte hash without decoding an untrusted gob payload.
	if p.PrepareBlock == 0 || p.PrepareSequence == 0 || !validTransferDigest(p.CommitDigest) ||
		p.SourceStateRoot != p.Command.ExpectedStateRoot || p.StateHash != p.Command.ExpectedStateHash || len(p.State) == 0 {
		return fmt.Errorf("invalid native preparation evidence")
	}
	// AccountState.Hash is SHA256(Encode); verify the exact exported bytes.
	if hashSourcePreparedState(p.State) != p.StateHash {
		return fmt.Errorf("native prepared state hash mismatch")
	}
	e := NativeStateTransferEnvelope{ProposalID: p.Command.ProposalID, Account: "0x" + p.Command.Account,
		SourceShard: p.Command.SourceShard, TargetShard: p.Command.TargetShard, MappingVersion: p.Command.MappingVersion,
		AnchorVersion: p.Command.AnchorVersion, SourceStateRoot: p.SourceStateRoot, StateHash: p.StateHash}
	if !validTransferDigest(e.SourceStateRoot) {
		return fmt.Errorf("invalid source root")
	}
	a := m.integratedStateLocked().authority
	if old, ok := a.preparedTransfers[e.ProposalID][e.Account]; ok {
		if old == e {
			return m.publishPreparedTargetIfEnabledLocked(p)
		}
		return fmt.Errorf("conflicting native prepared result")
	}
	if err := a.registerPreparedTransfer(e); err != nil {
		return err
	}
	return m.publishPreparedTargetIfEnabledLocked(p)
}

// Runs inside the existing source-result poll, before target-receipt polling.
// Publication failure leaves the prepared binding intact so the next poll can
// retry the same immutable command; applied proposals are not re-authorized.
func (m *RuntimeUnknownAdmissionManager) publishPreparedTargetIfEnabledLocked(p core.SourcePreparation) error {
	path := os.Getenv("SAA_NATIVE_TARGET_INSTALL_PATH")
	if path == "" {
		return nil
	}
	if m.integratedStateLocked().authority.decisionByID[p.Command.ProposalID].Status == mappingApplied {
		return nil
	}
	return m.publishNativeTargetInstallationLocked(p, path)
}

func hashSourcePreparedState(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
