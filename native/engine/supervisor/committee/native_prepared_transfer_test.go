package committee

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Unit evidence only: these test hashes are not native consensus receipts.
func prepareReceiptForTest(t *testing.T, a *mappingAuthority, id, account string, target uint64) nativeInstallationReceiptMeta {
	t.Helper()
	e := NativeStateTransferEnvelope{ProposalID: id, Account: account, TargetShard: target,
		SourceStateRoot: strings.Repeat("1", 64), StateHash: strings.Repeat("2", 64)}
	if err := a.registerPreparedTransfer(e); err != nil {
		t.Fatal(err)
	}
	return nativeInstallationReceiptMeta{ProposalID: id, Account: account, TargetShard: target,
		StateHash: e.StateHash, SourceStateRoot: e.SourceStateRoot,
		TargetStateRoot: strings.Repeat("3", 64), CommitDigest: strings.Repeat("4", 64),
		InstallBlock: 1, InstallRound: 1, Installed: true, Committed: true}
}

func TestPreparedReceiptRejectsEachMismatchedField(t *testing.T) {
	changes := map[string]func(*nativeInstallationReceiptMeta){
		"proposal":      func(r *nativeInstallationReceiptMeta) { r.ProposalID = "other" },
		"account":       func(r *nativeInstallationReceiptMeta) { r.Account = authorityAccount('b') },
		"source":        func(r *nativeInstallationReceiptMeta) { r.SourceShard = 1 },
		"target":        func(r *nativeInstallationReceiptMeta) { r.TargetShard = 0 },
		"mapping":       func(r *nativeInstallationReceiptMeta) { r.MappingVersion++ },
		"anchor":        func(r *nativeInstallationReceiptMeta) { r.AnchorVersion++ },
		"state":         func(r *nativeInstallationReceiptMeta) { r.StateHash = strings.Repeat("5", 64) },
		"source_root":   func(r *nativeInstallationReceiptMeta) { r.SourceStateRoot = strings.Repeat("5", 64) },
		"target_root":   func(r *nativeInstallationReceiptMeta) { r.TargetStateRoot = "" },
		"commit_digest": func(r *nativeInstallationReceiptMeta) { r.CommitDigest = "" },
		"block":         func(r *nativeInstallationReceiptMeta) { r.InstallBlock = 0 },
		"installed":     func(r *nativeInstallationReceiptMeta) { r.Installed = false },
		"committed":     func(r *nativeInstallationReceiptMeta) { r.Committed = false },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			account := authorityAccount('a')
			a := newMappingAuthority(2, map[string]uint64{account: 0})
			a.enableInstallationReceiptGate()
			a.submit(mappingAuthorityProposal{ProposalID: "p", Addresses: []string{account}, TargetShard: 1, EffectiveEpoch: 2}, 1)
			good := prepareReceiptForTest(t, a, "p", account, 1)
			bad := good
			change(&bad)
			a.recordInstallationReceiptMeta(bad)
			if a.applyDue(2)[0].Status == mappingApplied {
				t.Fatal("bad evidence activated route")
			}
			a.recordInstallationReceiptMeta(good)
			if a.canFinalizeMigration("p", account, good.StateHash) {
				t.Fatal("finalized before activation")
			}
			if a.applyDue(2)[0].Status != mappingApplied {
				t.Fatal("valid evidence did not activate")
			}
			if !a.canFinalizeMigration("p", account, good.StateHash) {
				t.Fatal("finalization rejected after activation")
			}
		})
	}
}

func TestPreparedTransferCannotUseUnknownOrConflictingProposal(t *testing.T) {
	a := newMappingAuthority(2, nil)
	if err := a.registerPreparedTransfer(NativeStateTransferEnvelope{ProposalID: "unknown"}); err == nil {
		t.Fatal("unknown accepted")
	}
	account := authorityAccount('a')
	a.submit(mappingAuthorityProposal{ProposalID: "p", Addresses: []string{account}, TargetShard: 1, EffectiveEpoch: 2}, 1)
	prepareReceiptForTest(t, a, "p", account, 1)
	e := a.preparedTransfers["p"][account]
	e.StateHash = strings.Repeat("5", 64)
	if err := a.registerPreparedTransfer(e); err == nil {
		t.Fatal("conflicting source state accepted")
	}
}

func TestReceiptPollingRejectsThenConsumesWithoutOffsetDrift(t *testing.T) {
	m := &RuntimeUnknownAdmissionManager{shardNum: 2, stableShardByAccount: map[string]uint64{}}
	a := m.integratedStateLocked().authority
	a.enableInstallationReceiptGate()
	account := authorityAccount('a')
	a.submit(mappingAuthorityProposal{ProposalID: "p", Addresses: []string{account}, TargetShard: 1, EffectiveEpoch: 2}, 1)
	good := prepareReceiptForTest(t, a, "p", account, 1)
	event := nativeInstallationReceiptEvent{ProposalID: "p", Account: account, TargetShard: 1,
		StateHash: good.StateHash, SourceStateRoot: good.SourceStateRoot, TargetStateRoot: good.TargetStateRoot,
		CommitDigest: good.CommitDigest, InstallBlock: 1, InstallRound: 1, Installed: true, Committed: true}
	line, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	data := append([]byte("{\"proposal_id\":\"rejected\",\"committed\":false}\n"), append(line, '\n')...)
	path := filepath.Join(t.TempDir(), "receipts.jsonl")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	var offset int64
	prepared := a.preparedTransfers["p"][account]
	delete(a.preparedTransfers["p"], account)
	if n, err := m.ConsumeNativeInstallationReceipts(path, &offset); err != nil || n != 0 {
		t.Fatalf("premature consume %d %v", n, err)
	}
	if offset != int64(len(data)-len(line)-1) {
		t.Fatal("receipt lost before preparation arrived")
	}
	if err := a.registerPreparedTransfer(prepared); err != nil {
		t.Fatal(err)
	}
	if n, err := m.ConsumeNativeInstallationReceipts(path, &offset); err != nil || n != 1 {
		t.Fatalf("%d %v", n, err)
	}
	if offset != int64(len(data)) {
		t.Fatalf("offset %d != %d", offset, len(data))
	}
	if n, err := m.ConsumeNativeInstallationReceipts(path, &offset); err != nil || n != 0 {
		t.Fatalf("replayed %d %v", n, err)
	}
	if a.applyDue(2)[0].Status != mappingApplied {
		t.Fatal("not applied")
	}
}

func TestRuntimeReceiptPollUnderManagerLockDoesNotDeadlock(t *testing.T) {
	m := &RuntimeUnknownAdmissionManager{shardNum: 2, stableShardByAccount: map[string]uint64{}}
	m.lock.Lock()
	defer m.lock.Unlock()
	var offset int64
	if _, err := m.consumeNativeInstallationReceiptsLocked(filepath.Join(t.TempDir(), "missing"), &offset); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimePollConsumesReceiptBeforeActivatingRoute(t *testing.T) {
	m := &RuntimeUnknownAdmissionManager{shardNum: 2, softDecisionMode: saaIntegratedDecisionMode, stableShardByAccount: map[string]uint64{}}
	s := m.integratedStateLocked()
	s.authority.enableInstallationReceiptGate()
	account := authorityAccount('a')
	s.authority.submit(mappingAuthorityProposal{ProposalID: "p", Addresses: []string{account}, TargetShard: 1, EffectiveEpoch: 2}, 1)
	r := prepareReceiptForTest(t, s.authority, "p", account, 1)
	event := nativeInstallationReceiptEvent{ProposalID: "p", Account: account, TargetShard: 1, StateHash: r.StateHash,
		SourceStateRoot: r.SourceStateRoot, TargetStateRoot: r.TargetStateRoot, CommitDigest: r.CommitDigest,
		InstallBlock: 1, InstallRound: 1, Installed: true, Committed: true}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	s.nativeReceiptPath = filepath.Join(t.TempDir(), "receipts.jsonl")
	m.lock.Lock()
	m.applySAAIntegratedDueLocked(2)
	m.lock.Unlock()
	if s.authority.routeVersion != 0 {
		t.Fatal("activated without receipt")
	}
	if err = os.WriteFile(s.nativeReceiptPath, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	m.lock.Lock()
	m.applySAAIntegratedDueLocked(2)
	m.lock.Unlock()
	if s.authority.routeVersion != 1 || s.authority.decisionByID["p"].Status != mappingApplied {
		t.Fatal("runtime poll did not activate")
	}
}
