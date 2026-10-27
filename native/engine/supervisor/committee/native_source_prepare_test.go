package committee

import (
	"blockEmulator/core"
	"encoding/json"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
)

// These are constructed unit records, not native consensus certificates.
func sourceResultFixture(t *testing.T) (*RuntimeUnknownAdmissionManager, core.SourcePreparation) {
	t.Helper()
	account := authorityAccount('a')
	m := &RuntimeUnknownAdmissionManager{shardNum: 2, softDecisionMode: saaIntegratedDecisionMode, stableShardByAccount: map[string]uint64{account: 0}}
	a := m.integratedStateLocked().authority
	a.enableInstallationReceiptGate()
	if d := a.submit(mappingAuthorityProposal{ProposalID: "p", Addresses: []string{account}, TargetShard: 1, EffectiveEpoch: 2}, 1); d.Status != mappingAccepted {
		t.Fatal(d)
	}
	state := (&core.AccountState{Balance: big.NewInt(100)}).Encode()
	hash := hashSourcePreparedState(state)
	root := strings.Repeat("1", 64)
	return m, core.SourcePreparation{Command: core.SourcePrepareCommand{ProposalID: "p", Account: strings.TrimPrefix(account, "0x"), TargetShard: 1, ExpectedBlockHash: strings.Repeat("2", 64), ExpectedStateRoot: root, ExpectedStateHash: hash}, State: state, StateHash: hash, SourceStateRoot: root, PrepareBlock: 2, PrepareSequence: 2, CommitDigest: strings.Repeat("3", 64)}
}

func writeSourceResult(t *testing.T, p core.SourcePreparation) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "prepared.json")
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err = core.PublishSourcePrepareFile(path, append(data, '\n')); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReplayedSourcePreparationCannotActivateWithoutTargetReceipt(t *testing.T) {
	m, p := sourceResultFixture(t)
	s := m.integratedStateLocked()
	a := s.authority
	s.nativeSourcePreparedPath = writeSourceResult(t, p)
	for _, epoch := range []int{2, 2, 3, 10} {
		m.lock.Lock()
		m.applySAAIntegratedDueLocked(epoch)
		m.lock.Unlock()
		e, ok := a.preparedTransfers["p"]["0x"+p.Command.Account]
		if !ok || e.StateHash != p.StateHash || e.SourceStateRoot != p.SourceStateRoot {
			t.Fatal("original preparation not retained")
		}
		if a.routeVersion != 0 || a.decisionByID["p"].Status == mappingApplied || m.stableShardByAccount["0x"+p.Command.Account] != 0 {
			t.Fatal("source-only evidence activated route")
		}
		if a.canFinalizeMigration("p", "0x"+p.Command.Account, p.StateHash) {
			t.Fatal("source-only evidence authorized finalization")
		}
	}
}

func TestSourcePreparationIngressRejectsMismatchedEvidence(t *testing.T) {
	mutations := map[string]func(*core.SourcePreparation){
		"proposal":      func(p *core.SourcePreparation) { p.Command.ProposalID = "unknown" },
		"account":       func(p *core.SourcePreparation) { p.Command.Account = strings.Repeat("b", 40) },
		"source":        func(p *core.SourcePreparation) { p.Command.SourceShard = 1 },
		"target":        func(p *core.SourcePreparation) { p.Command.TargetShard = 0 },
		"version":       func(p *core.SourcePreparation) { p.Command.MappingVersion++ },
		"state_bytes":   func(p *core.SourcePreparation) { p.State = []byte("corrupt") },
		"source_root":   func(p *core.SourcePreparation) { p.SourceStateRoot = strings.Repeat("4", 64) },
		"commit_digest": func(p *core.SourcePreparation) { p.CommitDigest = "" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			m, p := sourceResultFixture(t)
			mutate(&p)
			m.lock.Lock()
			err := m.consumeNativeSourcePreparedLocked(writeSourceResult(t, p))
			m.lock.Unlock()
			if err == nil {
				t.Fatal("mismatched evidence accepted")
			}
			a := m.integratedStateLocked().authority
			if len(a.preparedTransfers["p"]) != 0 || a.routeVersion != 0 {
				t.Fatal("rejected evidence changed authority")
			}
		})
	}
}

func TestSourcePreparationReplayRejectsConflictingState(t *testing.T) {
	m, p := sourceResultFixture(t)
	m.lock.Lock()
	defer m.lock.Unlock()
	if err := m.consumeNativeSourcePreparedLocked(writeSourceResult(t, p)); err != nil {
		t.Fatal(err)
	}
	original := m.integratedStateLocked().authority.preparedTransfers["p"]["0x"+p.Command.Account]
	p.State = (&core.AccountState{Balance: big.NewInt(101)}).Encode()
	p.StateHash = hashSourcePreparedState(p.State)
	p.Command.ExpectedStateHash = p.StateHash
	if err := m.consumeNativeSourcePreparedLocked(writeSourceResult(t, p)); err == nil {
		t.Fatal("conflicting replay replaced original state")
	}
	if m.integratedStateLocked().authority.preparedTransfers["p"]["0x"+p.Command.Account] != original {
		t.Fatal("conflict mutated binding")
	}
}
