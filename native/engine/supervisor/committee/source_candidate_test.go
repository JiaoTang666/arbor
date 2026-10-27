package committee

import (
	"blockEmulator/core"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourceCandidateRequiresAcceptedProposalAndReplays(t *testing.T) {
	dir := t.TempDir()
	candidate := filepath.Join(dir, "candidate.json")
	inbox := filepath.Join(dir, "prepare.json")
	t.Setenv("SAA_NATIVE_SOURCE_CANDIDATE_PATH", candidate)
	t.Setenv("SAA_NATIVE_SOURCE_PREPARE_PATH", inbox)
	account := "0000000000000000000000000000000000000010"
	m := &RuntimeUnknownAdmissionManager{shardNum: 2, softDecisionMode: saaIntegratedDecisionMode, stableShardByAccount: map[string]uint64{"0x" + account: 0}}
	a := m.integratedStateLocked().authority
	a.enableInstallationReceiptGate()
	c := core.SourcePrepareCommand{ProposalID: "candidate", Account: account, TargetShard: 1, ExpectedBlockHash: strings.Repeat("1", 64), ExpectedStateRoot: strings.Repeat("2", 64), ExpectedStateHash: strings.Repeat("3", 64)}
	raw, _ := json.Marshal(c)
	if err := core.PublishSourcePrepareFile(candidate, raw); err != nil {
		t.Fatal(err)
	}
	if err := m.publishSourceCandidateIfEnabledLocked(); err == nil {
		t.Fatal("unaccepted proposal published")
	}
	if _, err := os.Stat(inbox); !os.IsNotExist(err) {
		t.Fatal("rejected candidate created native inbox")
	}
	if d := a.submit(mappingAuthorityProposal{ProposalID: c.ProposalID, Addresses: []string{"0x" + account}, TargetShard: 1, EffectiveEpoch: 2}, 1); d.Status != mappingAccepted {
		t.Fatal(d)
	}
	// Exercise production polling, not only the helper API.
	m.applySAAIntegratedDueLocked(1)
	first, err := os.ReadFile(inbox)
	if err != nil {
		t.Fatal(err)
	}
	m.applySAAIntegratedDueLocked(1)
	second, err := os.ReadFile(inbox)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("replay changed native command")
	}
	var got core.SourcePrepareCommand
	if json.Unmarshal(second, &got) != nil || got != c || a.routeVersion != 0 {
		t.Fatal("publication changed command or activated mapping")
	}
}
