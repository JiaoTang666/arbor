package committee

import (
	"blockEmulator/core"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGroupCompletionConsumesBundleAndKeepsSourceRequirement(t *testing.T) {
	accounts := []string{strings.Repeat("0", 39) + "2", strings.Repeat("0", 39) + "4"}
	tx := core.NewTransaction(strings.Repeat("0", 39)+"9", accounts[1], big.NewInt(1), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	tx.Relayed = true
	tx.RoutedSenderShard = 1
	h, e := core.BuildGroupMigrationHandoff(tx, "bundle", accounts, 0, 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(h.Execution)
	ack := core.HandoffAck{ID: h.ID, ProposalID: h.ProposalID, TargetShard: 1, ExecutionHash: hex.EncodeToString(sum[:]), Block: 2, BlockHash: strings.Repeat("a", 64), StateRoot: strings.Repeat("b", 64), Executed: true}
	dir, acks := t.TempDir(), t.TempDir()
	t.Setenv("SAA_NATIVE_HANDOFF_OUTBOX_DIR", dir)
	t.Setenv("SAA_NATIVE_HANDOFF_ACK_DIR", acks)
	raw, _ := json.Marshal(h)
	if e = core.PublishMigrationArtifacts(dir, ".handoff.json", core.MigrationArtifacts{h.ID: raw}); e != nil {
		t.Fatal(e)
	}
	raw, _ = json.Marshal(ack)
	if e = core.PublishMigrationArtifacts(acks, ".ack.json", core.MigrationArtifacts{h.ID: raw}); e != nil {
		t.Fatal(e)
	}
	spec := nativeGroupSpec{Arm: "move"}
	for _, a := range accounts {
		spec.Command.Members = append(spec.Command.Members, core.SourcePrepareCommand{ProposalID: "bundle", Account: a, SourceShard: 0, TargetShard: 1})
	}
	m := &RelayCommitteeModule{groupControl: &nativeGroupControl{spec: spec}, nativeAudit: newBatchAudit(filepath.Join(t.TempDir(), "audit.jsonl")), lifecycle: map[string]*ordinaryLifecycle{occurrenceKey(tx): {Tx: snapshotOrdinaryRoute(tx), Routed: true}}}
	defer m.nativeAudit.flush()
	if e = m.pollGroupCompletions(); e != nil {
		t.Fatal(e)
	}
	if m.lifecycle[occurrenceKey(tx)].Done {
		t.Fatal("completion bypassed old source leg")
	}
	m.lifecycle[occurrenceKey(tx)].Source = true
	m.groupControl.lastCompletionScan = time.Time{}
	if e = m.pollGroupCompletions(); e != nil {
		t.Fatal(e)
	}
	if !m.lifecycle[occurrenceKey(tx)].Done || !m.groupControl.completedHandoffs[h.ID] {
		t.Fatal("bundle completion lost")
	}
	m.groupControl.lastCompletionScan = time.Time{}
	if e = m.pollGroupCompletions(); e != nil {
		t.Fatal("duplicate failed", e)
	}
}
