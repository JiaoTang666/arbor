package committee

import (
	"blockEmulator/core"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNativeGroupCompletionJoinsOriginalAndWaitsForSource(t *testing.T) {
	a, b := strings.Repeat("0", 39)+"2", strings.Repeat("0", 39)+"4"
	tx := core.NewTransaction("0000000000000000000000000000000000000009", b, big.NewInt(1), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	tx.Relayed = true
	tx.RoutedSenderShard = 1
	h, e := core.BuildGroupMigrationHandoff(tx, "g", []string{a, b}, 0, 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	hash := sha256.Sum256(h.Execution)
	ack := core.HandoffAck{ID: h.ID, ProposalID: "g", TargetShard: 1, ExecutionHash: hex.EncodeToString(hash[:]), Block: 2, BlockHash: strings.Repeat("a", 64), StateRoot: strings.Repeat("b", 64), Executed: true}
	m := &RelayCommitteeModule{nativeAudit: newBatchAudit(filepath.Join(t.TempDir(), "audit.jsonl")), lifecycle: map[string]*ordinaryLifecycle{occurrenceKey(tx): {Tx: snapshotOrdinaryRoute(tx), Routed: true}}}
	done, e := m.registerGroupCompletion(h, ack, nil)
	if e != nil || done || m.lifecycle[occurrenceKey(tx)].Done {
		t.Fatal("credit completed before source evidence", e)
	}
	m.lifecycle[occurrenceKey(tx)].Source = true
	done, e = m.registerGroupCompletion(h, ack, nil)
	if e != nil || !done || !m.lifecycle[occurrenceKey(tx)].Done {
		t.Fatal("valid completion not joined", e)
	}
	if done, e = m.registerGroupCompletion(h, ack, nil); e != nil || !done {
		t.Fatal("duplicate completion not idempotent", e)
	}
	m.nativeAudit.flush()
}

func TestNativeGroupCompletionRejectsDifferentOriginalRoute(t *testing.T) {
	a, b := strings.Repeat("0", 39)+"2", strings.Repeat("0", 39)+"4"
	tx := core.NewTransaction(a, b, big.NewInt(1), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	h, e := core.BuildGroupMigrationHandoff(tx, "g", []string{a, b}, 0, 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	hash := sha256.Sum256(h.Execution)
	ack := core.HandoffAck{ID: h.ID, ProposalID: "g", TargetShard: 1, ExecutionHash: hex.EncodeToString(hash[:]), Block: 2, BlockHash: strings.Repeat("a", 64), StateRoot: strings.Repeat("b", 64), Executed: true}
	route := snapshotOrdinaryRoute(tx)
	route.MappingVersion = 99
	m := &RelayCommitteeModule{lifecycle: map[string]*ordinaryLifecycle{occurrenceKey(tx): {Tx: route, Routed: true}}}
	if done, e := m.registerGroupCompletion(h, ack, nil); e == nil || done {
		t.Fatal("mismatched original route accepted")
	}
}
