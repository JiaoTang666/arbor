package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"testing"
	"time"
)

func forestHookAddrV2(n int) string { return fmt.Sprintf("%040x", n) }

// The cache is already validated/publication-frozen in this fixture. Bootstrap,
// file integrity and background generation replacement have separate tests.
func forestHookFixtureV2(t *testing.T, enabled bool) (*RelayCommitteeModule, *bytes.Buffer) {
	t.Helper()
	m, log := shareFixture(t)
	m.offeredAuditPath = "in-memory-lifecycle"
	m.initialLedger.Routes[forestHookAddrV2(5)] = 7
	m.newNewWork.population[7]++
	if enabled {
		seed := map[string]string{forestHookAddrV2(1): "history:-2", forestHookAddrV2(5): "history:-3"}
		f, e := NewForestFactsV2(seed, "history", 2, m.placementSignals.clock.id, 16)
		if e != nil {
			t.Fatal(e)
		}
		m.forestFacts = f
		m.forestJoint = &forestJointCacheV2{
			meta:   forestSlowMetaV2{Schema: "forest_slow_facts_v2", Semantics: "source_activity_facts_v2.0", Version: 0, Cutoff: 2, SourceTime: 1000, Namespace: "history", SeedCutoff: 2, Shards: 16},
			joint:  forestJointFileV2{Schema: "graph_forest_joint_v2", Graph: json.RawMessage(`{}`), Forest: json.RawMessage(`{"schema":"forest_slow_facts_v2"}`)},
			digest: "test-only-immutable-validated-cache",
		}
	}
	return m, log
}

func forestHookTransactionV2(t *testing.T, m *RelayCommitteeModule, seq uint64, a, b int) *core.Transaction {
	t.Helper()
	// Equal payloads remain equal hashes; ingress sequence distinguishes input
	// occurrences, including deliberate duplicate records below.
	tx := core.NewTransaction(forestHookAddrV2(a), forestHookAddrV2(b), big.NewInt(1), 0, time.Unix(1000, 0))
	tx.IngressSequence, tx.IngressSequenceAssigned = seq, true
	m.placementSequence = seq
	m.auditAccepted(tx)
	sa, sb := m.nativePlacement(tx.Sender, tx.Recipient)
	tx.RoutingFrozen, tx.RoutingEpoch = true, 1
	tx.RoutedSenderShard, tx.RoutedRecipientShard = sa, sb
	m.auditRoute(tx)
	return tx
}

func forestHookBlockV2(tx *core.Transaction, leg string, shard uint64) *message.BlockInfoMsg {
	b := &message.BlockInfoMsg{Epoch: 1, SenderShardID: shard, CommitTime: time.Unix(1100, 0)}
	switch leg {
	case "inner":
		b.InnerShardTxs = []*core.Transaction{tx}
	case "relay1":
		b.Relay1Txs = []*core.Transaction{tx}
	case "relay2":
		b.Relay2Txs = []*core.Transaction{tx}
	default:
		panic("invalid test leg")
	}
	return b
}

func forestHookPolicyEvidenceV2(t *testing.T, m *RelayCommitteeModule, log *bytes.Buffer) []map[string]interface{} {
	t.Helper()
	m.nativeAudit.flush()
	var out []map[string]interface{}
	for _, line := range bytes.Split(bytes.TrimSpace(log.Bytes()), []byte("\n")) {
		var row map[string]interface{}
		if e := json.Unmarshal(line, &row); e != nil {
			t.Fatal(e)
		}
		switch row["event"] {
		case "share_batch_decision":
			out = append(out, map[string]interface{}{"event": row["event"], "batch": row["batch"], "probabilities": row["probabilities"], "base_logits": row["base_logits"]})
		case "share_placement":
			out = append(out, map[string]interface{}{"event": row["event"], "sequence": row["sequence"], "ordinal": row["nn_ordinal"], "u": row["u"], "action": row["action"]})
		}
	}
	return out
}

func TestForestNativeHooksV2PolicyIsObservational(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 16
	defer func() { params.ShardNum = old }()
	plain, plainLog := forestHookFixtureV2(t, false)
	with, withLog := forestHookFixtureV2(t, true)
	pairs := [][2]int{{1, 2}, {4, 1}, {9, 10}, {11, 11}, {9, 10}, {1, 5}, {1, 2}, {9, 1}, {12, 9}, {9, 12}, {13, 14}}
	var txs []*core.Transaction
	for seq, p := range pairs {
		a := forestHookTransactionV2(t, plain, uint64(seq), p[0], p[1])
		b := forestHookTransactionV2(t, with, uint64(seq), p[0], p[1])
		if a.RoutedSenderShard != b.RoutedSenderShard || a.RoutedRecipientShard != b.RoutedRecipientShard {
			t.Fatalf("facts changed route at %d", seq)
		}
		if !reflect.DeepEqual(plain.newNewPolicy.share.prob, with.newNewPolicy.share.prob) {
			t.Fatalf("facts changed probability at %d", seq)
		}
		if !reflect.DeepEqual(plain.newNewWork.work, with.newNewWork.work) || !reflect.DeepEqual(plain.newNewWork.population, with.newNewWork.population) {
			t.Fatalf("facts changed assigned work/population at %d", seq)
		}
		txs = append(txs, b)
	}
	if !reflect.DeepEqual(forestHookPolicyEvidenceV2(t, plain, plainLog), forestHookPolicyEvidenceV2(t, with, withLog)) {
		t.Fatal("facts altered counter-random placement evidence")
	}
	if !reflect.DeepEqual(plain.nativeRoutes, with.nativeRoutes) {
		t.Fatal("bound map differs")
	}
	if txs[0].RoutedSenderShard != 3 || txs[0].RoutedRecipientShard != 3 || txs[1].RoutedSenderShard != 3 || txs[1].RoutedRecipientShard != 3 {
		t.Fatal("new-old directions did not follow")
	}
	if !bytes.Equal(txs[0].TxHash, txs[6].TxHash) {
		t.Fatal("duplicate input fixture does not have equal payload/hash")
	}
	a, e := with.forestFacts.Audit()
	if e != nil || a.Assigned != uint64(len(pairs)) || a.OutstandingOccurrences != len(pairs) {
		t.Fatal(a, e)
	}
	v2, _ := with.forestFacts.Source(forestHookAddrV2(2))
	v4, _ := with.forestFacts.Source(forestHookAddrV2(4))
	if v2.Parent != forestHookAddrV2(1) || v4.Parent != forestHookAddrV2(1) || v2.CommittedCreated || v4.CommittedCreated {
		t.Fatal("source/binding/creation distinctions lost")
	}
	v9, _ := with.forestFacts.Source(forestHookAddrV2(9))
	v12, _ := with.forestFacts.Source(forestHookAddrV2(12))
	if v12.Parent != forestHookAddrV2(9) || v9.PlanningOrigin != v12.PlanningOrigin || v9.Parent != "" {
		t.Fatal("cross-family work changed origins")
	}
	with.nativeAudit.flush()
	queries := 0
	for _, line := range bytes.Split(bytes.TrimSpace(withLog.Bytes()), []byte("\n")) {
		var row struct {
			Event      string                `json:"event"`
			Sequence   uint64                `json:"sequence"`
			Fast       ForestFactsSnapshotV2 `json:"fast"`
			Influences bool                  `json:"influences_placement"`
		}
		if e := json.Unmarshal(line, &row); e != nil {
			t.Fatal(e)
		}
		if row.Event == "forest_facts_v2_query" {
			queries++
			if row.Influences || row.Fast.BoundPrefix != row.Sequence || row.Fast.AssignedPrefix != row.Sequence {
				t.Fatal("query included current or later input")
			}
		}
	}
	if queries != 3 {
		t.Fatalf("wanted exactly three new-new/self predecision queries, got %d", queries)
	}
}

func TestForestNativeHooksV2ValidatedLifecycleAndDedup(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 16
	defer func() { params.ShardNum = old }()
	m, _ := forestHookFixtureV2(t, true)
	cross := forestHookTransactionV2(t, m, 0, 1, 5)
	self := forestHookTransactionV2(t, m, 1, 9, 9)
	follow := forestHookTransactionV2(t, m, 2, 10, 1)
	repeat := forestHookTransactionV2(t, m, 3, 10, 1)
	if cross.RoutedSenderShard == cross.RoutedRecipientShard {
		t.Fatal("cross fixture is not cross-shard")
	}
	before, _ := m.forestFacts.Audit()
	bad := *cross
	bad.Recipient = forestHookAddrV2(999)
	m.auditBlock(forestHookBlockV2(&bad, "relay1", cross.RoutedSenderShard))
	m.auditBlock(forestHookBlockV2(cross, "relay1", (cross.RoutedSenderShard+1)%16))
	after, _ := m.forestFacts.Audit()
	if !reflect.DeepEqual(before.ConfirmedStages, after.ConfirmedStages) {
		t.Fatal("rejected native evidence changed forest")
	}
	// Receipt arrival may be reverse order; repeats from replicas do not count.
	b2 := forestHookBlockV2(cross, "relay2", cross.RoutedRecipientShard)
	m.auditBlock(b2)
	m.auditBlock(b2)
	partial, e := m.forestFacts.Audit()
	if e != nil || partial.Completed != 0 || partial.ConfirmedStages[cross.RoutedRecipientShard] != 1 {
		t.Fatal(partial, e)
	}
	b1 := forestHookBlockV2(cross, "relay1", cross.RoutedSenderShard)
	m.auditBlock(b1)
	m.auditBlock(b1)
	m.auditBlock(b2)
	for _, tx := range []*core.Transaction{self, follow, repeat} {
		if tx.RoutedSenderShard != tx.RoutedRecipientShard {
			t.Fatal("association first input was cross-shard")
		}
		b := forestHookBlockV2(tx, "inner", tx.RoutedSenderShard)
		m.auditBlock(b)
		m.auditBlock(b)
	}
	a, e := m.forestFacts.Audit()
	if e != nil || a.Completed != 4 || a.OutstandingOccurrences != 0 || !reflect.DeepEqual(a.AssignedStages, a.ConfirmedStages) {
		t.Fatal(a, e)
	}
	if len(m.forestFacts.pending) != 0 {
		t.Fatal("finished receipt evidence was retained")
	}
	for _, account := range []int{9, 10} {
		v, ok := m.forestFacts.Source(forestHookAddrV2(account))
		if !ok || !v.CommittedCreated {
			t.Fatal("verified creation was not distinguished from binding", account, v)
		}
	}
	s, e := m.forestFacts.SnapshotNow(m.placementSignals.clock)
	if e != nil || s.ConfirmedReceiptMonoNS < 0 || s.ConfirmedReceiptMonoNS > s.RuntimeMonoNS {
		t.Fatal("confirmation time boundary", s, e)
	}
}
