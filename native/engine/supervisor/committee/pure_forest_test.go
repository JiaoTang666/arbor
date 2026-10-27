package committee

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/utils"
	"bufio"
	"bytes"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

func TestPureForestHashNoPolicy(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 16
	defer func() { params.ShardNum = old }()
	m, log := forestHookFixtureV2(t, true)
	m.forestSource = m.newNewPolicy.source
	m.newNewPolicy = nil
	m.newNewWork = nil
	m.nativeFixedRoutes = true
	// A fresh native-hash run never enables the policy's historical sampler.
	// The old fixture retained historyOn=true after removing its policy.
	m.placementSignals = newPlacementSignals(16)
	pairs := [][2]int{{1, 2}, {4, 1}, {9, 10}, {11, 11}, {9, 10}, {12, 9}, {9, 12}}
	stages := 0
	for i, p := range pairs {
		tx := forestHookTransactionV2(t, m, uint64(i), p[0], p[1])
		state, err := m.forestFacts.Audit()
		if err != nil || state.Bound != uint64(i+1) || state.Assigned != uint64(i+1) {
			t.Fatalf("unrecorded forest work with policy history disabled: %+v, %v", state, err)
		}
		for j, a := range []string{tx.Sender, tx.Recipient} {
			want, ok := m.initialLedger.Routes[a]
			if !ok {
				want = uint64(utils.Addr2Shard(a))
			}
			got := tx.RoutedSenderShard
			if j == 1 {
				got = tx.RoutedRecipientShard
			}
			if got != want {
				t.Fatal("non-native placement", i, j, got, want)
			}
		}
		if tx.RoutedSenderShard == tx.RoutedRecipientShard {
			m.auditBlock(forestHookBlockV2(tx, "inner", tx.RoutedSenderShard))
			stages++
		} else {
			m.auditBlock(forestHookBlockV2(tx, "relay2", tx.RoutedRecipientShard))
			m.auditBlock(forestHookBlockV2(tx, "relay1", tx.RoutedSenderShard))
			stages += 2
		}
		m.auditBlock(forestHookBlockV2(tx, "relay1", tx.RoutedSenderShard))
	}
	a, e := m.forestFacts.Audit()
	if e != nil || a.Completed != uint64(len(pairs)) {
		t.Fatal(a, e)
	}
	n := int64(0)
	for _, s := range a.ConfirmedStages {
		n += s
	}
	if n != int64(stages) {
		t.Fatal("work conservation")
	}
	x, _ := m.forestFacts.Source(forestHookAddrV2(9))
	y, _ := m.forestFacts.Source(forestHookAddrV2(10))
	if x.PlanningOrigin != y.PlanningOrigin || x.Shard == y.Shard {
		t.Fatal("split source must remain one origin")
	}
	m.nativeAudit.flush()
	if bytes.Contains(log.Bytes(), []byte("share_placement")) || bytes.Contains(log.Bytes(), []byte("share_batch_decision")) {
		t.Fatal("unexpected strategy")
	}
	if m.placementSignals.historyOn || len(m.placementSignals.history) != 0 {
		t.Fatal("facts must not enable policy history to work")
	}
	if _, err := m.forestFacts.CompleteAudit(uint64(len(pairs))); err != nil {
		t.Fatal(err)
	}
	if _, err := m.forestFacts.CompleteAudit(uint64(len(pairs) + 1)); err == nil {
		t.Fatal("wrong closeout boundary accepted")
	}
	queries := 0
	for _, line := range bytes.Split(bytes.TrimSpace(log.Bytes()), []byte("\n")) {
		var row struct {
			Event string                `json:"event"`
			Seq   uint64                `json:"sequence"`
			Fast  ForestFactsSnapshotV2 `json:"fast"`
		}
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatal(err)
		}
		if row.Event == "forest_facts_v2_query" {
			queries++
			if row.Fast.BoundPrefix != row.Seq || row.Fast.AssignedPrefix != row.Seq {
				t.Fatal("query missing earlier work", row)
			}
		}
	}
	if queries == 0 {
		t.Fatal("missing native-hash newnew queries")
	}
}

func TestPureClockWithoutHistory(t *testing.T) {
	p := newPlacementSignals(4)
	a := p.assign(0, 1)
	c := p.commit(0)
	d := p.commit(1)
	if a < 0 || c < a || d < c || p.historyOn || len(p.history) != 0 {
		t.Fatal("work clock depends on history", a, c, d)
	}
	rows := p.snapshot()
	if rows[0].Assigned != 1 || rows[1].Assigned != 1 || rows[0].Confirmed != 1 || rows[1].Confirmed != 1 {
		t.Fatal(rows)
	}
	p.enableHistory()
	a = p.assign(2, 2)
	if len(p.history) == 0 || p.history[len(p.history)-1].MonoNS != a {
		t.Fatal("history clock differs from work clock")
	}
}

func TestPureCompleteAuditRejectsUnassignedBindings(t *testing.T) {
	f, err := NewForestFactsV2(map[string]string{}, "fixture", 0, "clock", 4)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Bind(0, "a", "b", false, false, 0, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Audit(); err != nil {
		t.Fatal("partial prefix should conserve", err)
	}
	if _, err = f.CompleteAudit(1); err == nil {
		t.Fatal("bound-only forest reported complete")
	}
}

type pausedAuditSink struct {
	entered, release chan struct{}
	out              bytes.Buffer
}

func (w *pausedAuditSink) Write(p []byte) (int, error) {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	<-w.release
	return w.out.Write(p)
}
func TestPureAuditSlowSinkDoesNotBlockAcceptance(t *testing.T) {
	sink := &pausedAuditSink{entered: make(chan struct{}, 1), release: make(chan struct{})}
	a := &batchAudit{w: bufio.NewWriterSize(sink, 16)}
	a.startWriter()
	m := &RelayCommitteeModule{offeredAuditPath: "fixture", nativeAudit: a}
	m.auditAccepted(&core.Transaction{IngressSequence: 0, TxHash: bytes.Repeat([]byte{1}, 32)})
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("sink not exercised")
	}
	done := make(chan struct{})
	go func() {
		m.auditAccepted(&core.Transaction{IngressSequence: 1, TxHash: bytes.Repeat([]byte{2}, 32)})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		close(sink.release)
		t.Fatal("disk held lifecycle acceptance")
	}
	close(sink.release)
	a.flush()
	if bytes.Count(sink.out.Bytes(), []byte(`"event":"accepted"`)) != 2 || atomic.LoadInt64(&a.async.queued) != 0 {
		t.Fatal("lost audit record")
	}
}
func TestPureAuditQueueOverflowFails(t *testing.T) {
	a := &batchAudit{async: &auditWriter{records: make(chan auditRecord, 1)}}
	atomic.StoreInt64(&a.async.queued, auditQueueBytes)
	defer func() {
		if recover() == nil {
			t.Fatal("overflow silently allowed")
		}
	}()
	a.enqueueAudit(auditRecord{data: []byte{1}})
}
