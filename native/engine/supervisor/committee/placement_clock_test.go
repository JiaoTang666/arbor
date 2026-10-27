package committee

import (
	"blockEmulator/message"
	"blockEmulator/supervisor/supervisor_log"
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"reflect"
	"testing"
	"time"
)

func clockTestRows() []placementSignal {
	queue, mono := 10, int64(0)
	return []placementSignal{{Queue: &queue, ClockID: "fixture-clock", ReceivedMonoNS: &mono}, {}}
}

func TestPlacementClockProcessOriginAndIdentity(t *testing.T) {
	a, b := newPlacementSignals(2), newPlacementSignals(2)
	if a.clock != b.clock || a.clock != processPlacementClock {
		t.Fatal("placement objects do not share the process origin")
	}
	x, y := a.clock.now(), b.clock.now()
	if len(x.ClockID) != 32 || x.ClockID != y.ClockID || x.MonoNS < 0 || y.MonoNS < x.MonoNS {
		t.Fatalf("invalid shared clock: %+v %+v", x, y)
	}
	if newPlacementClock().id == x.ClockID {
		t.Fatal("independent clock instance reused identity")
	}
}

func TestPlacementClockWallJumpsDoNotChangeFeatures(t *testing.T) {
	p := entryFixture().newNewPolicy.prior
	wall := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	decision := placementClockReading{Wall: wall.Add(5 * time.Second), ClockID: "fixture-clock", MonoNS: 5e9}
	rows := clockTestRows()
	rows[0].ReceivedAt = wall
	want, err := newNewFeatures(rows, p, []int64{1, 0}, 2, 1000, decision)
	if err != nil || want[0][4] != 1 || want[0][5] != 0 || want[1][5] != 1 {
		t.Fatalf("zero offset/missing queue rejected: %v %v", want, err)
	}
	for _, jump := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
		t.Run(jump.String(), func(t *testing.T) {
			changed := decision
			changed.Wall = decision.Wall.Add(jump)
			got, err := newNewFeatures(rows, p, []int64{1, 0}, 2, 1000, changed)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("wall jump changed features: %v %v", got, err)
			}
		})
	}
	rows[0].ReceivedAt = time.Time{}
	decision.Wall = time.Time{}
	got, err := newNewFeatures(rows, p, []int64{1, 0}, 2, 1000, decision)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("display-only clocks affected duration: %v %v", got, err)
	}
}

func TestPlacementClockRejectsIncompleteOrForeignSamples(t *testing.T) {
	p := entryFixture().newNewPolicy.prior
	for _, tc := range []struct {
		name   string
		mutate func([]placementSignal, *placementClockReading)
	}{
		{"missing decision ID", func(_ []placementSignal, d *placementClockReading) { d.ClockID = "" }},
		{"negative decision", func(_ []placementSignal, d *placementClockReading) { d.MonoNS = -1 }},
		{"missing sample ID", func(r []placementSignal, _ *placementClockReading) { r[0].ClockID = "" }},
		{"foreign sample ID", func(r []placementSignal, _ *placementClockReading) { r[0].ClockID = "other-clock" }},
		{"missing sample offset", func(r []placementSignal, _ *placementClockReading) { r[0].ReceivedMonoNS = nil }},
		{"negative sample offset", func(r []placementSignal, _ *placementClockReading) { *r[0].ReceivedMonoNS = -1 }},
		{"future sample offset", func(r []placementSignal, d *placementClockReading) { *r[0].ReceivedMonoNS = d.MonoNS + 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := clockTestRows()
			// A valid wall timestamp must never supply a legacy fallback.
			rows[0].ReceivedAt = time.Now().Add(-time.Second)
			decision := placementClockReading{Wall: time.Now(), ClockID: "fixture-clock", MonoNS: 5e9}
			tc.mutate(rows, &decision)
			if _, err := newNewFeatures(rows, p, []int64{1, 0}, 2, 1000, decision); err == nil {
				t.Fatal("invalid monotonic evidence accepted")
			}
		})
	}
}

func TestPlacementClockIntegerJSONAndZeroOffset(t *testing.T) {
	for _, mono := range []int64{0, 1<<53 + 123} {
		rows := clockTestRows()
		*rows[0].ReceivedMonoNS = mono
		data, err := json.Marshal(rows)
		if err != nil {
			t.Fatal(err)
		}
		var decoded []placementSignal
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded[0].ReceivedMonoNS == nil || *decoded[0].ReceivedMonoNS != mono || decoded[1].ReceivedMonoNS != nil {
			t.Fatalf("integer offset or missing value lost: %s", data)
		}
		decision := placementClockReading{ClockID: "fixture-clock", MonoNS: mono + 5e9}
		got, err := newNewFeatures(decoded, entryFixture().newNewPolicy.prior, []int64{1, 0}, 2, 1000, decision)
		if err != nil || got[0][4] != 1 {
			t.Fatalf("serialized age changed: %v %v", got, err)
		}
	}
}

func TestPlacementClockObserveRejectsForeignOrMissingIdentity(t *testing.T) {
	p := newPlacementSignals(2)
	queue, wall := 10, time.Now()
	b := &message.BlockInfoMsg{SenderShardID: 0, Epoch: 1, CommittedBlockHash: "a", CommitTime: wall,
		TxPoolSize: &queue, TxPoolObservedAt: wall}
	for _, id := range []string{"", "foreign"} {
		if p.observe(b, placementClockReading{Wall: wall, ClockID: id, MonoNS: 0}) {
			t.Fatal("foreign receipt clock accepted")
		}
	}
	if p.observe(b, placementClockReading{Wall: wall, ClockID: p.clock.id, MonoNS: -1}) {
		t.Fatal("negative receipt offset accepted")
	}
	if !p.observe(b, placementClockReading{Wall: wall, ClockID: p.clock.id, MonoNS: 0}) {
		t.Fatal("zero offset rejected")
	}
	row := p.snapshot()[0]
	if row.ClockID != p.clock.id || row.ReceivedMonoNS == nil || *row.ReceivedMonoNS != 0 {
		t.Fatal("receipt provenance missing from signal")
	}
}

func TestPlacementClockAuditRoundTripUsesOneClock(t *testing.T) {
	m := entryFixture()
	var audit bytes.Buffer
	m.nativeAudit = &batchAudit{w: bufio.NewWriter(&audit)}
	m.shardEpoch = map[uint64]int64{}
	m.sl = &supervisor_log.SupervisorLog{Slog: log.New(io.Discard, "", 0)}
	queue, wall := 10, time.Now()
	m.HandleBlockInfo(&message.BlockInfoMsg{SenderShardID: 0, Epoch: 1, CommittedBlockHash: "a", CommitTime: wall,
		TxPoolSize: &queue, TxPoolObservedAt: wall})
	m.chooseNewNewPolicy(0, entryContext{PartnerShard: -1})
	m.nativeAudit.flush()
	decoder := json.NewDecoder(&audit)
	var sample struct {
		Event          string
		ClockSchema    string `json:"clock_schema"`
		ClockID        string `json:"clock_id"`
		ReceivedMonoNS *int64 `json:"received_mono_ns"`
	}
	if err := decoder.Decode(&sample); err != nil {
		t.Fatal(err)
	}
	var decision struct {
		Event          string
		ClockSchema    string            `json:"clock_schema"`
		ClockID        string            `json:"clock_id"`
		DecisionMonoNS *int64            `json:"decision_mono_ns"`
		Signals        []placementSignal `json:"signals"`
		Raw            [][]float64       `json:"raw_features"`
	}
	if err := decoder.Decode(&decision); err != nil {
		t.Fatal(err)
	}
	if sample.Event != "placement_queue_sample" || decision.Event != "entry_decision" ||
		sample.ClockSchema != placementClockSchema || decision.ClockSchema != placementClockSchema ||
		sample.ClockID == "" || sample.ClockID != decision.ClockID ||
		sample.ReceivedMonoNS == nil || decision.DecisionMonoNS == nil ||
		decision.Signals[0].ClockID != decision.ClockID || decision.Signals[0].ReceivedMonoNS == nil ||
		*sample.ReceivedMonoNS != *decision.Signals[0].ReceivedMonoNS ||
		*decision.DecisionMonoNS < *sample.ReceivedMonoNS {
		t.Fatalf("audit clock mismatch: %+v %+v", sample, decision)
	}
	// JSON strips Go's hidden monotonic component. Reconstruction still uses
	// exactly the same integer delta, even with an unrelated wall timestamp.
	stamp := placementClockReading{Wall: wall.Add(-time.Hour), ClockID: decision.ClockID, MonoNS: *decision.DecisionMonoNS}
	raw, err := newNewFeatures(decision.Signals, m.newNewPolicy.prior, m.newNewWork.population, 2, 1000, stamp)
	if err != nil {
		t.Fatal(err)
	}
	addEntryFeatures(raw, m.newNewWork.work, entryContext{PartnerShard: -1}, nil)
	if !reflect.DeepEqual(raw, decision.Raw) {
		t.Fatalf("logged input did not reproduce features: %v != %v", raw, decision.Raw)
	}
}

func TestPlacementClockTrueFeatureChangeAndNearTieActions(t *testing.T) {
	for _, tc := range []struct {
		name       string
		feature    int
		queues     [2]int
		offsets    [2]int64
		wantAction uint64
		nearTie    bool
	}{
		{"true queue difference", 0, [2]int{10, 11}, [2]int64{0, 0}, 1, false},
		{"exact tie keeps first shard", 4, [2]int{10, 10}, [2]int64{0, 0}, 0, false},
		{"one nanosecond is a real near tie", 4, [2]int{10, 10}, [2]int64{1, 0}, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := entryFixture()
			m.newNewPolicy.model.Layers[0].Weight[0][19] = 0
			m.newNewPolicy.model.Layers[0].Weight[0][tc.feature] = 1
			m.newNewPolicy.model.Layers[1].Weight[0][0] = 1
			for s := range tc.queues {
				queue, mono := tc.queues[s], tc.offsets[s]
				m.placementSignals.rows[s] = placementSignal{Queue: &queue, ClockID: m.placementSignals.clock.id, ReceivedMonoNS: &mono}
			}
			var audit bytes.Buffer
			m.nativeAudit = &batchAudit{w: bufio.NewWriter(&audit)}
			if got := m.chooseNewNewPolicy(0, entryContext{PartnerShard: -1}); got != tc.wantAction {
				t.Fatalf("action %d != %d", got, tc.wantAction)
			}
			m.nativeAudit.flush()
			var event struct{ Scores []float64 }
			if err := json.Unmarshal(audit.Bytes(), &event); err != nil {
				t.Fatal(err)
			}
			if tc.nearTie && !(event.Scores[1]-event.Scores[0] > 0 && event.Scores[1]-event.Scores[0] < 1e-6) {
				t.Fatalf("fixture did not exercise near tie: %v", event.Scores)
			}
		})
	}
}
