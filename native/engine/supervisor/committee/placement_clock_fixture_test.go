package committee

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"
)

// The fixture contains outputs of the production feature builders. It creates
// no committee, network, training process, or chain workload.
func TestPlacementClockExportFixture(t *testing.T) {
	p := &newNewPrior{
		Schema: "joint_live_prior_v1", Version: 1, Cutoff: 42, SourceTime: 1000,
		GraphCutoff: 42, ForestCutoff: 42, GraphTime: 1000, ForestTime: 1000,
		Rates:       [][]float64{{0, .001, .002}, {.002, .003, .004}, {.01, .02, .03}, {.1, .2, .3}},
		Persistence: []float64{0, .25, .5, 1}, Graph: []float64{0, 1, 2, 3}, Forest: []float64{3, 2, 1, 0},
		Arrivals: []float64{0, .01, .02, .03}, Followup: []float64{0, .03, .02, .01}, Mass: []float64{0, 2, 4, 8},
	}
	wall := time.Date(2026, 10, 11, 12, 0, 0, 123456789, time.UTC)
	population, work := []int64{10, 20, 30, 40}, []int64{100, 200, 300, 400}
	c := entryContext{KnownSender: true, Novel: 1, PartnerShard: 1, Partner: "fixture-partner"}
	profile := [3]float64{.0123, 7, 1.5}
	cases := make([]map[string]interface{}, 0, 4)
	var baseline [][]float64
	for _, tc := range []struct {
		name       string
		wallJump   time.Duration
		monoOffset int64
	}{
		{"base_zero_sample", 0, 0},
		{"wall_backward", -24 * time.Hour, 0},
		{"wall_forward", 24 * time.Hour, 0},
		{"integer_offset_above_2pow53", 0, 1<<53 + 123},
	} {
		q0, q1, q3 := 19, 0, 321
		m0, m1, m3 := tc.monoOffset, tc.monoOffset+1234567890, tc.monoOffset+5000000000
		id := "clock-fixture-v2"
		rows := []placementSignal{
			{Assigned: 123, Confirmed: 100, Queue: &q0, ClockID: id, ReceivedMonoNS: &m0, ReceivedAt: wall, BodyLength: 700, Intervals: []float64{4, 6}},
			{Assigned: 220, Confirmed: 200, Queue: &q1, ClockID: id, ReceivedMonoNS: &m1, ReceivedAt: wall.Add(time.Second), BodyLength: 400, Intervals: []float64{6, 4, 5}},
			{Assigned: 330, Confirmed: 300, BodyLength: 0},
			{Assigned: 400, Confirmed: 400, Queue: &q3, ClockID: id, ReceivedMonoNS: &m3, ReceivedAt: wall.Add(5 * time.Second), BodyLength: 1000, Intervals: []float64{5}},
		}
		stamp := placementClockReading{Wall: wall.Add(5*time.Second + tc.wallJump), ClockID: id, MonoNS: tc.monoOffset + 5000000123}
		raw, err := newNewFeatures(rows, p, population, 45, 1060, stamp)
		if err != nil {
			t.Fatal(err)
		}
		addEntryFeatures(raw, work, c, &profile)
		if baseline == nil {
			baseline = raw
		} else if !reflect.DeepEqual(raw, baseline) {
			t.Fatalf("clock representation changed fixture features: %s", tc.name)
		}
		cases = append(cases, map[string]interface{}{
			"fixture_case": tc.name, "event": "entry_decision", "clock_schema": placementClockSchema,
			"clock_id": id, "decision_mono_ns": stamp.MonoNS, "decision_at": stamp.Wall.UTC(),
			"sequence": 3, "decision_sequence": 45, "source_time": 1060, "signals": rows,
			"prior": p, "population": population, "cumulative_work": work, "context": c,
			"partner_profile": profile, "raw_features": raw,
		})
	}
	path := os.Getenv("SAA_PLACEMENT_CLOCK_FIXTURE_PATH")
	if path == "" {
		return
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(map[string]interface{}{"schema": "placement_clock_feature_fixture_v1", "cases": cases}); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}
