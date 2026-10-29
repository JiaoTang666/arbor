package committee

import (
	"encoding/json"
	"math"
	"os"
	"testing"
)

func tinyNewNewModel() newNewModel {
	return newNewModel{Schema: "newnew_shared_scorer_v1", Architecture: "linear", FeatureOrder: newNewFeatureNames,
		Normalizer: newNewNormalizer{Features: newNewFeatureNames, Mean: make([]float64, 18), Scale: []float64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, FittedOn: "policy_fit_only"},
		Layers:     []newNewLayer{{Weight: [][]float64{make([]float64, 18)}, Bias: []float64{0}}}}
}

func TestNewNewModelRejectsMalformedBeforeDispatch(t *testing.T) {
	m := tinyNewNewModel()
	m.FeatureOrder = []string{"wrong"}
	b, _ := json.Marshal(m)
	if _, err := parseNewNewModel(b); err == nil {
		t.Fatal("accepted wrong features")
	}
	m = tinyNewNewModel()
	m.Normalizer.Scale[0] = 0
	b, _ = json.Marshal(m)
	if _, err := parseNewNewModel(b); err == nil {
		t.Fatal("accepted zero scale")
	}
	m = tinyNewNewModel()
	m.Layers[0].Weight[0] = []float64{1}
	b, _ = json.Marshal(m)
	if _, err := parseNewNewModel(b); err == nil {
		t.Fatal("accepted wrong layer")
	}
}

func TestNewNewFeaturesMissingAndFutureEvidence(t *testing.T) {
	p := &newNewPrior{Cutoff: 2, SourceTime: 100, Rates: [][]float64{{0, 0, 0}, {0, 0, 0}}, Persistence: []float64{0, 0}, Arrivals: []float64{0, 0}, Followup: []float64{0, 0}, Mass: []float64{0, 0}}
	rows := []placementSignal{{Assigned: 10, Confirmed: 4}, {}}
	decision := processPlacementClock.now()
	x, err := newNewFeatures(rows, p, []int64{1, 1}, 2, 100, decision)
	if err != nil {
		t.Fatal(err)
	}
	if x[0][5] != 1 || x[0][1] != .006 || x[0][0] != 0 {
		t.Fatal(x)
	}
	if _, err = newNewFeatures(rows, p, []int64{1, 1}, 1, 100, decision); err == nil {
		t.Fatal("future cutoff accepted")
	}
	if _, err = newNewFeatures(rows, p, []int64{1, 1}, 2, 99, decision); err == nil {
		t.Fatal("future source accepted")
	}
}

func TestNewNewPythonGoParity(t *testing.T) {
	path := os.Getenv("SAA_NEWNEW_PARITY_FIXTURE")
	if path == "" {
		t.Skip("external saved-state fixture")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Model  json.RawMessage
		Raw    [][]float64
		Scores []float64
		Action int
	}
	if err = json.Unmarshal(b, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatal("empty parity fixture")
	}
	for i, c := range cases {
		got, err := ScoreNewNewModel(c.Model, c.Raw)
		if err != nil {
			t.Fatal(err)
		}
		best := 0
		for s, v := range got {
			if math.Abs(v-c.Scores[s]) > 1e-6 {
				t.Fatalf("case %d shard %d: %.17g != %.17g", i, s, v, c.Scores[s])
			}
			if v > got[best] {
				best = s
			}
		}
		if best != c.Action {
			t.Fatalf("case %d action %d != %d", i, best, c.Action)
		}
	}
}
