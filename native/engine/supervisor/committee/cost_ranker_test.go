package committee

import (
	"blockEmulator/params"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"
)

func testCostModel() *costRankerModel {
	m := &costRankerModel{Schema: costRankerSchema, Status: "FIT", FeatureOrder: costFeatureNames, Coef: make([]float64, 20), TargetScale: 5000, Ridge: .01, Intercept: new(bool), ModelParameters: 20, ScoreUnits: "d=total original complete-waiting seconds/5000", ScoreFormula: "target_scale * dot(coef, (raw-mean)/scale)", TieTolerance: costTieTolerance, TieOrder: "first unique candidate in rule order", TrainingRecordsHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SourceHashes: json.RawMessage(`{}`), Training: json.RawMessage(`{}`)}
	m.Normalizer.Features = costFeatureNames
	m.Normalizer.Mean = make([]float64, 20)
	m.Normalizer.Scale = make([]float64, 20)
	for i := range m.Normalizer.Scale {
		m.Normalizer.Scale[i] = 1
	}
	m.Normalizer.UnnormalizedColumns = []int{3, 5, 9, 13, 18, 19}
	m.Normalizer.FittedOn = "training points only; all 16 shards per point"
	contract := map[string]interface{}{"feature_order": costFeatureNames, "candidates": 4, "seed": 43, "first": "original P CDF using original pressure arithmetic", "additional": "counter-hash independent domain; remaining positive probability without replacement", "tie_tolerance_cost_units": costTieTolerance, "tie_order": "candidate order", "pressure": "max(queue,assigned-visible_confirmed), missing queue uses unconfirmed", "previous_share": "previous base pressure proportions, never a learned output"}
	m.CandidateContract, _ = json.Marshal(contract)
	h := sha256.Sum256(m.CandidateContract)
	m.CandidateContractHash = hex.EncodeToString(h[:])
	return m
}

func TestCostRankerSchemaAndPhysicalDoubleScores(t *testing.T) {
	m := testCostModel()
	m.Coef[0] = 2
	m.TargetScale = 3
	data, _ := json.Marshal(m)
	parsed, e := parseCostRankerModel(data)
	if e != nil {
		t.Fatal(e)
	}
	raw := make([][]float64, 16)
	for i := range raw {
		raw[i] = make([]float64, 20)
	}
	raw[7][0] = 1.0000000001
	q, e := parsed.scores(raw, []uint64{7})
	if e != nil || math.Abs(q[0]-6.0000000006) > 1e-14 {
		t.Fatal("physical float64 score lost", q, e)
	}
	for name, mutate := range map[string]func(*costRankerModel){
		"residual schema": func(m *costRankerModel) { m.Schema = "entry_share_residual_v2" },
		"old last feature": func(m *costRankerModel) {
			m.FeatureOrder = append([]string(nil), m.FeatureOrder...)
			m.FeatureOrder[19] = "previous_share"
		},
		"hidden intercept":    func(m *costRankerModel) { *m.Intercept = true },
		"changed tolerance":   func(m *costRankerModel) { m.TieTolerance = 1e-3 },
		"normalised previous": func(m *costRankerModel) { m.Normalizer.Scale[19] = 2 },
		"signal manufacture":  func(m *costRankerModel) { m.Status = "NO_SIGNAL"; m.Coef[0] = 1 },
		"bad provenance":      func(m *costRankerModel) { m.CandidateContractHash = "" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := testCostModel()
			mutate(bad)
			b, _ := json.Marshal(bad)
			if _, e := parseCostRankerModel(b); e == nil {
				t.Fatal("bad contract accepted")
			}
		})
	}
}

func TestCostSamplerFirstPressureAndNoReplacement(t *testing.T) {
	for _, p := range [][]float64{{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, {.1, .2, .7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, {.0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625, .0625}} {
		for _, ordinal := range []uint64{0, 1, 7, 1000} {
			ids, uniforms, e := sampleCostCandidates(p, shareRandomContext, ordinal)
			if e != nil {
				t.Fatal(e)
			}
			u, _ := ShareUniform(shareRandomContext, ordinal)
			first, _ := shareDraw(p, u)
			if uniforms[0] != u || ids[0] != first {
				t.Fatal("candidate zero differs from original P")
			}
			seen := map[uint64]bool{}
			positive := 0
			for _, v := range p {
				if v > 0 {
					positive++
				}
			}
			if positive > 4 {
				positive = 4
			}
			if len(ids) != positive {
				t.Fatal("wrong positive support")
			}
			for _, s := range ids {
				if seen[s] || p[s] <= 0 {
					t.Fatal("replacement/zero mass selected")
				}
				seen[s] = true
			}
		}
	}
	if _, _, e := sampleCostCandidates(make([]float64, 16), shareRandomContext, 0); e == nil {
		t.Fatal("zero distribution accepted")
	}
	// Power-of-two scaling puts the second counted draw exactly at the
	// remaining slate's CDF boundary; the interval is right-open.
	u1, _ := costUniform(shareRandomContext, 0, 1)
	boundary := make([]float64, 16)
	boundary[0] = .5
	boundary[1] = .5 * u1
	boundary[2] = .5 * (1 - u1)
	ids, _, e := sampleCostCandidates(boundary, shareRandomContext, 0)
	if e != nil || !reflect.DeepEqual(ids, []uint64{0, 2, 1}) {
		t.Fatal("remaining positive CDF boundary", ids, e)
	}
	if s, e := chooseCostCandidate([]uint64{7, 2, 9}, []float64{1e-7, 0, 2}); e != nil || s != 7 {
		t.Fatal("physical near tie not in candidate order")
	}
	if s, e := chooseCostCandidate([]uint64{7, 2, 9}, []float64{2e-6, 0, 2}); e != nil || s != 2 {
		t.Fatal("physical minimum ignored")
	}
}

func TestCostRankerEveryEntryPreviousBasePressure(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 16
	defer func() { params.ShardNum = old }()
	m, log := forestHookFixtureV2(t, true)
	m.newNewPolicy.mode = "cost_ranker_v1"
	m.newNewPolicy.share.costModel = testCostModel()
	m.forestJoint.lightDayRates = make([]float64, 16)
	m.newNewPolicy.share.costModel.Coef[19] = 100
	for i, pair := range [][2]int{{8, 9}, {10, 10}, {1, 11}, {8, 12}, {13, 14}} {
		forestHookTransactionV2(t, m, uint64(i), pair[0], pair[1])
	}
	m.nativeAudit.flush()
	var previous []float64
	count := 0
	for _, line := range bytes.Split(bytes.TrimSpace(log.Bytes()), []byte("\n")) {
		var row struct {
			Event    string                `json:"event"`
			Seq      uint64                `json:"sequence"`
			Previous []float64             `json:"previous_pressure_share"`
			Base     []float64             `json:"base_proportions"`
			Fast     ForestFactsSnapshotV2 `json:"fast"`
			Features []string              `json:"feature_order"`
		}
		if e := json.Unmarshal(line, &row); e != nil {
			t.Fatal(e)
		}
		if row.Event != "cost_entry_decision" {
			continue
		}
		if row.Fast.BoundPrefix != row.Seq || row.Fast.AssignedPrefix != row.Seq || !sameNames(row.Features, costFeatureNames) {
			t.Fatal("current binding/feature semantics leaked")
		}
		if previous != nil && !reflect.DeepEqual(previous, row.Previous) {
			t.Fatal("previous value is not base pressure proportions")
		}
		previous = row.Base
		count++
	}
	if count != 3 || !reflect.DeepEqual(m.newNewPolicy.share.previous, previous) || !reflect.DeepEqual(m.newNewPolicy.share.prob, previous) {
		t.Fatal("not per NN/base distribution cache", count)
	}
	if bytes.Contains(log.Bytes(), []byte(`"event":"light_entry_decision"`)) {
		t.Fatal("residual audit path used")
	}
}

func TestCostRankerExportParity(t *testing.T) {
	path := os.Getenv("SAA_COST_PARITY_FIXTURE")
	if path == "" {
		t.Skip("frozen full cost model fixture required")
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var fixture struct {
		Model json.RawMessage `json:"model"`
		Cases []struct {
			Raw        [][]float64 `json:"raw"`
			Pressure   []float64   `json:"pressure"`
			Prob       []float64   `json:"probabilities"`
			Candidates []uint64    `json:"candidates"`
			Uniforms   []float64   `json:"uniforms"`
			Scores     []float64   `json:"scores"`
			Action     uint64      `json:"action"`
			Context    string      `json:"context"`
			Ordinal    uint64      `json:"ordinal"`
		} `json:"cases"`
	}
	if e = json.Unmarshal(raw, &fixture); e != nil {
		t.Fatal(e)
	}
	model, e := parseCostRankerModel(fixture.Model)
	if e != nil {
		t.Fatal(e)
	}
	if len(fixture.Cases) == 0 {
		t.Fatal("empty cost parity")
	}
	for i, c := range fixture.Cases {
		got, e := model.scores(c.Raw, nil)
		if e != nil {
			t.Fatal(e)
		}
		if len(c.Scores) != 16 || len(c.Pressure) != 16 || len(c.Prob) != 16 {
			t.Fatal("bad parity widths")
		}
		rows := make([]placementSignal, 16)
		observed := make([]lightTelemetryObservationV2, 16)
		for s, v := range c.Pressure {
			if v < 0 || v != math.Trunc(v) {
				t.Fatal("noninteger reference pressure")
			}
			rows[s].Assigned = int64(v)
			if math.Abs(got[s]-c.Scores[s]) > 1e-6 {
				t.Fatalf("case%d score%d %.17g != %.17g", i, s, got[s], c.Scores[s])
			}
		}
		_, p, e := costBasePressure(rows, observed)
		if e != nil {
			t.Fatal(e)
		}
		for s, v := range p {
			if math.Abs(v-c.Prob[s]) > 1e-12 {
				t.Fatalf("case%d pressure probability%d", i, s)
			}
		}
		ids, uniforms, e := sampleCostCandidates(p, c.Context, c.Ordinal)
		if e != nil || !reflect.DeepEqual(ids, c.Candidates) || !reflect.DeepEqual(uniforms, c.Uniforms) {
			t.Fatalf("case%d counted candidates differ %v/%v %v", i, ids, c.Candidates, e)
		}
		selected := make([]float64, len(ids))
		for k, s := range ids {
			selected[k] = got[s]
		}
		a, e := chooseCostCandidate(ids, selected)
		if e != nil || a != c.Action {
			t.Fatalf("case%d action%d != %d %v", i, a, c.Action, e)
		}
	}
}
