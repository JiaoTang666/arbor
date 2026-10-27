package committee

import (
	"blockEmulator/initialledger"
	"blockEmulator/params"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"testing"
	"time"
)

func zeroShareModel() *shareModel {
	m := &shareModel{Schema: "entry_share_residual_v1", FeatureOrder: shareFeatureNames, PressureScale: 1000, ResidualBound: math.Log(2)}
	m.Normalizer.Mean = make([]float64, 28)
	m.Normalizer.Scale = make([]float64, 28)
	for i := range m.Normalizer.Scale {
		m.Normalizer.Scale[i] = 1
	}
	m.Normalizer.Features = shareFeatureNames
	m.Normalizer.Fit = "training fixed-A traces only"
	m.Normalizer.UnnormalizedColumns = []int{5, 6, 15, 21, 22, 27}
	for i, l := range []*newNewLayer{&m.Encoder, &m.Hidden, &m.Output} {
		shapes := [][2]int{{28, 16}, {48, 16}, {16, 1}}
		l.Weight = make([][]float64, shapes[i][1])
		l.Bias = make([]float64, shapes[i][1])
		for j := range l.Weight {
			l.Weight[j] = make([]float64, shapes[i][0])
		}
	}
	return m
}
func TestSharePressureAndZeroResidual(t *testing.T) {
	raw := make([][]float64, 16)
	for s := range raw {
		raw[s] = make([]float64, 28)
		raw[s][1] = float64(s)
		raw[s][5] = 1
		raw[s][0] = 1000
	}
	a, _, _, p, e := shareProbabilities(raw, nil)
	if e != nil {
		t.Fatal(e)
	}
	_, delta, _, q, e := shareProbabilities(raw, zeroShareModel())
	if e != nil {
		t.Fatal(e)
	}
	for s := range p {
		if p[s] != q[s] || delta[s] != 0 || a[s] != 0 {
			t.Fatal("zero residual failed")
		}
	}
	if p[0] <= p[1] {
		t.Fatal("pressure direction")
	}
	raw[0][5] = 0
	_, _, _, r, e := shareProbabilities(raw, nil)
	if e != nil || r[0] >= r[1] {
		t.Fatal("queue max ignored")
	}
}
func TestShareModelContractAndCDF(t *testing.T) {
	data, _ := json.Marshal(zeroShareModel())
	if _, e := parseShareModel(data); e != nil {
		t.Fatal(e)
	}
	var fields map[string]interface{}
	json.Unmarshal(data, &fields)
	fields["future"] = 1
	b, _ := json.Marshal(fields)
	if _, e := parseShareModel(b); e == nil {
		t.Fatal("unknown schema accepted")
	}
	p := make([]float64, 16)
	for i := range p {
		p[i] = 1. / 16
	}
	if s, e := shareDraw(p, 1./16); e != nil || s != 1 {
		t.Fatal("CDF boundary")
	}
	if s, e := shareDraw(p, math.Nextafter(1., 0.)); e != nil || s != 15 {
		t.Fatal("CDF tail")
	}
	if _, e := shareDraw(p, 1); e == nil {
		t.Fatal("uniform=1 accepted")
	}
	if _, e := ShareUniform("", 0); e == nil {
		t.Fatal("empty context")
	}
}

func TestShareCounterRandomPythonVectors(t *testing.T) {
	for ordinal, want := range map[uint64]float64{0: 0.052251606326652156, 1: 0.5773128548575653, 2: 0.3735525830328964, 1000: 0.03872772489742571} {
		got, err := ShareUniform(shareRandomContext, ordinal)
		if err != nil || got != want {
			t.Fatalf("counter %d: %.17g != %.17g %v", ordinal, got, want, err)
		}
	}
	got, err := ShareUniform("<a>&b", 0)
	if err != nil || got != 0.3924097365885836 {
		t.Fatal("HTML escaping changed random contract")
	}
}
func TestShareExposureBoundariesAndUniqueStages(t *testing.T) {
	e := shareExposure{counts: make([][4]int64, 16)}
	e.birth(0, 3)
	e.work(0, 3, 3)
	e.advance(5 * int64(time.Second))
	if e.counts[3] != [4]int64{0, 1, 0, 1} {
		t.Fatal(e.counts[3])
	}
	e.advance(5*int64(time.Second) + 1)
	if e.counts[3][3] != 0 {
		t.Fatal("past5 boundary")
	}
	e.advance(20 * int64(time.Second))
	if e.counts[3][1] != 1 {
		t.Fatal("age20 excluded")
	}
	e.advance(20*int64(time.Second) + 1)
	if e.counts[3][2] != 1 {
		t.Fatal("old age")
	}
	e.work(e.now, 3, 7)
	if e.counts[3][3] != 1 || e.counts[7][3] != 1 {
		t.Fatal("cross stages")
	}
}
func shareFixture(t *testing.T) (*RelayCommitteeModule, *bytes.Buffer) {
	t.Helper()
	p, e := newSharePlacement("pressure_share", 16)
	if e != nil {
		t.Fatal(e)
	}
	prior := &newNewPrior{Cutoff: 2, SourceTime: 1000, Rates: make([][]float64, 16), Persistence: make([]float64, 16), Arrivals: make([]float64, 16), Followup: make([]float64, 16), Mass: make([]float64, 16)}
	for s := range prior.Rates {
		prior.Rates[s] = make([]float64, 3)
	}
	source := make([]int64, 4000)
	for i := range source {
		source[i] = 1000 + int64(i)
	}
	population := make([]int64, 16)
	population[3] = 1
	var log bytes.Buffer
	m := &RelayCommitteeModule{initialLedger: &initialledger.Ledger{Routes: map[string]uint64{fmt.Sprintf("%040x", 1): 3}}, nativeRoutes: map[string]uint64{}, offeredLoadTPS: 2000, newNewWork: &newNewWorkLedger{cumulative: true, work: make([]int64, 16), population: population}, placementSignals: newPlacementSignals(16), nativeAudit: &batchAudit{w: bufio.NewWriter(&log)}, newNewPolicy: &newNewPlacementPolicy{mode: "pressure_share", source: source, base: 2, prior: prior, bindingSequence: map[string]int64{}, share: p}}
	m.placementSignals.enableHistory()
	return m, &log
}
func TestShareNativeFollowBindingBatchAndReplay(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 16
	defer func() { params.ShardNum = old }()
	for _, reverse := range []bool{false, true} {
		m, log := shareFixture(t)
		a, b := fmt.Sprintf("%040x", 1), fmt.Sprintf("%040x", 2)
		if reverse {
			a, b = b, a
		}
		s, d := m.nativePlacement(a, b)
		if s != 3 || d != 3 || m.newNewPolicy.share.ordinal != 0 {
			t.Fatal("new-old did not follow")
		}
		m.placementSignals.assign(s, d)
		m.placementSequence = 1
		s, d = m.nativePlacement(fmt.Sprintf("%040x", 3), fmt.Sprintf("%040x", 3))
		if s != d || m.newNewPolicy.share.ordinal != 1 || m.newNewWork.population[s] < 1 {
			t.Fatal("new self contract")
		}
		m.placementSignals.assign(s, d)
		root := s
		m.placementSequence = 2
		s, d = m.nativePlacement(fmt.Sprintf("%040x", 3), fmt.Sprintf("%040x", 4))
		if s != root || d != root || m.newNewPolicy.share.families[fmt.Sprintf("%040x", 4)] != 2 {
			t.Fatal("family inheritance")
		}
		m.placementSignals.assign(s, d)
		frozen := append([]float64(nil), m.newNewPolicy.share.prob...)
		for i := 3; i < 8; i++ {
			m.placementSequence = uint64(i)
			s, d = m.nativePlacement(fmt.Sprintf("%040x", 100+i*2), fmt.Sprintf("%040x", 101+i*2))
			m.placementSignals.assign(s, d)
			for j, p := range frozen {
				if m.newNewPolicy.share.prob[j] != p {
					t.Fatal("intra-batch probabilities changed")
				}
			}
		}
		m.nativeAudit.w.Flush()
		decisions := bytes.Count(log.Bytes(), []byte(`"event":"share_batch_decision"`))
		if decisions != 1 {
			t.Fatalf("batch decisions=%d", decisions)
		}
		m.placementSequence = 1000
		m.nativePlacement(fmt.Sprintf("%040x", 9000), fmt.Sprintf("%040x", 9001))
		if m.newNewPolicy.share.batch != 1 {
			t.Fatal("next batch not updated")
		}
	}
}
func TestShareExportParity(t *testing.T) {
	path := os.Getenv("SAA_SHARE_PARITY_FIXTURE")
	if path == "" {
		t.Skip("external frozen model fixture")
	}
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var f struct {
		Model json.RawMessage `json:"model"`
		Cases []struct {
			Raw      [][]float64 `json:"raw"`
			Latent   []float64   `json:"latent_mean"`
			Prob     []float64   `json:"probabilities"`
			Residual []float64   `json:"residual"`
			Base     []float64   `json:"base_logits"`
			U        float64     `json:"u"`
			Action   uint64      `json:"action"`
		} `json:"cases"`
	}
	if e = json.Unmarshal(data, &f); e != nil {
		t.Fatal(e)
	}
	if len(f.Cases) == 0 {
		t.Fatal("empty parity")
	}
	for i, c := range f.Cases {
		got, e := ScoreShareModel(f.Model, c.Raw)
		if e != nil {
			t.Fatal(e)
		}
		for name, want := range map[string][]float64{"latent_mean": c.Latent, "residual": c.Residual, "probabilities": c.Prob, "base_logits": c.Base} {
			if len(want) != 16 {
				t.Fatal("parity dimensions")
			}
			for s, v := range want {
				if math.Abs(got[name][s]-v) > 1e-6 {
					t.Fatalf("case%d %s shard%d %.17g != %.17g", i, name, s, got[name][s], v)
				}
			}
		}
		a, e := shareDraw(got["probabilities"], c.U)
		if e != nil || a != c.Action {
			t.Fatalf("case%d target%d != %d, %v", i, a, c.Action, e)
		}
	}
}
