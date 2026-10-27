package committee

import (
	"blockEmulator/initialledger"
	"blockEmulator/params"
	"bufio"
	"io"
	"strings"
	"testing"
)

func entryFixture() *RelayCommitteeModule {
	width := len(newNewFeatureNames)
	scale := make([]float64, width)
	for i := range scale {
		scale[i] = 1
	}
	weight := make([][]float64, 32)
	for i := range weight {
		weight[i] = make([]float64, 3*width)
	}
	weight[0][19] = 1
	last := make([]float64, 32)
	last[0] = -1
	model := &newNewModel{Architecture: "plain", Normalizer: newNewNormalizer{Mean: make([]float64, width), Scale: scale},
		Layers: []newNewLayer{{Weight: weight, Bias: make([]float64, 32)}, {Weight: [][]float64{last}, Bias: []float64{0}}}}
	p := &newNewPrior{Cutoff: 2, SourceTime: 1000, Rates: [][]float64{{0, 0, 0}, {0, 0, 0}},
		Persistence: []float64{0, 0}, Arrivals: []float64{0, 0}, Followup: []float64{0, 0}, Mass: []float64{0, 0}, profiles: map[string][3]float64{}}
	return &RelayCommitteeModule{initialLedger: &initialledger.Ledger{Routes: map[string]uint64{strings.Repeat("0", 39) + "1": 0}},
		nativeRoutes: map[string]uint64{}, offeredLoadTPS: 2000,
		newNewWork:       &newNewWorkLedger{cumulative: true, work: make([]int64, 2), population: []int64{1, 0}},
		placementSignals: newPlacementSignals(2), nativeAudit: &batchAudit{w: bufio.NewWriter(io.Discard)},
		newNewPolicy: &newNewPlacementPolicy{mode: "ppo", model: model, source: []int64{1000, 1001}, base: 2, prior: p, bindingSequence: map[string]int64{}}}
}

func TestEntryNewOldBothDirections(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 2
	defer func() { params.ShardNum = old }()
	a, b := strings.Repeat("0", 39)+"1", strings.Repeat("0", 39)+"2"
	for _, reverse := range []bool{false, true} {
		m := entryFixture()
		x, y := a, b
		if reverse {
			x, y = b, a
		}
		s, d := m.nativePlacement(x, y)
		if !reverse && (s != 0 || d != 1) || reverse && (s != 1 || d != 0) {
			t.Fatalf("new-old action ignored: %d %d", s, d)
		}
		if m.initialLedger.Routes[a] != 0 || m.nativeRoutes[b] != 1 || m.newNewWork.population[1] != 1 {
			t.Fatal("binding invariant")
		}
		m.placementSequence = 1
		s2, d2 := m.nativePlacement(x, y)
		if s2 != s || d2 != d {
			t.Fatal("existing route changed")
		}
		if m.newNewWork.work[0] != 2 || m.newNewWork.work[1] != 2 {
			t.Fatal("stage accounting")
		}
	}
}

func TestEntryNewNewAndSelfBindOnce(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 2
	defer func() { params.ShardNum = old }()
	a, b := strings.Repeat("0", 39)+"2", strings.Repeat("0", 39)+"3"
	for _, self := range []bool{false, true} {
		m := entryFixture()
		other := b
		if self {
			other = a
		}
		s, d := m.nativePlacement(a, other)
		if s != d {
			t.Fatal("split new-new")
		}
		expected := int64(3)
		if self {
			expected = 2
		}
		if m.newNewWork.population[0] != expected || m.newNewWork.work[0] != 1 {
			t.Fatal("duplicate population or stage")
		}
	}
}
