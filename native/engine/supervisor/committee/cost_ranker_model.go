package committee

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
)

const costRankerSchema = "entry_cost_ranker_v1"
const costTieTolerance = 1e-6

var costFeatureNames = func() []string {
	x := append([]string(nil), lightFeatureNamesV2...)
	x[19] = "previous_pressure_share"
	return x
}()

type costRankerModel struct {
	ForestEnabled *bool `json:"forest_enabled,omitempty"`
	AblationRole string `json:"ablation_role,omitempty"`
	Schema       string   `json:"schema"`
	Status       string   `json:"status"`
	FeatureOrder []string `json:"feature_order"`
	Normalizer   struct {
		Features            []string  `json:"features"`
		Mean                []float64 `json:"mean"`
		Scale               []float64 `json:"scale"`
		UnnormalizedColumns []int     `json:"unnormalized_columns"`
		FittedOn            string    `json:"fitted_on"`
		Weighting           string    `json:"weighting"`
	} `json:"normalizer"`
	Coef                  []float64       `json:"coef"`
	TargetScale           float64         `json:"target_scale"`
	Ridge                 float64         `json:"ridge"`
	Intercept             *bool           `json:"intercept"`
	ModelParameters       int             `json:"model_parameters"`
	ScoreUnits            string          `json:"score_units"`
	ScoreFormula          string          `json:"score_formula"`
	TieTolerance          float64         `json:"tie_tolerance"`
	TieOrder              string          `json:"tie_order"`
	TargetCentering       string          `json:"target_centering"`
	TargetScaling         string          `json:"target_scaling"`
	Objective             string          `json:"objective"`
	CandidateContract     json.RawMessage `json:"candidate_contract"`
	CandidateContractHash string          `json:"candidate_contract_sha256"`
	SourceHashes          json.RawMessage `json:"source_hashes"`
	TrainingRecordsHash   string          `json:"training_records_sha256"`
	Training              json.RawMessage `json:"training"`
}

func parseCostRankerModel(data []byte) (*costRankerModel, error) {
	var m costRankerModel
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if e := d.Decode(&m); e != nil {
		return nil, e
	}
	if e := d.Decode(new(interface{})); e != io.EOF {
		return nil, fmt.Errorf("trailing cost model data")
	}
	if (m.ForestEnabled != nil && (*m.ForestEnabled || m.AblationRole != "GP")) || (m.ForestEnabled == nil && m.AblationRole != "") {
		return nil, fmt.Errorf("cost model forest/ablation metadata mismatch")
	}
	if m.Schema != costRankerSchema || (m.Status != "FIT" && m.Status != "NO_SIGNAL") || !sameNames(m.FeatureOrder, costFeatureNames) || !sameNames(m.Normalizer.Features, costFeatureNames) || len(m.Coef) != 20 || len(m.Normalizer.Mean) != 20 || len(m.Normalizer.Scale) != 20 || m.Intercept == nil || *m.Intercept || m.ModelParameters != 20 || m.Ridge != .01 || m.TieTolerance != costTieTolerance || !finite(m.TargetScale) || m.TargetScale <= 0 || m.ScoreUnits != "d=total original complete-waiting seconds/5000" || m.ScoreFormula != "target_scale * dot(coef, (raw-mean)/scale)" || m.TieOrder != "first unique candidate in rule order" {
		return nil, fmt.Errorf("cost model semantic/architecture contract mismatch")
	}
	mask := []int{3, 5, 9, 13, 18, 19}
	if len(m.Normalizer.UnnormalizedColumns) != len(mask) || m.Normalizer.FittedOn != "training points only; all 16 shards per point" {
		return nil, fmt.Errorf("cost normalizer contract mismatch")
	}
	for i, v := range m.Coef {
		if !finite(v) || !finite(m.Normalizer.Mean[i]) || !finite(m.Normalizer.Scale[i]) || m.Normalizer.Scale[i] <= 0 || (m.Status == "NO_SIGNAL" && v != 0) {
			return nil, fmt.Errorf("invalid cost coefficient/normalization")
		}
	}
	for j, i := range mask {
		if m.Normalizer.UnnormalizedColumns[j] != i || m.Normalizer.Mean[i] != 0 || m.Normalizer.Scale[i] != 1 {
			return nil, fmt.Errorf("cost flag/previous pressure normalized")
		}
	}
	var contract struct {
		Features     []string `json:"feature_order"`
		Candidates   int      `json:"candidates"`
		Seed         int      `json:"seed"`
		First        string   `json:"first"`
		Additional   string   `json:"additional"`
		TieTolerance float64  `json:"tie_tolerance_cost_units"`
		TieOrder     string   `json:"tie_order"`
		Pressure     string   `json:"pressure"`
		Previous     string   `json:"previous_share"`
	}
	if e := json.Unmarshal(m.CandidateContract, &contract); e != nil {
		return nil, e
	}
	if !sameNames(contract.Features, costFeatureNames) || contract.Candidates != 4 || contract.Seed != 43 || contract.TieTolerance != costTieTolerance || contract.TieOrder != "candidate order" || contract.First != "original P CDF using original pressure arithmetic" || contract.Additional != "counter-hash independent domain; remaining positive probability without replacement" || contract.Pressure != "max(queue,assigned-visible_confirmed), missing queue uses unconfirmed" || contract.Previous != "previous base pressure proportions, never a learned output" {
		return nil, fmt.Errorf("cost candidate contract mismatch")
	}
	// Preserve the original number spellings when checking Python's canonical
	// sorted ASCII contract JSON; no floating-point re-rendering is involved.
	var canonical interface{}
	decoder := json.NewDecoder(bytes.NewReader(m.CandidateContract))
	decoder.UseNumber()
	if e := decoder.Decode(&canonical); e != nil {
		return nil, e
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if e := encoder.Encode(canonical); e != nil {
		return nil, e
	}
	h := sha256.Sum256(bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'}))
	if hex.EncodeToString(h[:]) != m.CandidateContractHash {
		return nil, fmt.Errorf("cost candidate contract hash mismatch")
	}
	if b, e := hex.DecodeString(m.TrainingRecordsHash); e != nil || len(b) != 32 {
		return nil, fmt.Errorf("cost training provenance missing")
	}
	return &m, nil
}

func (m *costRankerModel) scores(raw [][]float64, candidates []uint64) ([]float64, error) {
	shards := len(raw)
	all := candidates == nil
	if all {
		candidates = make([]uint64, shards)
		for i := range candidates {
			candidates[i] = uint64(i)
		}
	}
	if shards < 1 || len(candidates) < 1 || (!all && len(candidates) > 4) {
		return nil, fmt.Errorf("cost decision dimensions")
	}
	for _, row := range raw {
		if len(row) != 20 {
			return nil, fmt.Errorf("cost feature width")
		}
		for _, v := range row {
			if !finite(v) {
				return nil, fmt.Errorf("cost feature nonfinite")
			}
		}
	}
	out := make([]float64, len(candidates))
	seen := map[uint64]bool{}
	for k, s := range candidates {
		if s >= uint64(shards) || seen[s] {
			return nil, fmt.Errorf("invalid cost candidate slate")
		}
		seen[s] = true
		for i, v := range raw[s] {
			out[k] += ((v - m.Normalizer.Mean[i]) / m.Normalizer.Scale[i]) * m.Coef[i]
		}
		out[k] *= m.TargetScale
		if !finite(out[k]) {
			return nil, fmt.Errorf("cost score overflow")
		}
	}
	return out, nil
}

func chooseCostCandidate(candidates []uint64, values []float64) (uint64, error) {
	if len(candidates) < 1 || len(candidates) != len(values) {
		return 0, fmt.Errorf("cost candidates/scores mismatch")
	}
	minimum := math.Inf(1)
	for _, v := range values {
		if !finite(v) {
			return 0, fmt.Errorf("nonfinite cost score")
		}
		minimum = math.Min(minimum, v)
	}
	for i, v := range values {
		if v <= minimum+costTieTolerance {
			return candidates[i], nil
		}
	}
	return 0, fmt.Errorf("no cost minimum")
}

func costUniform(context string, ordinal uint64, k int) (float64, error) {
	if k == 0 {
		return ShareUniform(context, ordinal)
	}
	if k < 1 || k > 3 {
		return 0, fmt.Errorf("invalid cost draw index")
	}
	if _, e := ShareUniform(context, ordinal); e != nil {
		return 0, e
	}
	var b bytes.Buffer
	encoder := json.NewEncoder(&b)
	encoder.SetEscapeHTML(false)
	if e := encoder.Encode([]interface{}{43, context, ordinal, k}); e != nil {
		return 0, e
	}
	h := sha256.Sum256(bytes.TrimSuffix(b.Bytes(), []byte{'\n'}))
	return float64(binary.BigEndian.Uint64(h[:8])>>11) / 9007199254740992., nil
}

func sampleCostCandidates(p []float64, context string, ordinal uint64) ([]uint64, []float64, error) {
	u, e := costUniform(context, ordinal, 0)
	if e != nil {
		return nil, nil, e
	}
	first, e := costPressureDraw(p, u)
	if e != nil {
		return nil, nil, e
	}
	ids, uniforms := []uint64{first}, []float64{u}
	remaining := append([]float64(nil), p...)
	remaining[first] = 0
	for k := 1; k < 4; k++ {
		available := []uint64{}
		total := 0.
		for s, v := range remaining {
			if v > 0 {
				available = append(available, uint64(s))
				total += v
			}
		}
		if len(available) == 0 {
			break
		}
		u, e = costUniform(context, ordinal, k)
		if e != nil {
			return nil, nil, e
		}
		cdf := 0.
		target := available[len(available)-1]
		for _, s := range available {
			cdf += remaining[s] / total
			if u < cdf {
				target = s
				break
			}
		}
		ids = append(ids, target)
		uniforms = append(uniforms, u)
		remaining[target] = 0
	}
	return ids, uniforms, nil
}

// The cost policy's original P CDF, with only its vector length generalized.
// Keep the arithmetic order identical to shareDraw for the frozen 16-shard
// fixtures; legacy residual policies retain their separate 16-shard guard.
func costPressureDraw(p []float64, u float64) (uint64, error) {
	if len(p) < 1 || !finite(u) || u < 0 || u >= 1 {
		return 0, fmt.Errorf("invalid cost pressure draw")
	}
	total := 0.
	for _, v := range p {
		if !finite(v) || v < 0 {
			return 0, fmt.Errorf("invalid cost pressure probability")
		}
		total += v
	}
	if math.Abs(total-1) > 1e-12 {
		return 0, fmt.Errorf("cost pressure probabilities do not sum to one")
	}
	cdf := 0.
	for s, v := range p {
		cdf += v
		if u < cdf {
			return uint64(s), nil
		}
	}
	return uint64(len(p) - 1), nil
}

func ScoreCostRankerModel(data []byte, raw [][]float64, candidates []uint64) ([]float64, error) {
	m, e := parseCostRankerModel(data)
	if e != nil {
		return nil, e
	}
	return m.scores(raw, candidates)
}
