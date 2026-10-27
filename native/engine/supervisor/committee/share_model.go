package committee

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
)

var shareFeatureNames = append(append([]string(nil), newNewFeatureNames[:19]...),
	"visible_pending_delta_5s", "visible_pending_delta_20s", "pending_history_5s_available", "pending_history_20s_available",
	"newnew_family_age_lt5_count", "newnew_family_age_5_to20_count", "newnew_family_age_gt20_count", "newnew_family_stage_past5_count", "previous_share")

const shareRandomContext = "native-contiguous10m-v1"

type shareModel struct {
	Schema       string   `json:"schema"`
	FeatureOrder []string `json:"feature_order"`
	Normalizer   struct {
		Schema              string    `json:"schema,omitempty"`
		Decisions           string    `json:"decisions,omitempty"`
		Mean                []float64 `json:"mean"`
		Scale               []float64 `json:"scale"`
		Features            []string  `json:"features"`
		Fit                 string    `json:"fit"`
		UnnormalizedColumns []int     `json:"unnormalized_columns"`
	} `json:"normalizer"`
	Encoder           newNewLayer `json:"encoder"`
	Hidden            newNewLayer `json:"hidden"`
	Output            newNewLayer `json:"output"`
	PressureScale     float64     `json:"pressure_scale"`
	ResidualBound     float64     `json:"residual_bound"`
	NativeExploration bool        `json:"native_exploration"`
	Checkpoint        string      `json:"checkpoint"`
	CheckpointHash    string      `json:"checkpoint_sha256"`
	NormalizerHash    string      `json:"normalizer_sha256"`
	OldWeightsReused  bool        `json:"old_weights_reused"`
	ActorParameters   int        `json:"actor_parameters,omitempty"`
	DecisionGranularity string   `json:"decision_granularity,omitempty"`
	ArtifactMetadata json.RawMessage `json:"artifact_metadata,omitempty"`
}

func parseShareModel(data []byte) (*shareModel, error) {
	var m shareModel
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if e := d.Decode(&m); e != nil {
		return nil, e
	}
	if e := d.Decode(new(interface{})); e != io.EOF {
		return nil, fmt.Errorf("trailing model data")
	}
	features, width, masks := shareFeatureNames, 28, []int{5, 6, 15, 21, 22, 27}
	if m.Schema == "entry_share_residual_v2" {
		features, width, masks = lightFeatureNamesV2, 20, []int{3,5,9,13,18,19}
		if m.ActorParameters!=1138 || m.DecisionGranularity!="each_newnew_first_binding" || m.Normalizer.Schema!=m.Schema || m.Normalizer.Decisions!="every newnew first binding" {
			return nil,fmt.Errorf("light model architecture or decision granularity mismatch")
		}
	}
	if (m.Schema != "entry_share_residual_v1" && m.Schema != "entry_share_residual_v2") || !sameNames(m.FeatureOrder, features) || !sameNames(m.Normalizer.Features, features) || m.Normalizer.Fit != "training fixed-A traces only" || m.NativeExploration || m.PressureScale != 1000 || !finite(m.ResidualBound) || math.Abs(m.ResidualBound-math.Log(2)) > 1e-15 || len(m.Normalizer.Mean) != width || len(m.Normalizer.Scale) != width {
		return nil, fmt.Errorf("share model contract mismatch")
	}
	if m.OldWeightsReused || m.CheckpointHash != "" && len(m.CheckpointHash) != 64 || m.NormalizerHash != "" && len(m.NormalizerHash) != 64 {
		return nil, fmt.Errorf("share checkpoint provenance invalid")
	}
	for i, v := range m.Normalizer.Mean {
		if !finite(v) || !finite(m.Normalizer.Scale[i]) || m.Normalizer.Scale[i] <= 0 {
			return nil, fmt.Errorf("share normalization invalid")
		}
	}
	if len(m.Normalizer.UnnormalizedColumns) != len(masks) {
		return nil, fmt.Errorf("share normalization mask")
	}
	for j, i := range masks {
		if m.Normalizer.UnnormalizedColumns[j] != i || m.Normalizer.Mean[i] != 0 || m.Normalizer.Scale[i] != 1 {
			return nil, fmt.Errorf("share mask normalized")
		}
	}
	for k, l := range []newNewLayer{m.Encoder, m.Hidden, m.Output} {
		widths := [][2]int{{width, 16}, {48, 16}, {16, 1}}
		in, out := widths[k][0], widths[k][1]
		if len(l.Weight) != out || len(l.Bias) != out {
			return nil, fmt.Errorf("share layer shape")
		}
		for j, w := range l.Weight {
			if len(w) != in || !finite(l.Bias[j]) {
				return nil, fmt.Errorf("share layer value")
			}
			for _, v := range w {
				if !finite(v) {
					return nil, fmt.Errorf("share weight nonfinite")
				}
			}
		}
	}
	return &m, nil
}

func shareLayer(x []float64, l newNewLayer, relu bool) []float64 {
	y := make([]float64, len(l.Bias))
	for j, w := range l.Weight {
		y[j] = l.Bias[j]
		for i, v := range w {
			y[j] += x[i] * v
		}
		if relu && y[j] < 0 {
			y[j] = 0
		}
	}
	return y
}

func (m *shareModel) latent(raw [][]float64) ([]float64, error) {
	if len(raw) != 16 {
		return nil, fmt.Errorf("share requires 16 shards")
	}
	h := make([][]float64, len(raw))
	mean := make([]float64, 16)
	peak := make([]float64, 16)
	width := len(m.Normalizer.Mean)
	for s, row := range raw {
		if len(row) != width {
			return nil, fmt.Errorf("share feature width")
		}
		x := make([]float64, width)
		for j, v := range row {
			if !finite(v) {
				return nil, fmt.Errorf("share feature nonfinite")
			}
			x[j] = float64(float32((v - m.Normalizer.Mean[j]) / m.Normalizer.Scale[j]))
			if !finite(x[j]) {
				return nil, fmt.Errorf("share normalization overflow")
			}
		}
		h[s] = shareLayer(x, m.Encoder, true)
		for j, v := range h[s] {
			mean[j] += v / float64(len(raw))
			peak[j] = math.Max(peak[j], v)
		}
	}
	out := make([]float64, len(raw))
	for s, row := range h {
		x := append(append(append([]float64(nil), row...), mean...), peak...)
		out[s] = shareLayer(shareLayer(x, m.Hidden, true), m.Output, false)[0]
		if !finite(out[s]) {
			return nil, fmt.Errorf("share score nonfinite")
		}
	}
	return out, nil
}

// Scores and probabilities use float64; the pressure input never uses normalized values.
func shareProbabilities(raw [][]float64, model *shareModel) (latent, residual, base, prob []float64, err error) {
	n := len(raw)
	if n != 16 {
		err = fmt.Errorf("share requires 16 shards")
		return
	}
	latent = make([]float64, n)
	if model != nil {
		latent, err = model.latent(raw)
		if err != nil {
			return
		}
	}
	residual = make([]float64, n)
	base = make([]float64, n)
	prob = make([]float64, n)
	peak := math.Inf(-1)
	width:=len(raw[0])
	for s, row := range raw {
		if len(row)!=width || (len(row) != 28 && len(row) != 20) {
			err = fmt.Errorf("share feature width")
			return
		}
		for _, v := range row {
			if !finite(v) {
				err = fmt.Errorf("share feature nonfinite")
				return
			}
		}
		p := row[1]
		if len(row)==20 {
			p = row[0]
			if row[3]==0 { p=math.Max(p,row[1]) }
		} else if row[5] == 0 { p = math.Max(p, row[0]) }
		if p < 0 {
			err = fmt.Errorf("negative share pressure")
			return
		}
		base[s] = -p
		residual[s] = math.Log(2) * math.Tanh(latent[s])
		prob[s] = base[s] + residual[s]
		peak = math.Max(peak, prob[s])
	}
	total := 0.
	for s, v := range prob {
		prob[s] = math.Exp(v - peak)
		total += prob[s]
	}
	for s := range prob {
		prob[s] /= total
	}
	return
}

func ShareUniform(context string, ordinal uint64) (float64, error) {
	if context == "" {
		return 0, fmt.Errorf("share random context is empty")
	}
	for _, c := range context {
		if c > 127 {
			return 0, fmt.Errorf("share random context must be ASCII")
		}
	}
	var b bytes.Buffer
	encoder := json.NewEncoder(&b)
	encoder.SetEscapeHTML(false)
	e := encoder.Encode([]interface{}{43, context, ordinal})
	if e != nil {
		return 0, e
	}
	data := bytes.TrimSuffix(b.Bytes(), []byte{'\n'})
	h := sha256.Sum256(data)
	return float64(binary.BigEndian.Uint64(h[:8])>>11) / 9007199254740992., nil
}

func shareDraw(p []float64, u float64) (uint64, error) {
	if len(p) != 16 || !finite(u) || u < 0 || u >= 1 {
		return 0, fmt.Errorf("invalid share draw")
	}
	total := 0.
	for _, v := range p {
		if !finite(v) || v < 0 {
			return 0, fmt.Errorf("invalid probability")
		}
		total += v
	}
	if math.Abs(total-1) > 1e-12 {
		return 0, fmt.Errorf("probabilities do not sum to one")
	}
	c := 0.
	for s, v := range p {
		c += v
		if u < c {
			return uint64(s), nil
		}
	}
	return uint64(len(p) - 1), nil
}

func ScoreShareModel(data []byte, raw [][]float64) (map[string][]float64, error) {
	var m *shareModel
	var e error
	if len(data) > 0 {
		m, e = parseShareModel(data)
		if e != nil {
			return nil, e
		}
	}
	l, d, b, p, e := shareProbabilities(raw, m)
	if e != nil {
		return nil, e
	}
	return map[string][]float64{"latent_mean": l, "residual": d, "base_logits": b, "probabilities": p}, nil
}
