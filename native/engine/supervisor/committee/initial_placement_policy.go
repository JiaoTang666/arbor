package committee

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"
)

var newNewFeatureNames = []string{
	"queue_slots", "pending_slots", "last_block_fill", "interval_over_target",
	"queue_age_intervals", "queue_missing", "interval_missing", "population_share",
	"graph_6h_log_hour_rate", "graph_1d_log_hour_rate", "graph_7d_log_hour_rate",
	"graph_day_weighted_persistence", "forest_origin_log_hour_arrivals",
	"forest_origin_log_hour_followup_incidence", "forest_log_mature_mass",
	"forest_no_mature_evidence", "prior_source_age_hours", "prior_lag_batches",
	"cumulative_work_slots", "partner_here", "known_sender", "known_recipient", "self_transfer", "novel_distinct_count",
	"partner_log_hour_arrivals", "partner_log_mature_mass", "partner_log_followup_incidence", "partner_profile_missing",
	"partner_no_mature_evidence", "first_extra_relay",
}
var entryResidualIndices = []int{8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29}

type newNewLayer struct {
	Weight [][]float64
	Bias   []float64
}
type newNewNormalizer struct {
	Features    []string
	Mean, Scale []float64
	FittedOn    string `json:"fitted_on"`
}
type newNewModel struct {
	Schema, Architecture string
	FeatureOrder         []string `json:"feature_order"`
	Normalizer           newNewNormalizer
	Layers               []newNewLayer
	ResidualLayers       []newNewLayer `json:"residual_layers"`
	ResidualIndices      []int         `json:"residual_indices"`
}

func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }
func sameNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func parseNewNewModel(data []byte) (*newNewModel, error) {
	var m newNewModel
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if m.Schema != "entry_shared_scorer_v1" || !sameNames(m.FeatureOrder, newNewFeatureNames) ||
		!sameNames(m.Normalizer.Features, newNewFeatureNames) || m.Normalizer.FittedOn != "policy_fit_only" ||
		len(m.Normalizer.Mean) != len(newNewFeatureNames) || len(m.Normalizer.Scale) != len(newNewFeatureNames) {
		return nil, fmt.Errorf("model schema/normalizer mismatch")
	}
	widths := map[string][]int{"plain": {90, 32, 1}, "entry_residual": {90, 32, 1}}
	width, ok := widths[m.Architecture]
	if !ok || len(m.Layers) != len(width)-1 {
		return nil, fmt.Errorf("unsupported scorer structure")
	}
	for i := range m.Normalizer.Mean {
		if !finite(m.Normalizer.Mean[i]) || !finite(m.Normalizer.Scale[i]) || m.Normalizer.Scale[i] <= 0 {
			return nil, fmt.Errorf("invalid normalization")
		}
	}
	for i, l := range m.Layers {
		if len(l.Weight) != width[i+1] || len(l.Bias) != width[i+1] {
			return nil, fmt.Errorf("layer shape mismatch")
		}
		for j, row := range l.Weight {
			if len(row) != width[i] || !finite(l.Bias[j]) {
				return nil, fmt.Errorf("layer shape/value mismatch")
			}
			for _, x := range row {
				if !finite(x) {
					return nil, fmt.Errorf("nonfinite weight")
				}
			}
		}
	}
	if len(m.ResidualIndices) != len(entryResidualIndices) {
		return nil, fmt.Errorf("residual feature mismatch")
	}
	for i, v := range m.ResidualIndices {
		if v != entryResidualIndices[i] {
			return nil, fmt.Errorf("residual index mismatch")
		}
	}
	if m.Architecture == "plain" && len(m.ResidualLayers) != 0 {
		return nil, fmt.Errorf("plain residual present")
	}
	if m.Architecture == "entry_residual" {
		if len(m.ResidualLayers) != 2 {
			return nil, fmt.Errorf("missing residual")
		}
		w := []int{len(entryResidualIndices), 16, 1}
		for i, l := range m.ResidualLayers {
			if len(l.Weight) != w[i+1] || len(l.Bias) != w[i+1] {
				return nil, fmt.Errorf("residual shape")
			}
			for j, row := range l.Weight {
				if len(row) != w[i] || !finite(l.Bias[j]) {
					return nil, fmt.Errorf("residual values")
				}
				for _, v := range row {
					if !finite(v) {
						return nil, fmt.Errorf("residual weight")
					}
				}
			}
		}
	}
	return &m, nil
}

func (m *newNewModel) scores(raw [][]float64) ([]float64, error) {
	if len(raw) < 2 {
		return nil, fmt.Errorf("invalid shard matrix")
	}
	width := len(newNewFeatureNames)
	x := make([][]float64, len(raw))
	mean := make([]float64, width)
	peak := make([]float64, width)
	for i := range peak {
		peak[i] = math.Inf(-1)
	}
	for s, row := range raw {
		if len(row) != width {
			return nil, fmt.Errorf("invalid feature width")
		}
		x[s] = make([]float64, width)
		for i, value := range row {
			if !finite(value) {
				return nil, fmt.Errorf("invalid feature value")
			}
			v := (value - m.Normalizer.Mean[i]) / m.Normalizer.Scale[i]
			x[s][i] = v
			mean[i] += v
			if v > peak[i] {
				peak[i] = v
			}
		}
	}
	for i := range mean {
		mean[i] /= float64(len(raw))
	}
	out := make([]float64, len(raw))
	for s, row := range x {
		residual := make([]float64, len(entryResidualIndices))
		for j, k := range entryResidualIndices {
			residual[j] = row[k]
		}
		row = append(append(row, mean...), peak...)
		for k, l := range m.Layers {
			next := make([]float64, len(l.Bias))
			for j, w := range l.Weight {
				v := l.Bias[j]
				for i, a := range w {
					v += a * row[i]
				}
				if k+1 < len(m.Layers) && v < 0 {
					v = 0
				}
				next[j] = v
			}
			row = next
		}
		for k, l := range m.ResidualLayers {
			next := make([]float64, len(l.Bias))
			for j, w := range l.Weight {
				v := l.Bias[j]
				for i, a := range w {
					v += a * residual[i]
				}
				if k+1 < len(m.ResidualLayers) && v < 0 {
					v = 0
				}
				next[j] = v
			}
			residual = next
		}
		out[s] = row[0]
		if len(m.ResidualLayers) > 0 {
			out[s] += residual[0]
		}
		if !finite(out[s]) {
			return nil, fmt.Errorf("nonfinite model score")
		}
	}
	return out, nil
}

// Shared by the production loader and cross-language parity command.
func ScoreNewNewModel(data []byte, raw [][]float64) ([]float64, error) {
	m, err := parseNewNewModel(data)
	if err != nil {
		return nil, err
	}
	return m.scores(raw)
}

type newNewPrior struct {
	ForestEnabled *bool `json:"forest_enabled"`
	ForestChannelsDisabled bool `json:"forest_channels_disabled"`
	Schema       string
	Version      int
	Cutoff       int64       `json:"accepted_cutoff_exclusive"`
	SourceTime   int64       `json:"source_time"`
	GraphCutoff  int64       `json:"graph_cutoff"`
	ForestCutoff int64       `json:"forest_cutoff"`
	GraphTime    int64       `json:"graph_source_time"`
	ForestTime   int64       `json:"forest_source_time"`
	Digest       string      `json:"snapshot_sha256"`
	Rates        [][]float64 `json:"relation_stage_rate_by_scale"`
	Persistence  []float64   `json:"relation_day_rate_weighted_persistence"`
	Graph        []float64   `json:"relation_stage_rate_proxy"`
	Forest       []float64   `json:"forest_incidence_rate_proxy"`
	Arrivals     []float64   `json:"direct_arrival_rate"`
	Followup     []float64   `json:"forest_followup_incidence_rate_proxy"`
	Mass         []float64   `json:"forest_mature_child_mass_by_shard"`
	ProfilePath  string      `json:"entry_profile_path"`
	ProfileHash  string      `json:"entry_profile_file_sha256"`
	GlobalMean   float64     `json:"global_weighted_followup_incidence"`
	profiles     map[string][3]float64
}

func readNewNewPrior(path string, shards int) (*newNewPrior, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p newNewPrior
	if err = json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	if p.Schema != "joint_live_prior_v1" || p.Version < 1 || len(p.Digest) != 64 || p.Cutoff < 0 ||
		p.GraphCutoff != p.Cutoff || p.ForestCutoff != p.Cutoff || p.GraphTime != p.SourceTime || p.ForestTime != p.SourceTime || len(p.Rates) != shards {
		return nil, fmt.Errorf("invalid joint publication generation")
	}
	for _, channel := range [][]float64{p.Persistence, p.Graph, p.Forest, p.Arrivals, p.Followup, p.Mass} {
		if len(channel) != shards {
			return nil, fmt.Errorf("invalid prior channel width")
		}
		for _, v := range channel {
			if !finite(v) || v < 0 {
				return nil, fmt.Errorf("invalid prior channel")
			}
		}
	}
	for s, row := range p.Rates {
		if len(row) != 3 || p.Persistence[s] > 1+1e-9 {
			return nil, fmt.Errorf("invalid relation features")
		}
		for _, v := range row {
			if !finite(v) || v < 0 {
				return nil, fmt.Errorf("invalid relation rates")
			}
		}
	}
	return &p, nil
}

func (p *newNewPrior) loadProfiles() error {
	data, err := checkedFile(p.ProfilePath, p.ProfileHash)
	if err != nil {
		return err
	}
	if len(data) < 16 || string(data[:8]) != "ENTPROF1" {
		return fmt.Errorf("profile header")
	}
	n := binary.LittleEndian.Uint64(data[8:16])
	if n != uint64((len(data)-16)/64) || (len(data)-16)%64 != 0 {
		return fmt.Errorf("profile length")
	}
	p.profiles = make(map[string][3]float64, int(n))
	for start := 16; start < len(data); start += 64 {
		key := string(data[start : start+40])
		if len(key) != 40 || nativeAdmissionAccount(key) != key {
			return fmt.Errorf("profile address")
		}
		if _, ok := p.profiles[key]; ok {
			return fmt.Errorf("duplicate profile")
		}
		var row [3]float64
		for k := range row {
			row[k] = math.Float64frombits(binary.LittleEndian.Uint64(data[start+40+k*8 : start+48+k*8]))
			if !finite(row[k]) || row[k] < 0 {
				return fmt.Errorf("profile value")
			}
		}
		p.profiles[key] = row
	}
	if !finite(p.GlobalMean) || p.GlobalMean < 0 {
		return fmt.Errorf("profile global mean")
	}
	return nil
}

type entryContext struct {
	KnownSender    bool   `json:"known_sender"`
	KnownRecipient bool   `json:"known_recipient"`
	Self           bool   `json:"self_transfer"`
	Novel          int    `json:"novel_distinct_count"`
	PartnerShard   int    `json:"partner_shard"`
	Partner        string `json:"partner_address"`
}

func addEntryFeatures(raw [][]float64, work []int64, c entryContext, profile *[3]float64) {
	for s, x := range raw {
		x[18] = float64(work[s]) / 1000.
		if c.PartnerShard == s {
			x[19] = 1
		}
		if c.KnownSender {
			x[20] = 1
		}
		if c.KnownRecipient {
			x[21] = 1
		}
		if c.Self {
			x[22] = 1
		}
		x[23] = float64(c.Novel)
		if profile == nil {
			x[27] = 1
			x[28] = 1
		} else {
			x[24] = math.Log1p(profile[0] * 3600.)
			x[25] = math.Log1p(profile[1])
			x[26] = math.Log1p(profile[2])
			if profile[1] == 0 {
				x[28] = 1
			}
		}
		if c.PartnerShard >= 0 && c.PartnerShard != s {
			x[29] = 1
		}
	}
}

func newNewFeatures(signals []placementSignal, p *newNewPrior, population []int64, seq, source int64, decision placementClockReading) ([][]float64, error) {
	if decision.ClockID == "" || decision.MonoNS < 0 {
		return nil, fmt.Errorf("invalid decision monotonic clock")
	}
	if p.Cutoff > seq || p.SourceTime > source {
		return nil, fmt.Errorf("future prior at decision")
	}
	total := int64(0)
	for _, n := range population {
		if n < 0 {
			return nil, fmt.Errorf("negative population")
		}
		total += n
	}
	out := make([][]float64, len(signals))
	for s, row := range signals {
		if row.Assigned < row.Confirmed || row.Confirmed < 0 {
			return nil, fmt.Errorf("invalid decision telemetry")
		}
		x := make([]float64, len(newNewFeatureNames))
		out[s] = x
		x[1] = float64(row.Assigned-row.Confirmed) / 1000.
		x[2] = float64(row.BodyLength) / 1000.
		x[3] = 1
		x[5] = 1
		x[6] = 1
		if row.Queue != nil {
			if *row.Queue < 0 || row.ReceivedMonoNS == nil || row.ClockID == "" ||
				row.ClockID != decision.ClockID || *row.ReceivedMonoNS < 0 || *row.ReceivedMonoNS > decision.MonoNS {
				return nil, fmt.Errorf("invalid queue sample")
			}
			x[0] = float64(*row.Queue) / 1000.
			x[4] = float64(decision.MonoNS-*row.ReceivedMonoNS) / 1e9 / 5.
			x[5] = 0
		}
		if len(row.Intervals) > 0 {
			a := append([]float64(nil), row.Intervals...)
			sort.Float64s(a)
			for _, v := range a {
				if !finite(v) || v <= 0 {
					return nil, fmt.Errorf("invalid interval")
				}
			}
			v := a[len(a)/2]
			if len(a)%2 == 0 {
				v = (a[len(a)/2-1] + v) / 2
			}
			x[3] = v / 5.
			x[6] = 0
		}
		if total > 0 {
			x[7] = float64(population[s]) / float64(total)
		}
		for i := 0; i < 3; i++ {
			x[8+i] = math.Log1p(p.Rates[s][i] * 3600.)
		}
		x[11] = p.Persistence[s]
		x[12] = math.Log1p(p.Arrivals[s] * 3600.)
		x[13] = math.Log1p(p.Followup[s] * 3600.)
		x[14] = math.Log1p(p.Mass[s])
		if p.Mass[s] == 0 {
			x[15] = 1
		}
		x[16] = float64(source-p.SourceTime) / 3600.
		x[17] = float64(seq-p.Cutoff) / 1000.
	}
	return out, nil
}

type newNewPlacementPolicy struct {
	priorReceivedMonoNS int64
	mode, modelHash string
	model           *newNewModel
	source          []int64
	base            int64
	mu              sync.RWMutex
	prior           *newNewPrior
	bindingSequence map[string]int64
	share           *sharePlacement
}

func checkedFile(path, expected string) ([]byte, error) {
	if len(expected) != 64 {
		return nil, fmt.Errorf("missing file hash: %s", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != expected {
		return nil, fmt.Errorf("file hash mismatch: %s", path)
	}
	return data, nil
}

func (m *RelayCommitteeModule) configureNewNewPolicy() error {
	mode := os.Getenv("SAA_NEWNEW_POLICY")
	if mode == "" {
		return m.initForestNativeV2()
	}
	if mode != "cumulative" && mode != "pressure" && mode != "ppo" && mode != "pressure_share" && mode != "residual_share" && mode != "association_hash" && !lightModeV2(mode) {
		return fmt.Errorf("unknown new-new policy")
	}
	if !m.nativeBatch || m.placementSignals == nil || m.newNewWork == nil || !m.newNewWork.cumulative || m.groupControl != nil || m.nativeFixedRoutes || os.Getenv("SAA_LIVE_PLAN_URL") != "" {
		return fmt.Errorf("NN policy requires cumulative native accounting, telemetry and no alternate control")
	}
	p := &newNewPlacementPolicy{mode: mode, bindingSequence: map[string]int64{}}
	data, err := checkedFile(os.Getenv("SAA_NEWNEW_SOURCE_TIME_PATH"), os.Getenv("SAA_NEWNEW_SOURCE_TIME_SHA256"))
	if err != nil {
		return err
	}
	if len(data)%8 != 0 || len(data)/8 != m.dataTotalNum {
		return fmt.Errorf("source time scope mismatch")
	}
	p.source = make([]int64, len(data)/8)
	for i := range p.source {
		p.source[i] = int64(binary.LittleEndian.Uint64(data[i*8 : i*8+8]))
		if i > 0 && p.source[i] < p.source[i-1] {
			return fmt.Errorf("nonchronological source time")
		}
	}
	path := os.Getenv("SAA_NEWNEW_PRIOR_PATH")
	p.prior, err = readNewNewPrior(path, len(m.newNewWork.work))
	if err != nil {
		return err
	}
	p.base = p.prior.Cutoff
	p.priorReceivedMonoNS = m.placementSignals.clock.now().MonoNS
	if mode != "pressure_share" && mode != "residual_share" && mode != "association_hash" && !lightModeV2(mode) {
		if err = p.prior.loadProfiles(); err != nil {
			return err
		}
	}
	if p.prior.SourceTime > p.source[0] {
		return fmt.Errorf("seed contains future evidence")
	}
	if mode == "ppo" {
		p.modelHash = os.Getenv("SAA_NEWNEW_MODEL_SHA256")
		data, err = checkedFile(os.Getenv("SAA_NEWNEW_MODEL_PATH"), p.modelHash)
		if err != nil {
			return err
		}
		p.model, err = parseNewNewModel(data)
		if err != nil {
			return err
		}
	}
	if mode == "pressure_share" || mode == "residual_share" || mode == "association_hash" || lightModeV2(mode) {
		p.share, err = newSharePlacement(mode, len(m.newNewWork.work))
		if err != nil {
			return err
		}
		p.modelHash = p.share.modelHash
		m.placementSignals.enableHistory()
	}
	m.newNewPolicy = p
	if err = m.initForestNativeV2(); err != nil {
		return err
	}
	graphOnlyCost := mode == "cost_ranker_v1" && p.share.costModel.ForestEnabled != nil && !*p.share.costModel.ForestEnabled
	if graphOnlyCost && (m.forestFacts != nil || m.forestJoint != nil || os.Getenv("SAA_FOREST_FACTS_V2") != "false" || p.prior.ForestEnabled == nil || *p.prior.ForestEnabled || !p.prior.ForestChannelsDisabled) {
		return fmt.Errorf("GP requires absent forest objects and marked graph-only prior")
	}
	if lightModeV2(mode) && ((!graphOnlyCost && m.forestFacts == nil) || os.Getenv("SAA_PLACEMENT_TELEMETRY_V2") != "true") {
		return fmt.Errorf("light v2 requires forest v2 and independent telemetry")
	}
	go func() {
		for range time.NewTicker(250 * time.Millisecond).C {
			next, e := readNewNewPrior(path, len(m.newNewWork.work))
			if e != nil {
				panic(e)
			}
			p.mu.Lock()
			if next.Version < p.prior.Version || next.Cutoff < p.prior.Cutoff || next.SourceTime < p.prior.SourceTime {
				p.mu.Unlock()
				panic("prior generation regressed")
			}
			if next.Version == p.prior.Version && next.Digest != p.prior.Digest {
				p.mu.Unlock()
				panic("prior generation mutated")
			}
			if next.Version == p.prior.Version {
				p.mu.Unlock()
				continue
			}
			p.mu.Unlock()
			if p.share == nil {
				if e = next.loadProfiles(); e != nil {
					panic(e)
				}
			}
			p.mu.Lock()
			p.prior = next
			p.priorReceivedMonoNS = m.placementSignals.clock.now().MonoNS
			p.mu.Unlock()
		}
	}()
	startupFeatures := newNewFeatureNames
	if lightModeV2(mode) {
		startupFeatures = lightFeatureNamesV2
	}
	if mode == "cost_ranker_v1" {
		startupFeatures = costFeatureNames
	}
	m.nativeAudit.write(map[string]interface{}{"event": "newnew_policy_startup", "clock_schema": placementClockSchema, "clock_id": m.placementSignals.clock.id, "mode": mode, "model_sha256": p.modelHash, "seed_cutoff": p.base, "feature_order": startupFeatures, "inference": "Go local; no per-occurrence RPC"})
	if p.share != nil {
		if mode == "cost_ranker_v1" {
			m.nativeAudit.write(map[string]interface{}{"event": "cost_policy_startup", "schema": costRankerSchema, "mode": mode, "model_sha256": p.modelHash, "feature_order": costFeatureNames, "random_context": shareRandomContext, "random_seed": 43, "max_candidates": 4, "tie_tolerance": costTieTolerance, "candidate_contract_sha256": p.share.costModel.CandidateContractHash, "previous_share_semantics": "previous base pressure proportions, never a learned output", "training_shards": 16, "deployment_shards": len(m.newNewWork.work), "clock_schema": placementClockSchema, "clock_id": m.placementSignals.clock.id})
			return nil
		}
		features := shareFeatureNames
		if lightModeV2(mode) {
			features = lightFeatureNamesV2
		}
		m.nativeAudit.write(map[string]interface{}{"event": "share_policy_startup", "mode": mode, "model_sha256": p.modelHash, "feature_order": features, "random_context": shareRandomContext, "clock_schema": placementClockSchema, "clock_id": m.placementSignals.clock.id})
	}
	return nil
}

func (m *RelayCommitteeModule) chooseNewNewPolicy(cumulative uint64, c entryContext) uint64 {
	began := time.Now()
	p := m.newNewPolicy
	i := m.placementSequence
	if i >= uint64(len(p.source)) {
		panic("placement source occurrence outside scope")
	}
	p.mu.RLock()
	prior := p.prior
	p.mu.RUnlock()
	rows := m.placementSignals.snapshot()
	// Capture the decision clock AFTER copying telemetry: a concurrent report
	// must not appear newer than the clock chosen just before its snapshot lock.
	started := m.placementSignals.clock.now()
	raw, err := newNewFeatures(rows, prior, m.newNewWork.population, p.base+int64(i), p.source[i], started)
	if err != nil {
		panic(err)
	}
	var profile *[3]float64
	if c.PartnerShard >= 0 {
		born, live := p.bindingSequence[c.Partner]
		if !live || born < prior.Cutoff {
			row, ok := prior.profiles[c.Partner]
			if !ok {
				row = [3]float64{0, 0, prior.GlobalMean}
			}
			profile = &row
		}
	}
	addEntryFeatures(raw, m.newNewWork.work, c, profile)
	target := cumulative
	var scores []float64
	if p.mode == "ppo" {
		scores, err = p.model.scores(raw)
		if err != nil {
			panic(err)
		}
		target = 0
		for s := 1; s < len(scores); s++ {
			if scores[s] > scores[target] {
				target = uint64(s)
			}
		}
	} else if p.mode == "pressure" {
		graphSum, forestSum := 0., 0.
		for s := range raw {
			graphSum += prior.Graph[s]
			forestSum += prior.Forest[s]
		}
		risk := make([]float64, len(raw))
		pressure := make([]float64, len(raw))
		for s, row := range rows {
			pressure[s] = float64(row.Assigned - row.Confirmed)
			if row.Queue != nil {
				pressure[s] = math.Max(pressure[s], float64(*row.Queue))
			}
			if graphSum > 0 {
				risk[s] += prior.Graph[s] / graphSum
			}
			if forestSum > 0 {
				risk[s] += prior.Forest[s] / forestSum
			}
		}
		target = 0
		for s := 1; s < len(raw); s++ {
			b := int(target)
			if pressure[s] < pressure[b] || pressure[s] == pressure[b] && (risk[s] < risk[b] || risk[s] == risk[b] && (m.newNewWork.population[s] < m.newNewWork.population[b])) {
				target = uint64(s)
			}
		}
	}
	m.nativeAudit.write(map[string]interface{}{"event": "entry_decision", "clock_schema": placementClockSchema, "clock_id": started.ClockID, "decision_mono_ns": started.MonoNS, "sequence": i, "source_time": p.source[i], "decision_at": started.Wall.UTC(), "context": c, "partner_profile": profile, "signals": rows, "population": append([]int64(nil), m.newNewWork.population...), "cumulative_work": append([]int64(nil), m.newNewWork.work...), "mode": p.mode, "action": target, "cumulative_action": cumulative, "raw_features": raw, "scores": scores, "prior_version": prior.Version, "prior_cutoff": prior.Cutoff, "prior_sha256": prior.Digest, "model_sha256": p.modelHash, "inference_ns": time.Since(began).Nanoseconds()})
	return target
}
