package committee

import (
	"fmt"
	"math"
	"time"
)

// Counts come from the same causal work/report witnesses as the model input;
// avoid reconstructing integer pressure through normalized feature slots.
func costBasePressure(rows []placementSignal, observed []lightTelemetryObservationV2) ([]float64, []float64, error) {
	if len(rows) < 1 || len(observed) != len(rows) {
		return nil, nil, fmt.Errorf("cost pressure dimensions")
	}
	pressure, p := make([]float64, len(rows)), make([]float64, len(rows))
	peak := math.Inf(-1)
	for s, r := range rows {
		if r.Assigned < r.Confirmed || r.Confirmed < 0 {
			return nil, nil, fmt.Errorf("cost pressure work invalid")
		}
		pressure[s] = float64(r.Assigned - r.Confirmed)
		if observed[s].Valid {
			if observed[s].Report.Queue < 0 {
				return nil, nil, fmt.Errorf("cost queue invalid")
			}
			pressure[s] = math.Max(pressure[s], float64(observed[s].Report.Queue))
		}
		p[s] = -pressure[s] / 1000.
		peak = math.Max(peak, p[s])
	}
	total := 0.
	for s, v := range p {
		p[s] = math.Exp(v - peak)
		total += p[s]
	}
	for s := range p {
		p[s] /= total
	}
	return pressure, p, nil
}

func (m *RelayCommitteeModule) chooseCostRankerPolicy() (uint64, placementClockReading) {
	began := time.Now()
	p, q := m.newNewPolicy, m.newNewPolicy.share
	seq := m.placementSequence
	if q.costModel == nil || seq >= uint64(len(p.source)) {
		panic("cost ranker missing frozen model or causal facts")
	}
	var fast *ForestFactsSnapshotV2
	var graph lightGraphSnapshotV2
	if q.costModel.ForestEnabled != nil && !*q.costModel.ForestEnabled {
		if m.forestFacts != nil || m.forestJoint != nil { panic("GP constructed forest facts") }
		graph = m.graphOnlyCostSnapshot()
	} else {
		if m.forestFacts == nil || m.forestJoint == nil { panic("Full requires forest facts") }
		value, err := m.forestFacts.SnapshotNow(m.placementSignals.clock)
		if err != nil { panic(err) }
		fast = &value
		graph = m.forestJoint.lightGraphSnapshot()
	}
	rows, observed, at, past := m.placementSignals.lightSnapshotV2(q.originNS)
	raw, e := lightFeaturesOptionalForest(rows, observed, at, past, fast, graph, q.previous, seq, p.base, p.source[seq])
	if e != nil {
		panic(e)
	}
	pressure, base, e := costBasePressure(rows, observed)
	if e != nil {
		panic(e)
	}
	candidates, uniforms, e := sampleCostCandidates(base, shareRandomContext, q.ordinal)
	if e != nil {
		panic(e)
	}
	costs, e := q.costModel.scores(raw, candidates)
	if e != nil {
		panic(e)
	}
	target, e := chooseCostCandidate(candidates, costs)
	if e != nil {
		panic(e)
	}
	r4 := candidates[0]
	for _, s := range candidates[1:] {
		if pressure[s] < pressure[r4] {
			r4 = s
		}
	}
	previous := append([]float64(nil), q.previous...)
	m.nativeAudit.write(map[string]interface{}{"event": "cost_entry_decision", "schema": costRankerSchema, "mode": p.mode, "sequence": seq, "nn_ordinal": q.ordinal, "clock_schema": placementClockSchema, "clock_id": at.ClockID, "decision_mono_ns": at.MonoNS, "decision_at": at.Wall.UTC(), "runtime_origin_mono_ns": q.originNS, "source_time": p.source[seq], "signals": rows, "telemetry_rows": observed, "pending_history": past, "raw_features": raw, "feature_order": costFeatureNames, "fast": fast, "prior": graph, "previous_share": previous, "previous_pressure_share": previous, "pressure": pressure, "base_proportions": base, "probabilities": base, "candidate_ids": candidates, "candidate_uniforms": uniforms, "candidate_costs": costs, "action": target, "pfirst_target": candidates[0], "r4_target": r4, "random_context": shareRandomContext, "random_seed": 43, "model_sha256": q.modelHash, "candidate_contract_sha256": q.costModel.CandidateContractHash, "inference_ns": time.Since(began).Nanoseconds()})
	q.previous = append([]float64(nil), base...)
	q.prob = append([]float64(nil), base...)
	q.ordinal++
	return target, at
}
