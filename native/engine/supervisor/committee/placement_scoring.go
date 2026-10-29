package committee

import (
	"fmt"
	"math"
	"time"
)

var lightFeatureNamesV2 = []string{
	"visible_pending_slots", "queue_slots", "queue_receive_age_intervals", "queue_missing", "visible_pending_delta_5s", "pending_history_missing",
	"last_nonempty_block_fill", "nonempty_service_mean_intervals", "nonempty_commit_receive_age_intervals", "service_missing",
	"graph_day_log_hour_stage_rate", "graph_source_age_hours", "graph_receive_age_intervals", "graph_missing",
	"new_origins_20s_log_count", "active_origins_20s_log_count", "assigned_nn_half_stages_5s_slots", "family_concentration_20s", "family_concentration_missing", "previous_share",
}

func lightModeV2(mode string) bool {
	return mode == "pressure_share_v2" || mode == "residual_share_v2" || mode == "cost_ranker_v1"
}

type lightGraphSnapshotV2 struct {
	Version        int64     `json:"version"`
	Cutoff         int64     `json:"accepted_cutoff_exclusive"`
	SourceTime     int64     `json:"source_time"`
	SHA256         string    `json:"snapshot_sha256"`
	JointSHA256    string    `json:"joint_sha256"`
	ReceivedMonoNS int64     `json:"received_mono_ns"`
	DayRates       []float64 `json:"day_stage_rates"`
}

func (c *forestJointCacheV2) lightGraphSnapshot() lightGraphSnapshotV2 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return lightGraphSnapshotV2{c.meta.Version, c.meta.Cutoff, c.meta.SourceTime, c.meta.GraphHash, c.digest, c.receivedMonoNS, append([]float64(nil), c.lightDayRates...)}
}

func lightFeaturesV2(rows []placementSignal, observed []lightTelemetryObservationV2, at placementClockReading, past *sharePendingSample, fast ForestFactsSnapshotV2, graph lightGraphSnapshotV2, previous []float64, seq uint64, base, source int64) ([][]float64, error) {
	return lightFeaturesOptionalForest(rows, observed, at, past, &fast, graph, previous, seq, base, source)
}

// A nil forest means unavailable evidence, not a measured empty forest.
func lightFeaturesOptionalForest(rows []placementSignal, observed []lightTelemetryObservationV2, at placementClockReading, past *sharePendingSample, fast *ForestFactsSnapshotV2, graph lightGraphSnapshotV2, previous []float64, seq uint64, base, source int64) ([][]float64, error) {
	n := len(rows)
	if n < 1 || len(observed) != n || len(previous) != n || graph.Cutoff > base+int64(seq) || graph.SourceTime > source || graph.ReceivedMonoNS > at.MonoNS {
		return nil, fmt.Errorf("light v2 causal snapshot mismatch")
	}
	if fast != nil && (len(fast.NewOrigins20s) != n || len(fast.ActiveOrigins20s) != n || len(fast.AssignedNNHalfStages5s) != n || len(fast.Concentration20s) != n || len(fast.NoActivity20s) != n || fast.BoundPrefix != seq || fast.AssignedPrefix != seq || fast.ClockID != at.ClockID || fast.RuntimeMonoNS > at.MonoNS) {
		return nil, fmt.Errorf("light v2 causal forest snapshot mismatch")
	}
	if len(graph.DayRates) != 0 && len(graph.DayRates) != n {
		return nil, fmt.Errorf("light v2 graph topology mismatch")
	}
	out := make([][]float64, n)
	for s, r := range rows {
		if r.Assigned < r.Confirmed || r.Confirmed < 0 {
			return nil, fmt.Errorf("light v2 invalid work")
		}
		x := make([]float64, 20)
		out[s] = x
		x[0] = float64(r.Assigned-r.Confirmed) / 1000.
		x[3] = 1
		x[5] = 1
		x[9] = 1
		x[13] = 1
		v := observed[s]
		if v.Valid {
			if v.ClockID != at.ClockID || v.ReceivedMonoNS > at.MonoNS || v.ReceivedMonoNS < 0 {
				return nil, fmt.Errorf("light v2 cross-clock observation")
			}
			x[1] = float64(v.Report.Queue) / 1000.
			x[2] = float64(at.MonoNS-v.ReceivedMonoNS) / 5e9
			x[3] = 0
			if !v.Report.ServiceMissing {
				if v.ServiceFirstReceivedMonoNS < 0 || v.ServiceFirstReceivedMonoNS > at.MonoNS {
					return nil, fmt.Errorf("light v2 service clock")
				}
				x[6] = v.Report.LastNonemptyFill
				x[7] = v.Report.MeanNonemptyDurationNS / 5e9
				x[8] = float64(at.MonoNS-v.ServiceFirstReceivedMonoNS) / 5e9
				x[9] = 0
			}
		}
		if past != nil {
			if len(past.Pending) != n || past.MonoNS > at.MonoNS-5e9 {
				return nil, fmt.Errorf("light v2 invalid history witness")
			}
			x[4] = float64(r.Assigned-r.Confirmed-past.Pending[s]) / 1000.
			x[5] = 0
		}
		if len(graph.DayRates) == n {
			if !finite(graph.DayRates[s]) || graph.DayRates[s] < 0 {
				return nil, fmt.Errorf("light v2 graph rate")
			}
			x[10] = math.Log1p(graph.DayRates[s] * 3600.)
			x[11] = float64(source-graph.SourceTime) / 3600.
			x[12] = float64(at.MonoNS-graph.ReceivedMonoNS) / 5e9
			x[13] = 0
		}
		x[18] = 1
		if fast != nil {
			x[14] = math.Log1p(float64(fast.NewOrigins20s[s]))
			x[15] = math.Log1p(float64(fast.ActiveOrigins20s[s]))
			x[16] = float64(fast.AssignedNNHalfStages5s[s]) / 2000.
			x[17] = fast.Concentration20s[s]
			if !fast.NoActivity20s[s] { x[18] = 0 }
		}
		x[19] = previous[s]
		for _, v := range x {
			if !finite(v) {
				return nil, fmt.Errorf("light v2 nonfinite feature")
			}
		}
	}
	return out, nil
}

func (m *RelayCommitteeModule) chooseLightShareV2() (uint64, placementClockReading) {
	began := time.Now()
	p, q := m.newNewPolicy, m.newNewPolicy.share
	seq := m.placementSequence
	if m.forestFacts == nil || m.forestJoint == nil || seq >= uint64(len(p.source)) {
		panic("light v2 requires causal forest facts")
	}
	fast, e := m.forestFacts.SnapshotNow(m.placementSignals.clock)
	if e != nil {
		panic(e)
	}
	graph := m.forestJoint.lightGraphSnapshot()
	rows, observed, at, past := m.placementSignals.lightSnapshotV2(q.originNS)
	raw, e := lightFeaturesV2(rows, observed, at, past, fast, graph, q.previous, seq, p.base, p.source[seq])
	if e != nil {
		panic(e)
	}
	latent, delta, base, prob, e := shareProbabilities(raw, q.model)
	if e != nil {
		panic(e)
	}
	u, e := ShareUniform(shareRandomContext, q.ordinal)
	if e != nil {
		panic(e)
	}
	target, e := shareDraw(prob, u)
	if e != nil {
		panic(e)
	}
	m.nativeAudit.write(map[string]interface{}{"event": "light_entry_decision", "mode": p.mode, "sequence": seq, "nn_ordinal": q.ordinal, "clock_schema": placementClockSchema, "clock_id": at.ClockID, "decision_mono_ns": at.MonoNS, "decision_at": at.Wall.UTC(), "runtime_origin_mono_ns": q.originNS, "source_time": p.source[seq], "signals": rows, "telemetry_rows": observed, "pending_history": past, "raw_features": raw, "feature_order": lightFeatureNamesV2, "fast": fast, "prior": graph, "previous_share": append([]float64(nil), q.previous...), "latent_mean": latent, "residual": delta, "base_logits": base, "probabilities": prob, "random_context": shareRandomContext, "u": u, "action": target, "model_sha256": q.modelHash, "inference_ns": time.Since(began).Nanoseconds()})
	q.previous = append([]float64(nil), prob...)
	q.prob = prob
	q.ordinal++
	return target, at
}
