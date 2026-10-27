package committee

import (
	"fmt"
	"os"
	"time"
)

type sharePendingSample struct {
	MonoNS  int64   `json:"mono_ns"`
	Pending []int64 `json:"pending"`
}

func (p *placementSignals) enableHistory() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.historyOn = true
	p.rememberSharePending(p.clock.now().MonoNS)
}

// Optional policy-history sampling; it does not own work evidence or its clock.
// Called with the telemetry lock held and the current mutation timestamp.
func (p *placementSignals) rememberSharePending(at int64) {
	if !p.historyOn {
		return
	}
	values := make([]int64, len(p.rows))
	for s, r := range p.rows {
		values[s] = r.Assigned - r.Confirmed
	}
	n := len(p.history)
	sample := sharePendingSample{at, values}
	if n > 0 && p.history[n-1].MonoNS == at {
		p.history[n-1] = sample
	} else {
		p.history = append(p.history, sample)
	}
	for p.historyHead+1 < len(p.history) && p.history[p.historyHead+1].MonoNS < at-20*int64(time.Second) {
		p.historyHead++
	}
	if p.historyHead > 4096 && p.historyHead*2 > len(p.history) {
		p.history = append([]sharePendingSample(nil), p.history[p.historyHead:]...)
		p.historyHead = 0
	}
}

// Copy decision state and its two causal history witnesses under one lock.
func (p *placementSignals) shareSnapshot(origin int64) ([]placementSignal, placementClockReading, [2]*sharePendingSample) {
	p.mu.Lock()
	defer p.mu.Unlock()
	at := p.clock.now()
	rows := append([]placementSignal(nil), p.rows...)
	for s := range rows {
		rows[s].Intervals = append([]float64(nil), rows[s].Intervals...)
		if rows[s].Queue != nil {
			v := *rows[s].Queue
			rows[s].Queue = &v
		}
		if rows[s].ReceivedMonoNS != nil {
			v := *rows[s].ReceivedMonoNS
			rows[s].ReceivedMonoNS = &v
		}
	}
	var past [2]*sharePendingSample
	for j, seconds := range []int64{5, 20} {
		boundary := at.MonoNS - seconds*int64(time.Second)
		if origin < 0 || boundary < origin {
			continue
		}
		for k := len(p.history) - 1; k >= p.historyHead; k-- {
			if p.history[k].MonoNS <= boundary {
				x := p.history[k]
				x.Pending = append([]int64(nil), x.Pending...)
				past[j] = &x
				break
			}
		}
	}
	return rows, at, past
}

type shareTimedShard struct {
	at    int64
	shard uint64
}
type shareExposure struct {
	counts                           [][4]int64
	young, middle, stages            []shareTimedShard
	youngHead, middleHead, stageHead int
	now                              int64
}

func (e *shareExposure) advance(at int64) {
	if at < e.now {
		panic("share exposure clock regressed")
	}
	e.now = at
	for e.youngHead < len(e.young) && e.young[e.youngHead].at <= at-5*int64(time.Second) {
		x := e.young[e.youngHead]
		e.youngHead++
		e.counts[x.shard][0]--
		e.counts[x.shard][1]++
		e.middle = append(e.middle, x)
	}
	for e.middleHead < len(e.middle) && e.middle[e.middleHead].at < at-20*int64(time.Second) {
		x := e.middle[e.middleHead]
		e.middleHead++
		e.counts[x.shard][1]--
		e.counts[x.shard][2]++
	}
	for e.stageHead < len(e.stages) && e.stages[e.stageHead].at < at-5*int64(time.Second) {
		x := e.stages[e.stageHead]
		e.stageHead++
		e.counts[x.shard][3]--
	}
	// Bound retained evidence without scanning active families on decisions.
	compact := func(a *[]shareTimedShard, h *int) {
		if *h > 4096 && *h*2 > len(*a) {
			*a = append([]shareTimedShard(nil), (*a)[*h:]...)
			*h = 0
		}
	}
	compact(&e.young, &e.youngHead)
	compact(&e.middle, &e.middleHead)
	compact(&e.stages, &e.stageHead)
}
func (e *shareExposure) birth(at int64, s uint64) {
	e.advance(at)
	e.young = append(e.young, shareTimedShard{at, s})
	e.counts[s][0]++
}
func (e *shareExposure) work(at int64, a, b uint64) {
	e.advance(at)
	e.stages = append(e.stages, shareTimedShard{at, a})
	e.counts[a][3]++
	if b != a {
		e.stages = append(e.stages, shareTimedShard{at, b})
		e.counts[b][3]++
	}
}

type sharePlacement struct {
	costModel      *costRankerModel
	model          *shareModel
	modelHash      string
	originNS       int64
	batch          int64
	ordinal        uint64
	previous, prob []float64
	families       map[string]uint64 // Only NN-origin accounts; historical origins are absent.
	exposure       shareExposure
}

func newSharePlacement(mode string, shards int) (*sharePlacement, error) {
	if shards < 1 {
		return nil, fmt.Errorf("share policy requires positive shard count")
	}
	// The frozen cost ranker applies the same 20 coefficients independently
	// to each deployment shard. Legacy residual artifacts remain 16-shard
	// policies; topology support here must not silently widen their contract.
	if mode != "cost_ranker_v1" && shards != 16 {
		return nil, fmt.Errorf("share policy requires 16 shards")
	}
	p := &sharePlacement{originNS: -1, batch: -1, previous: make([]float64, shards), families: map[string]uint64{}, exposure: shareExposure{counts: make([][4]int64, shards)}}
	for s := range p.previous {
		p.previous[s] = 1 / float64(shards)
	}
	if mode == "cost_ranker_v1" {
		p.modelHash = os.Getenv("SAA_NEWNEW_MODEL_SHA256")
		data, e := checkedFile(os.Getenv("SAA_NEWNEW_MODEL_PATH"), p.modelHash)
		if e != nil {
			return nil, e
		}
		p.costModel, e = parseCostRankerModel(data)
		if e != nil {
			return nil, e
		}
		if e = validateSelfEvaluationModel(p.costModel, p.modelHash); e != nil {
			return nil, e
		}
	} else if mode == "residual_share" || mode == "residual_share_v2" {
		p.modelHash = os.Getenv("SAA_NEWNEW_MODEL_SHA256")
		data, e := checkedFile(os.Getenv("SAA_NEWNEW_MODEL_PATH"), p.modelHash)
		if e != nil {
			return nil, e
		}
		p.model, e = parseShareModel(data)
		if e != nil {
			return nil, e
		}
		if lightModeV2(mode) != (p.model.Schema == "entry_share_residual_v2") {
			return nil, fmt.Errorf("share mode/model semantic version mismatch")
		}
	} else if os.Getenv("SAA_NEWNEW_MODEL_PATH") != "" || os.Getenv("SAA_NEWNEW_MODEL_SHA256") != "" {
		return nil, fmt.Errorf("pressure_share cannot load model")
	}
	return p, nil
}

func (m *RelayCommitteeModule) chooseSharePolicy() (uint64, placementClockReading) {
	if m.newNewPolicy.mode == "cost_ranker_v1" {
		return m.chooseCostRankerPolicy()
	}
	if lightModeV2(m.newNewPolicy.mode) {
		return m.chooseLightShareV2()
	}
	began := time.Now()
	p := m.newNewPolicy
	q := p.share
	seq := m.placementSequence
	batch := int64(seq / 1000)
	if seq >= uint64(len(p.source)) {
		panic("share sequence exceeds source")
	}
	var at placementClockReading
	if q.batch != batch {
		rows, decision, history := m.placementSignals.shareSnapshot(q.originNS)
		at = decision
		p.mu.RLock()
		prior := p.prior
		p.mu.RUnlock()
		base, e := newNewFeatures(rows, prior, m.newNewWork.population, p.base+int64(seq), p.source[seq], at)
		if e != nil {
			panic(e)
		}
		q.exposure.advance(at.MonoNS)
		raw := make([][]float64, len(rows))
		for s, row := range rows {
			raw[s] = make([]float64, 28)
			copy(raw[s], base[s][:19])
			raw[s][18] = float64(m.newNewWork.work[s]) / 1000.
			for j, h := range history {
				if h != nil {
					raw[s][19+j] = float64(row.Assigned-row.Confirmed-h.Pending[s]) / 1000.
					raw[s][21+j] = 1
				}
			}
			for j, n := range q.exposure.counts[s] {
				raw[s][23+j] = float64(n)
			}
			raw[s][27] = q.previous[s]
		}
		latent, delta, baseLogits, prob, e := shareProbabilities(raw, q.model)
		if e != nil {
			panic(e)
		}
		m.nativeAudit.write(map[string]interface{}{"event": "share_batch_decision", "mode": p.mode, "batch": batch, "sequence": seq, "clock_schema": placementClockSchema, "clock_id": at.ClockID, "decision_mono_ns": at.MonoNS, "decision_at": at.Wall.UTC(), "runtime_origin_mono_ns": q.originNS, "source_time": p.source[seq], "signals": rows, "history_samples": history, "raw_features": raw, "previous_share": append([]float64(nil), q.previous...), "population": append([]int64(nil), m.newNewWork.population...), "cumulative_work": append([]int64(nil), m.newNewWork.work...), "prior_version": prior.Version, "prior_cutoff": prior.Cutoff, "prior_sha256": prior.Digest, "latent_mean": latent, "residual": delta, "base_logits": baseLogits, "probabilities": prob, "model_sha256": q.modelHash, "inference_ns": time.Since(began).Nanoseconds()})
		q.previous = append([]float64(nil), prob...)
		q.prob = prob
		q.batch = batch
	} else {
		at = m.placementSignals.clock.now()
	}
	u, e := ShareUniform(shareRandomContext, q.ordinal)
	if e != nil {
		panic(e)
	}
	target, e := shareDraw(q.prob, u)
	if e != nil {
		panic(e)
	}
	m.nativeAudit.write(map[string]interface{}{"event": "share_placement", "sequence": seq, "batch": batch, "nn_ordinal": q.ordinal, "random_context": shareRandomContext, "u": u, "action": target, "clock_id": at.ClockID, "binding_mono_ns": at.MonoNS, "model_sha256": q.modelHash})
	q.ordinal++
	return target, at
}

func (m *RelayCommitteeModule) recordSharePlacement(a, b string, ka, kb bool, sa, sb uint64, at placementClockReading) {
	if lightModeV2(m.newNewPolicy.mode) {
		m.nativeAudit.write(map[string]interface{}{"event": "share_dispatch", "sequence": m.placementSequence, "sender": a, "recipient": b, "known_sender": ka, "known_recipient": kb, "source_shard": sa, "target_shard": sb, "clock_id": at.ClockID, "dispatch_mono_ns": at.MonoNS, "runtime_origin_mono_ns": m.newNewPolicy.share.originNS})
		return
	}
	p := m.newNewPolicy.share
	p.exposure.advance(at.MonoNS)
	if !ka && !kb {
		family := m.placementSequence + 1
		p.families[a] = family
		p.families[b] = family
		p.exposure.birth(at.MonoNS, sa)
	} else if !ka {
		if f, ok := p.families[b]; ok {
			p.families[a] = f
		}
	} else if !kb {
		if f, ok := p.families[a]; ok {
			p.families[b] = f
		}
	}
	fa, fb := p.families[a], p.families[b]
	if fa > 0 || fb > 0 {
		p.exposure.work(at.MonoNS, sa, sb)
	}
	// Every transaction has a process-local timestamp for independent past-5s replay.
	m.nativeAudit.write(map[string]interface{}{"event": "share_dispatch", "sequence": m.placementSequence, "sender": a, "recipient": b, "known_sender": ka, "known_recipient": kb, "source_shard": sa, "target_shard": sb, "sender_family": fa, "recipient_family": fb, "clock_id": at.ClockID, "dispatch_mono_ns": at.MonoNS, "runtime_origin_mono_ns": p.originNS})
}
