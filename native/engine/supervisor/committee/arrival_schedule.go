package committee

import (
	"blockEmulator/core"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"
)

// Single workload-producer clock. Batch boundaries and control stalls never
// reset its origin. Late arrivals catch up and are explicitly recorded.
// This is admission attempt time, NOT socket dispatch or successful acceptance.
type arrivalSchedule struct {
	start time.Time
	count int64
	last  time.Time
	now   func() time.Time
	sleep func(time.Duration)
}

func (s *arrivalSchedule) next(rate float64) (int64, time.Time, time.Time) {
	if s.now == nil {
		s.now = time.Now
	}
	if s.sleep == nil {
		s.sleep = time.Sleep
	}
	if s.start.IsZero() {
		s.start = s.now()
	}
	ordinal := s.count
	offset := time.Duration(float64(ordinal) / rate * float64(time.Second))
	// Exact arithmetic for integral rates; retain legacy fractional-rate behavior.
	if rate >= 1 && rate <= float64(time.Second) && rate == math.Trunc(rate) {
		r := int64(rate)
		offset = time.Duration(ordinal/r)*time.Second + time.Duration((ordinal%r)*int64(time.Second)/r)
	}
	planned := s.start.Add(offset)
	eligible := planned
	// No unlimited catch-up burst: at most one attempt per nominal interval.
	if !s.last.IsZero() {
		floor := s.last.Add(time.Duration(math.Ceil(float64(time.Second) / rate)))
		if floor.After(eligible) {
			eligible = floor
		}
	}
	for remaining := eligible.Sub(s.now()); remaining > 0; remaining = eligible.Sub(s.now()) {
		s.sleep(remaining)
	}
	actual := s.now()
	s.last = actual
	s.count++
	return ordinal, planned, actual
}

func (m *RelayCommitteeModule) submitScheduled(txs []*core.Transaction, deliver func([]*core.Transaction)) {
	if m.arrivalClock == nil {
		m.arrivalClock = &arrivalSchedule{}
	}
	// Reject retries, gaps and reordering before any member of the batch enters
	// admission. Transport retries must not re-enter this source-occurrence API.
	for i, tx := range txs {
		if tx == nil || tx.IngressSequence != uint64(m.arrivalClock.count+int64(i)) {
			panic("source sequence must be contiguous, zero-based and submitted once")
		}
	}
	for _, tx := range txs {
		if m.finitePlan.closed() {
			return
		}
		if m.finitePlan != nil && m.arrivalClock.count >= int64(m.finitePlan.totalCount()) {
			panic("finite source total exceeded")
		}
		ordinal, planned, actual := m.arrivalClock.next(m.offeredLoadTPS)
		if ordinal == 0 {
			m.finitePlan.start(m.arrivalClock.start)
			m.startFiniteAcceptanceHook(m.arrivalClock.start)
		}
		if m.finitePlan.closed() {
			return
		}
		m.recordArrival(tx, ordinal, planned, actual)
		// Finite diagnostic contract: stop rather than silently drop or unleash
		// an unlimited catch-up burst. Record this attempt, but do not admit it.
		due := int64(math.Floor(actual.Sub(m.arrivalClock.start).Seconds()*m.offeredLoadTPS)) + 1
		if m.dataTotalNum > 0 && due > int64(m.dataTotalNum) {
			due = int64(m.dataTotalNum)
		}
		m.finitePlan.observe(due, ordinal)
		deliver([]*core.Transaction{tx})
	}
}

func (m *RelayCommitteeModule) recordArrival(tx *core.Transaction, ordinal int64, planned, actual time.Time) {
	if m.offeredAuditPath == "" {
		return
	}
	// Separate versioned sidecar; never mix new admission records into old CSV.
	path := m.offeredAuditPath + ".arrivals.v1.jsonl"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("arrival audit open: %v", err))
	}
	err = json.NewEncoder(f).Encode(map[string]interface{}{
		"schema": "pre_admission_arrival_v2", "ordinal": ordinal,
		"planned_total": m.dataTotalNum, "plan_origin": m.arrivalClock.start.UTC(),
		"catch_up": "no burst; minimum inter-attempt gap=1/target_tps", "max_unadmitted_debt": 10000,
		"ingress_sequence": tx.IngressSequence, "tx_hash": hex.EncodeToString(tx.TxHash),
		"planned_arrival": planned.UTC(), "admission_attempt_at": actual.UTC(),
		"lateness_ns": actual.Sub(planned).Nanoseconds(), "target_tps": m.offeredLoadTPS,
		"first_placement":    m.firstAdmission != nil,
		"epoch_wait_enabled": m.epochAwareDispatchEnabled,
		"boundary":           "before admission, HOLD and route selection; not acceptance or dispatch",
	})
	closeErr := f.Close()
	if err != nil {
		panic(fmt.Sprintf("arrival audit write: %v", err))
	}
	if closeErr != nil {
		panic(fmt.Sprintf("arrival audit close: %v", closeErr))
	}
}
