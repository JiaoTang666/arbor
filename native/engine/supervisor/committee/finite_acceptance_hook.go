package committee

import (
	"os"
	"sync/atomic"
	"time"
)

// Only suppresses entry consumption between production calls. Never blocks
// consensus, receipt handlers, or takes any routing/ledger lock while waiting.
// A creator already executing is not cancelled or rewound.
type finiteAcceptanceHook struct{ active atomic.Bool }

func (m *RelayCommitteeModule) startFiniteAcceptanceHook(origin time.Time) {
	if os.Getenv("SAA_FINITE_ACCEPTANCE_V1") != "true" || m.firstAdmission == nil {
		return
	}
	h := &finiteAcceptanceHook{}
	m.finiteHook = h
	f := m.firstAdmission
	f.record("finite_plan_origin", map[string]interface{}{"origin": origin, "pause_after_ns": int64(2 * time.Second), "pause_duration_ns": int64(2 * time.Second)})
	go func() {
		timer := time.NewTimer(time.Until(origin.Add(2 * time.Second)))
		defer timer.Stop()
		<-timer.C
		h.active.Store(true)
		f.record("finite_pause_start", map[string]interface{}{"relative_ns": time.Since(origin).Nanoseconds(), "boundary": "before entry consumption; existing in-progress operation not cancelled"})
		timer.Reset(time.Until(origin.Add(4 * time.Second)))
		<-timer.C
		h.active.Store(false)
		f.record("finite_pause_end", map[string]interface{}{"relative_ns": time.Since(origin).Nanoseconds()})
	}()
}
