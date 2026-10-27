package committee

import "fmt"

// Only the exclusive, fresh, fixed-route stream adapter may issue this proof.
// Called by the sole producer after EOF/declared count and final fence drain.
func (m *RelayCommitteeModule) confirmStreamUntouchedShards() error {
	f := m.firstAdmission
	if !m.streamFirstPlacement || m.initialLedger == nil || f == nil || !f.started || m.Ss == nil || m.admission == nil || m.dataTotalNum <= 0 || m.nowDataNum != m.dataTotalNum || len(m.streamPending) != 0 {
		return fmt.Errorf("stream_completion_unproven")
	}
	f.flowMu.Lock()
	defer f.flowMu.Unlock()
	touched, e := f.ledger.CompletedRouteShards(m.dataTotalNum)
	if e != nil {
		return e
	}
	seen := map[int]bool{}
	shards := int(m.admission.shardNum)
	for _, s := range touched {
		if s < 0 || s >= shards {
			return fmt.Errorf("invalid_completed_route")
		}
		seen[s] = true
	}
	untouched := []uint64{}
	for s := 0; s < shards; s++ {
		if !seen[s] {
			untouched = append(untouched, uint64(s))
		}
	}
	if e = m.Ss.ConfirmUntouchedAfterDrainedInput(untouched); e != nil {
		return e
	}
	f.record("stream_untouched_shards_confirmed", map[string]interface{}{"source_count": m.dataTotalNum, "touched": touched, "untouched": untouched, "basis": "exclusive input exhausted; all occurrences terminal; no creation owner; both frozen endpoints audited", "trust": "existing trusted supervisor/node reports, not Byzantine certificate"})
	return nil
}
