package committee

import (
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"blockEmulator/utils"
	"fmt"
	"time"
)

// Only the paced source producer calls these methods. Historical routes and
// already activated creator routes are relationship locations, not absence proofs.
func (m *RelayCommitteeModule) streamClaims(tx *core.Transaction) []firstplacement.Claim {
	a, b := nativeAdmissionAccount(tx.Sender), nativeAdmissionAccount(tx.Recipient)
	m.admission.lock.Lock()
	defer m.admission.lock.Unlock()
	lookup := func(a string) (uint64, bool) {
		if s, ok := m.initialLedger.Routes[a]; ok {
			return s, true
		}
		return m.admission.lookupAssignedShardLocked(a)
	}
	sa, ka := lookup(a)
	sb, kb := lookup(b)
	if ka && kb {
		return nil
	}
	target := uint64(utils.Addr2Shard(a))
	if ka {
		target = sa
	} else if kb {
		target = sb
	}
	claims := []firstplacement.Claim{}
	if !ka {
		claims = append(claims, firstplacement.Claim{Account: a, TargetShard: int(target)})
	}
	if !kb && b != a {
		claims = append(claims, firstplacement.Claim{Account: b, TargetShard: int(target)})
	}
	return claims
}

const streamOutstandingLimit = 256

func (m *RelayCommitteeModule) streamEntryOccupancy() int {
	f := m.firstAdmission
	if f.ordinaryPipeline {
		return len(m.streamPending) + f.ledger.PendingDispatch()
	}
	return len(m.streamPending) + f.ledger.Outstanding()
}

// Shared endpoints retain source order, including dependencies through skipped
// entries. Only disjoint entries may bypass an earlier pending occurrence.
func streamReadyIndex(txs []*core.Transaction, ready func(*core.Transaction) bool) int {
	blocked := map[string]bool{}
	for i, tx := range txs {
		a, b := nativeAdmissionAccount(tx.Sender), nativeAdmissionAccount(tx.Recipient)
		if !blocked[a] && !blocked[b] && ready(tx) {
			return i
		}
		blocked[a] = true
		blocked[b] = true
	}
	return -1
}

func (m *RelayCommitteeModule) pumpFirstPlacementStream() {
	if m.finitePlan.closed() {
		return
	}
	if m.finiteHook != nil && m.finiteHook.active.Load() {
		return
	}
	f := m.firstAdmission
	for {
		if m.finitePlan.closed() {
			return
		}
		if m.finiteHook != nil && m.finiteHook.active.Load() {
			return
		}
		i := streamReadyIndex(m.streamPending, func(tx *core.Transaction) bool {
			return len(m.streamClaims(tx)) == 0 || f.ledger.Idle([]string{nativeAdmissionAccount(tx.Sender), nativeAdmissionAccount(tx.Recipient)})
		})
		if i < 0 {
			return
		}
		tx := m.streamPending[i]
		f.record("stream_queue_selected", map[string]interface{}{"ingress_sequence": tx.IngressSequence, "bypassed": i, "pending": len(m.streamPending)})
		m.executeFirstPlacementStream(tx)
		if m.finitePlan.closed() {
			return
		}
		copy(m.streamPending[i:], m.streamPending[i+1:])
		m.streamPending[len(m.streamPending)-1] = nil
		m.streamPending = m.streamPending[:len(m.streamPending)-1]
	}
}

func (m *RelayCommitteeModule) submitFirstPlacementStream(txs []*core.Transaction) {
	f := m.firstAdmission
	if f == nil || !f.started || m.initialLedger == nil {
		panic("stream entry not initialized")
	}
	for _, tx := range txs {
		if m.finitePlan.closed() {
			return
		}
		deadline := time.Now().Add(120 * time.Second)
		backpressure := false
		var blockedAt time.Time
		for m.streamEntryOccupancy() >= streamOutstandingLimit {
			if m.finitePlan.closed() {
				return
			}
			if !backpressure {
				blockedAt = time.Now()
				hookActive := m.finiteHook != nil && m.finiteHook.active.Load()
				f.record("stream_capacity_backpressure", map[string]interface{}{"ingress_sequence": tx.IngressSequence, "limit": streamOutstandingLimit, "occupancy": m.streamEntryOccupancy(), "hook_active": hookActive})
				backpressure = true
			}
			m.pumpFirstPlacementStream()
			if m.finitePlan == nil && time.Now().After(deadline) {
				panic("stream capacity timeout; no retry or forced release")
			}
			time.Sleep(5 * time.Millisecond)
		}
		if backpressure {
			f.record("stream_capacity_resumed", map[string]interface{}{"ingress_sequence": tx.IngressSequence, "blocked_ns": time.Since(blockedAt).Nanoseconds(), "occupancy": m.streamEntryOccupancy()})
		}
		if m.finitePlan.closed() {
			return
		}
		m.streamPending = append(m.streamPending, tx)
		f.record("stream_queued", map[string]interface{}{"ingress_sequence": tx.IngressSequence, "pending": len(m.streamPending), "outstanding": f.ledger.Outstanding(), "entry_occupancy": m.streamEntryOccupancy(), "ordinary_pipeline": f.ordinaryPipeline, "limit": streamOutstandingLimit, "boundary": "pre-admission queue; pipelined capacity excludes successfully written in-flight work"})
		m.pumpFirstPlacementStream()
	}
}

func (m *RelayCommitteeModule) executeFirstPlacementStream(tx *core.Transaction) {
	f := m.firstAdmission
	if f == nil || !f.started || m.initialLedger == nil {
		panic("stream entry not initialized")
	}
	claims := m.streamClaims(tx)
	if len(claims) == 0 {
		f.submitOrdered([]*core.Transaction{tx})
		return
	}
	request := fmt.Sprintf("stream-%d", tx.IngressSequence)
	f.record("stream_entry_selected", map[string]interface{}{"request": request, "ingress_sequence": tx.IngressSequence, "claims": claims, "rule": "one-new follows known; both-new sender hash", "status": "location proposal, not native absence proof"})
	ctx, cancel := m.finitePlan.context()
	var e error
	if !f.ledger.Idle([]string{nativeAdmissionAccount(tx.Sender), nativeAdmissionAccount(tx.Recipient)}) {
		e = fmt.Errorf("stream creator unexpectedly busy")
	}
	if e == nil {
		f.record("stream_creator_dependencies_drained", map[string]interface{}{"request": request, "ingress_sequence": tx.IngressSequence})
		_, e = m.VerifyAndReserveFirstPlacement(ctx, request, claims)
	}
	if e == nil {
		f.submitOrdered([]*core.Transaction{tx})
		if m.finitePlan.closed() {
			cancel()
			return
		}
		_, e = m.ActivateFirstPlacementCreator(ctx, request, tx.AdmissionID)
	}
	cancel()
	if e != nil {
		if m.finitePlan.closed() {
			return
		}
		f.record("stream_entry_failed_no_retry", map[string]interface{}{"request": request, "error": e.Error()})
		panic(e)
	}
}
func (m *RelayCommitteeModule) drainFirstPlacementStream() {
	ctx, cancel := m.finitePlan.context()
	defer cancel()
	for len(m.streamPending) > 0 {
		if m.finitePlan.closed() {
			return
		}
		if e := ctx.Err(); e != nil {
			panic(fmt.Errorf("stream pending drain: %w", e))
		}
		m.pumpFirstPlacementStream()
		if len(m.streamPending) > 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if e := m.firstAdmission.ledger.WaitIdle(ctx, nil); e != nil {
		if m.finitePlan.closed() {
			return
		}
		panic(fmt.Errorf("stream final drain: %w", e))
	}
	if e := m.confirmStreamUntouchedShards(); e != nil {
		panic(e)
	}
	m.firstAdmission.record("stream_all_terminal", map[string]interface{}{"source_count": m.nowDataNum})
}
