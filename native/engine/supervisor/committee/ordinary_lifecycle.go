package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type ordinaryLifecycle struct {
	Tx                                    ordinaryRouteSnapshot
	Routed, Source, Target, Done, Written bool
	AcceptedAt                            time.Time
}

// Keep exactly the immutable values checked by auditBlock. Audit does not
// execute a transaction and needs neither its value/storage nor a gob clone.
// Value fields avoid aliasing mutable transaction pointers after binding.
type ordinaryRouteSnapshot struct {
	Sender, Recipient                       string
	MappingVersion                          uint64
	RoutingEpoch                            int
	RoutedSenderShard, RoutedRecipientShard uint64
}

func snapshotOrdinaryRoute(tx *core.Transaction) ordinaryRouteSnapshot {
	return ordinaryRouteSnapshot{
		Sender: tx.Sender, Recipient: tx.Recipient,
		MappingVersion: tx.MappingVersion, RoutingEpoch: tx.RoutingEpoch,
		RoutedSenderShard: tx.RoutedSenderShard, RoutedRecipientShard: tx.RoutedRecipientShard,
	}
}

func occurrenceKey(tx *core.Transaction) string {
	return fmt.Sprintf("%d/%s", tx.IngressSequence, hex.EncodeToString(tx.TxHash))
}

// Caller holds lifecycleMu. JSONL timestamps use the supervisor clock; commit
// timestamps are evidence, not interchangeable with receipt-registration time.
func (m *RelayCommitteeModule) lifecycleEvent(tx *core.Transaction, event string, at time.Time, extra interface{}) {
	if m.nativeAudit != nil {
		row := map[string]interface{}{"schema": "occurrence_lifecycle_v1", "event": event, "at": at.UTC(), "occurrence": occurrenceKey(tx), "ingress_sequence": tx.IngressSequence, "tx_hash": hex.EncodeToString(tx.TxHash), "admission_id": tx.AdmissionID, "details": extra}
		if os.Getenv("BLOCK_EMULATOR_BASELINE_CLOCK_V1") == "1" && !m.nativeClock.IsZero() {
			row["native_elapsed_ns"] = at.Sub(m.nativeClock).Nanoseconds()
			row["clock_instance"] = fmt.Sprintf("supervisor/%d/%d", os.Getpid(), m.nativeClock.UnixNano())
		}
		m.nativeAudit.write(row)
		if event == "route_bound" || event == "completed" {
			x := m.lifecycle[occurrenceKey(tx)]
			var accepted time.Time
			if x != nil {
				accepted = x.AcceptedAt
			}
			m.nativeAudit.writeObserver(tx, event, accepted)
		}
		return
	}
	f, e := os.OpenFile(m.offeredAuditPath+".lifecycle.v1.jsonl", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if e != nil {
		panic(e)
	}
	e = json.NewEncoder(f).Encode(map[string]interface{}{"schema": "occurrence_lifecycle_v1", "event": event, "at": at.UTC(),
		"occurrence": occurrenceKey(tx), "ingress_sequence": tx.IngressSequence, "tx_hash": hex.EncodeToString(tx.TxHash),
		"admission_id": tx.AdmissionID, "details": extra})
	ce := f.Close()
	if e != nil {
		panic(e)
	}
	if ce != nil {
		panic(ce)
	}
}

func (m *RelayCommitteeModule) auditAccepted(tx *core.Transaction) {
	if m.offeredAuditPath == "" {
		return
	}
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.lifecycle == nil {
		m.lifecycle = map[string]*ordinaryLifecycle{}
	}
	key := occurrenceKey(tx)
	if _, ok := m.lifecycle[key]; ok {
		panic("duplicate ingress occurrence in lifecycle audit")
	}
	at := time.Now()
	m.lifecycle[key] = &ordinaryLifecycle{Tx: snapshotOrdinaryRoute(tx), AcceptedAt: at}
	m.lifecycleEvent(tx, "accepted", at, "ordinary supervisor accepted occurrence before route selection")
}

func (m *RelayCommitteeModule) auditRoute(tx *core.Transaction) {
	if m.offeredAuditPath == "" {
		return
	}
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	x := m.lifecycle[occurrenceKey(tx)]
	if x == nil {
		panic("route before acceptance")
	}
	x.Tx = snapshotOrdinaryRoute(tx)
	x.Routed = true
	m.bindForestFacts(tx)
	details := map[string]interface{}{"source": tx.RoutedSenderShard, "target": tx.RoutedRecipientShard, "version": tx.MappingVersion, "epoch": tx.RoutingEpoch}
	if m.placementSignals != nil {
		if at := m.placementSignals.assign(tx.RoutedSenderShard, tx.RoutedRecipientShard); at >= 0 {
			details["assigned_mono_ns"], details["clock_id"] = at, m.placementSignals.clock.id
			if m.forestFacts != nil {
				if e := m.forestFacts.Assign(tx.IngressSequence, hex.EncodeToString(tx.TxHash), nativeAdmissionAccount(tx.Sender), nativeAdmissionAccount(tx.Recipient), tx.RoutedSenderShard, tx.RoutedRecipientShard, at); e != nil {
					panic(e)
				}
			}
		}
	}
	m.lifecycleEvent(tx, "route_bound", time.Now(), details)
}

func (m *RelayCommitteeModule) auditTransport(txs []*core.Transaction, event string, at time.Time, err error) {
	for _, tx := range txs {
		m.auditTransportOne(tx, len(txs), event, at, err)
	}
}
func (m *RelayCommitteeModule) auditTransportOne(tx *core.Transaction, count int, event string, at time.Time, err error) {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	x := m.lifecycle[occurrenceKey(tx)]
	if x == nil {
		return
	}
	if event == "first_socket_write" {
		if x.Written {
			return
		}
		x.Written = true
	}
	detail := ""
	if err != nil {
		detail = err.Error()
	}
	m.lifecycleEvent(tx, event, at, map[string]interface{}{"error": detail, "message_tx_count": count, "boundary": "first positive socket write of enclosing injection message; not remote receipt or full payload completion"})
}

func (m *RelayCommitteeModule) auditFenceJoin(tx *core.Transaction) {
	if m.offeredAuditPath == "" {
		return
	}
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	m.lifecycleEvent(tx, "fence_admission_join", time.Now(), "join to existing fence ledger by admission_id; not a replacement acceptance timestamp")
}

func (m *RelayCommitteeModule) auditBlock(b *message.BlockInfoMsg) {
	if m.offeredAuditPath == "" || b == nil {
		return
	}
	began := time.Now()
	var wait, hold, maxHold int64
	count := 0
	for _, group := range []struct {
		leg string
		txs []*core.Transaction
	}{{"inner", b.InnerShardTxs}, {"relay1", b.Relay1Txs}, {"relay2", b.Relay2Txs}} {
		for _, tx := range group.txs {
			w, h := m.auditBlockLeg(b, tx, group.leg)
			wait += w
			hold += h
			if h > maxHold {
				maxHold = h
			}
			count++
		}
	}
	if m.nativeAudit != nil && m.nativeBatch {
		m.nativeAudit.write(map[string]interface{}{"event": "audit_receipt_batch", "shard": b.SenderShardID, "height": b.Epoch, "count": count, "elapsed_ns": time.Since(began).Nanoseconds(), "lock_wait_ns": wait, "lock_hold_sum_ns": hold, "max_leg_lock_hold_ns": maxHold})
	}
}

// One validated receipt, not the entire block, is the atomic state transition.
func (m *RelayCommitteeModule) auditBlockLeg(b *message.BlockInfoMsg, tx *core.Transaction, leg string) (wait, hold int64) {
	begin := time.Now()
	m.lifecycleMu.Lock()
	locked := time.Now()
	wait = locked.Sub(begin).Nanoseconds()
	defer func() { hold = time.Since(locked).Nanoseconds(); m.lifecycleMu.Unlock() }()
	if tx == nil {
		return
	}
	x := m.lifecycle[occurrenceKey(tx)]
	valid := x != nil && x.Routed && tx.RoutingFrozen && !tx.HasBroker && b.Epoch > 0 && !b.CommitTime.IsZero()
	if valid {
		a := &x.Tx
		valid = tx.Sender == a.Sender && tx.Recipient == a.Recipient && tx.MappingVersion == a.MappingVersion && tx.RoutingEpoch == a.RoutingEpoch && tx.RoutedSenderShard == a.RoutedSenderShard && tx.RoutedRecipientShard == a.RoutedRecipientShard
	}
	if valid {
		same := x.Tx.RoutedSenderShard == x.Tx.RoutedRecipientShard
		valid = (leg == "inner" && same && b.SenderShardID == x.Tx.RoutedSenderShard) ||
			(leg == "relay1" && !same && b.SenderShardID == x.Tx.RoutedSenderShard) ||
			(leg == "relay2" && !same && b.SenderShardID == x.Tx.RoutedRecipientShard)
	}
	if !valid {
		m.lifecycleEvent(tx, "report_rejected", time.Now(), leg)
		return
	}
	if x.Done {
		return
	}
	duplicate := (leg == "relay1" && x.Source) || (leg == "relay2" && x.Target)
	if duplicate {
		return
	}
	if leg == "inner" {
		x.Source = true
		x.Target = true
	} else if leg == "relay1" {
		x.Source = true
	} else {
		x.Target = true
	}
	details := map[string]interface{}{"leg": leg, "shard": b.SenderShardID, "height": b.Epoch, "commit_at": b.CommitTime}
	if m.placementSignals != nil {
		if at := m.placementSignals.commit(b.SenderShardID); at >= 0 {
			details["confirmed_mono_ns"], details["clock_id"] = at, m.placementSignals.clock.id
			if m.forestFacts != nil {
				if _, e := m.forestFacts.Confirm(tx.IngressSequence, hex.EncodeToString(tx.TxHash), leg, b.SenderShardID, at); e != nil {
					panic(e)
				}
			}
		}
	}
	m.lifecycleEvent(tx, "leg_registered", time.Now(), details)
	if x.Source && x.Target {
		x.Done = true
		m.lifecycleEvent(tx, "completed", time.Now(), "all required native legs registered at supervisor; trusted reporter, not quorum proof")
	}
	return
}
