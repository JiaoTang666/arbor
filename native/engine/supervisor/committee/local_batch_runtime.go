package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync/atomic"
	"time"
)

// Only called under flowMu. Routes of initial-ledger accounts are immutable in
// the fixed-placement mode; runtime-created accounts remain on the old path.
func (f *firstAdmissionRuntime) localBatchShard(tx *core.Transaction) (int, bool) {
	m := f.module
	if tx == nil || tx.FirstPlacement != nil || tx.Relayed || tx.HasBroker || m.initialLedger == nil || m.admission == nil {
		return 0, false
	}
	a, b := nativeAdmissionAccount(tx.Sender), nativeAdmissionAccount(tx.Recipient)
	s, sa := m.initialLedger.Routes[a]
	t, sb := m.initialLedger.Routes[b]
	if !sa || !sb || s != t {
		return 0, false
	}
	m.admission.lock.Lock()
	defer m.admission.lock.Unlock()
	if m.admission.softDecisionMode == saaIntegratedDecisionMode || m.admission.dynamicAdmissionEnabled || m.admission.externalActionEnabled || m.admission.mappingBoundaryEnabled {
		return 0, false
	}
	for _, u := range []string{a, b} {
		if actual, ok := m.admission.lookupAssignedShardLocked(u); ok && actual != s {
			return 0, false
		}
	}
	return int(s), true
}

func (f *firstAdmissionRuntime) scheduleLocalBatchesLocked() {
	if !f.localBatchEnabled || f.module.finitePlan.closed() {
		return
	}
	f.mu.Lock()
	held := make(map[string]*core.Transaction, len(f.held))
	for id, tx := range f.held {
		held[id] = tx
	}
	f.mu.Unlock()
	eligible := map[string]int{}
	for id, tx := range held {
		if shard, ok := f.localBatchShard(tx); ok {
			eligible[id] = shard
		}
	}
	groups := f.ledger.TakeReadyLocalBatches(eligible, streamOutstandingLimit)
	shards := []int{}
	for s := range groups {
		shards = append(shards, s)
	}
	sort.Ints(shards)
	for _, s := range shards {
		txs := []*core.Transaction{}
		for _, flight := range groups[s] {
			tx := held[flight.ID]
			if tx == nil {
				panic("local_batch_held_missing")
			}
			txs = append(txs, tx)
		}
		f.sendLocalBatchLocked(s, txs)
	}
}

func (f *firstAdmissionRuntime) sendLocalBatchLocked(shard int, txs []*core.Transaction) {
	if len(txs) == 0 {
		return
	}
	ids := []string{}
	seqs := []uint64{}
	for _, tx := range txs {
		if !f.module.finitePlan.begin("bind", tx.IngressSequence) {
			return
		}
		f.bindFixedRoute(tx, int(atomic.LoadInt64(&f.module.observedEpoch))+1)
		if tx.RoutedSenderShard != uint64(shard) || tx.RoutedRecipientShard != uint64(shard) || !tx.SenderRequiresExistingState || !tx.RecipientRequiresExistingState {
			panic("local_batch_route_or_existing_state_mismatch")
		}
		f.routed(tx)
		ids = append(ids, tx.AdmissionID)
		seqs = append(seqs, tx.IngressSequence)
	}
	for _, tx := range txs {
		if tx.MappingVersion != txs[0].MappingVersion {
			panic("local_batch_version_changed")
		}
	}
	data, e := json.Marshal(message.InjectTxs{Txs: txs, ToShardID: uint64(shard)})
	if e != nil {
		panic(e)
	}
	wire := append(message.MergeMessage(message.CInject, data), '\n')
	hash := sha256.Sum256(wire)
	f.record("local_ordered_batch_bound", map[string]interface{}{"batch_id": ids[0], "ids": ids, "ingress_sequences": seqs, "shard": shard, "mapping_version": txs[0].MappingVersion, "wire_sha256": hex.EncodeToString(hash[:]), "contract": "initial-ledger same-shard; one ordered wire; no outside unfinished predecessor"})
	writer := f.writeOrdered
	if writer == nil {
		writer = writeReleased
	}
	address := f.module.IpNodeTable[uint64(shard)][0]
	go func() {
		for _, tx := range txs {
			if !f.module.finitePlan.begin("write", tx.IngressSequence) {
				return
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e := writer(ctx, address, wire)
		failure := ""
		if e != nil {
			failure = e.Error()
		}
		for _, tx := range txs {
			f.ledger.MarkSend(tx.AdmissionID, failure)
			if e != nil {
				f.record("held_release_send_uncertain", map[string]string{"id": tx.AdmissionID, "error": failure, "batch_id": ids[0]})
				continue
			}
			f.mu.Lock()
			delete(f.held, tx.AdmissionID)
			f.mu.Unlock()
			f.record("held_release_written", map[string]interface{}{"id": tx.AdmissionID, "mapping_version": tx.MappingVersion, "batch_id": ids[0]})
		}
	}()
}
