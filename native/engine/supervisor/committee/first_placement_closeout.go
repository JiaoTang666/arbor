package committee

import (
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"blockEmulator/message"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"sync/atomic"
	"time"
)

func validCreationHash(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == 32 }

// Validate the whole receipt set before mutating lifecycle or receipt state.
// The source is a trusted node report. No quorum/signature claim is made.
func (f *firstAdmissionRuntime) validateCreationReceipts(b *message.BlockInfoMsg) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := map[string]core.FirstCreationReceipt{}
	for _, xs := range [][]*core.Transaction{b.InnerShardTxs, b.Relay1Txs, b.Relay2Txs} {
		for _, tx := range xs {
			if tx == nil {
				return fmt.Errorf("nil_report_transaction")
			}
			for _, expected := range f.creators {
				if tx.AdmissionID == expected.AdmissionID && (expected.FirstPlacement.Validate(tx, f.session) != nil || !reflect.DeepEqual(expected.FirstPlacement, tx.FirstPlacement)) {
					return fmt.Errorf("creator_lifecycle_binding_mismatch")
				}
			}
		}
	}
	for _, r := range b.FirstCreationReceipts {
		expected := f.creators[r.Binding.RequestID]
		if expected == nil || !reflect.DeepEqual(expected.FirstPlacement, &r.Binding) {
			return fmt.Errorf("creation_binding_mismatch")
		}
		shard, ok := r.Binding.Claims[r.Account]
		if !ok || shard != r.Shard || r.Shard != b.SenderShardID || r.BlockHeight != uint64(b.Epoch) || r.BlockHeight == 0 || r.BlockHash != b.CommittedBlockHash || r.StateRoot != b.CommittedStateRoot || !validCreationHash(r.BlockHash) || !validCreationHash(r.StateRoot) || !validCreationHash(r.ParentStateRoot) || !validCreationHash(r.StateHash) || r.StateRoot == r.ParentStateRoot || r.ObservedAt.IsZero() || b.CommitTime.IsZero() {
			return fmt.Errorf("creation_commit_metadata_mismatch")
		}
		matches := 0
		for _, xs := range [][]*core.Transaction{b.InnerShardTxs, b.Relay1Txs, b.Relay2Txs} {
			for _, tx := range xs {
				if tx.AdmissionID == r.Binding.AdmissionID {
					if r.Binding.Validate(tx, f.session) != nil || !reflect.DeepEqual(tx.FirstPlacement, &r.Binding) {
						return fmt.Errorf("creation_block_transaction_mismatch")
					}
					matches++
				}
			}
		}
		if matches != 1 {
			return fmt.Errorf("creation_occurrence_not_in_report")
		}
		key := r.Binding.RequestID + "/" + r.Account
		if old, ok := seen[key]; ok && !reflect.DeepEqual(old, r) {
			return fmt.Errorf("conflicting_creation_receipt")
		}
		seen[key] = r
		if old, ok := f.creationReceipts[r.Binding.RequestID][r.Account]; ok && !reflect.DeepEqual(old, r) {
			return fmt.Errorf("conflicting_creation_replay")
		}
	}
	return nil
}

func (f *firstAdmissionRuntime) acceptCreationReceiptsAndRelease(b *message.BlockInfoMsg) error {
	f.mu.Lock()
	if f.creationReceipts == nil {
		f.creationReceipts = map[string]map[string]core.FirstCreationReceipt{}
	}
	for _, r := range b.FirstCreationReceipts {
		if f.creationReceipts[r.Binding.RequestID] == nil {
			f.creationReceipts[r.Binding.RequestID] = map[string]core.FirstCreationReceipt{}
		}
		f.creationReceipts[r.Binding.RequestID][r.Account] = r
	}
	candidates := []string{}
	for id, r := range f.reservations {
		if r.Status != firstplacement.RouteActive {
			continue
		}
		complete := true
		for _, c := range r.Request.Claims {
			if _, ok := f.creationReceipts[id][c.Account]; !ok {
				complete = false
			}
		}
		if complete {
			candidates = append(candidates, id)
		}
	}
	f.mu.Unlock()
	for _, receipt := range b.FirstCreationReceipts {
		f.record("creation_receipt_verified", receipt)
	}
	for _, id := range candidates {
		if !f.ledger.CreatorTerminal(id) {
			continue
		}
		_, e := f.ledger.CompleteCreator(id)
		if e != nil {
			return e
		}
		f.mu.Lock()
		r := f.reservations[id]
		r.Status = firstplacement.CreationCommitted
		if r.Committed == nil {
			r.Committed = map[string]time.Time{}
		}
		if f.readyAccounts == nil {
			f.readyAccounts = map[string]bool{}
		}
		for _, c := range r.Request.Claims {
			f.readyAccounts[c.Account] = true
			r.Committed[c.Account] = time.Now()
		}
		f.reservations[id] = r
		f.mu.Unlock()
		f.record("creation_committed_verified", map[string]interface{}{"request": id, "account_ready": true})
	}
	return nil
}

// Caller holds flowMu only while choosing and binding disjoint queue heads.
// Workers never hold flowMu while waiting for transport. A failed write keeps
// both account positions occupied until genuine terminal evidence arrives.
func (f *firstAdmissionRuntime) scheduleReadyLocked() {
	if f.module.finitePlan.closed() {
		return
	}
	f.scheduleLocalBatchesLocked()
	for _, flight := range f.ledger.TakeReady() {
		f.mu.Lock()
		tx := f.held[flight.ID]
		f.mu.Unlock()
		if tx == nil {
			panic("held_transaction_missing")
		}
		f.sendOrderedLocked(tx)
	}
}

func (f *firstAdmissionRuntime) submitOrdered(txs []*core.Transaction) {
	for _, tx := range txs {
		if !f.admit(tx) {
			continue
		}
		f.flowMu.Lock()
		f.mu.Lock()
		f.held[tx.AdmissionID] = tx
		f.mu.Unlock()
		f.sendOrderedLocked(tx)
		f.flowMu.Unlock()
	}
}

func (f *firstAdmissionRuntime) sendOrderedLocked(tx *core.Transaction) {
	if !f.module.finitePlan.begin("bind", tx.IngressSequence) {
		return
	}
	f.bindFixedRoute(tx, int(atomic.LoadInt64(&f.module.observedEpoch))+1)
	f.routed(tx)
	data, e := json.Marshal(message.InjectTxs{Txs: []*core.Transaction{tx}, ToShardID: tx.RoutedSenderShard})
	if e != nil {
		panic(e)
	}
	wire := append(message.MergeMessage(message.CInject, data), '\n')
	address := f.module.IpNodeTable[tx.RoutedSenderShard][0]
	writer := f.writeOrdered
	if writer == nil {
		writer = writeReleased
	}
	go func() {
		if !f.module.finitePlan.begin("write", tx.IngressSequence) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e := writer(ctx, address, wire)
		failure := ""
		if e != nil {
			failure = e.Error()
		}
		f.ledger.MarkSend(tx.AdmissionID, failure)
		if e != nil {
			f.record("held_release_send_uncertain", map[string]string{"id": tx.AdmissionID, "error": failure})
			return
		}
		f.mu.Lock()
		delete(f.held, tx.AdmissionID)
		f.mu.Unlock()
		f.record("held_release_written", map[string]interface{}{"id": tx.AdmissionID, "mapping_version": tx.MappingVersion})
		if f.ordinaryPipeline {
			f.flowMu.Lock()
			f.scheduleReadyLocked()
			f.flowMu.Unlock()
		}
	}()
}

func writeReleased(ctx context.Context, address string, wire []byte) error {
	c, e := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if e != nil {
		return e
	}
	defer c.Close()
	deadline, _ := ctx.Deadline()
	if e = c.SetWriteDeadline(deadline); e != nil {
		return e
	}
	for len(wire) > 0 {
		n, e := c.Write(wire)
		if e != nil {
			return e
		}
		if n == 0 {
			return fmt.Errorf("zero_write")
		}
		wire = wire[n:]
	}
	return nil
}

// Explicit harness/control API; uses the same admission and report hooks as
// normal RawRelay workload processing. Does not select accounts or targets.
func (m *RelayCommitteeModule) SubmitFencedTransactions(txs []*core.Transaction) error {
	if m.firstAdmission == nil || !m.firstAdmission.started {
		return fmt.Errorf("fence_not_started")
	}
	m.txSending(txs)
	return nil
}
func (m *RelayCommitteeModule) ProcessFirstPlacementBlockReport(b *message.BlockInfoMsg) error {
	if m.firstAdmission == nil || !m.firstAdmission.started {
		return fmt.Errorf("fence_not_started")
	}
	m.firstAdmission.report(b)
	return nil
}
func (m *RelayCommitteeModule) FirstPlacementProgress(id string) (firstplacement.Reservation, int, error) {
	f := m.firstAdmission
	if f == nil {
		return firstplacement.Reservation{}, 0, fmt.Errorf("fence_not_started")
	}
	f.mu.Lock()
	r, ok := f.reservations[id]
	raw, _ := json.Marshal(r)
	f.mu.Unlock()
	if !ok {
		return r, 0, fmt.Errorf("request_missing")
	}
	json.Unmarshal(raw, &r)
	return r, f.ledger.HeldCount(), nil
}
