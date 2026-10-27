package committee

import (
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"blockEmulator/message"
	"blockEmulator/utils"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

type FirstPlacementDispatch struct {
	SourceShard, TargetShard               uint64 // actual bound route, retained even without a later receipt
	RequestID, AdmissionID, Token          string
	MappingVersion                         uint64
	ActivatedAt, BoundAt, WriteCompletedAt time.Time
	TransportWritten                       bool
	Failure                                string
}

func (f *firstAdmissionRuntime) bindFixedRoute(tx *core.Transaction, epoch int) {
	m := f.module.admission
	m.lock.Lock()
	defer m.lock.Unlock()
	f.bindFixedRouteLocked(tx, epoch)
}
func (f *firstAdmissionRuntime) bindFixedRouteLocked(tx *core.Transaction, epoch int) {
	m := f.module.admission
	source, target := uint64(utils.Addr2Shard(nativeAdmissionAccount(tx.Sender))), uint64(utils.Addr2Shard(nativeAdmissionAccount(tx.Recipient)))
	if f.module.initialLedger != nil {
		if s, ok := f.module.initialLedger.Routes[nativeAdmissionAccount(tx.Sender)]; ok {
			source = s
			tx.SenderRequiresExistingState = true
		}
		if s, ok := f.module.initialLedger.Routes[nativeAdmissionAccount(tx.Recipient)]; ok {
			target = s
			tx.RecipientRequiresExistingState = true
		}
	}
	if s, ok := m.lookupAssignedShardLocked(tx.Sender); ok {
		source = s
	}
	if s, ok := m.lookupAssignedShardLocked(tx.Recipient); ok {
		target = s
	}
	tx.RoutingFrozen = true
	tx.MappingVersion = m.routeVersion
	tx.RoutingEpoch = epoch
	tx.RoutedSenderShard = source
	tx.RoutedRecipientShard = target
	f.mu.Lock()
	tx.SenderRequiresExistingState = tx.SenderRequiresExistingState || f.readyAccounts[nativeAdmissionAccount(tx.Sender)]
	tx.RecipientRequiresExistingState = tx.RecipientRequiresExistingState || f.readyAccounts[nativeAdmissionAccount(tx.Recipient)]
	f.mu.Unlock()
}

// ActivateFirstPlacementCreator is an explicit fixed-policy API, not an
// automatic selector. Only the existing runtime admission map is written.
// A full socket write is NOT a native ACK. An ambiguous send is never retried
// automatically; the route/gate remain in place for evidence reconciliation.
func (m *RelayCommitteeModule) ActivateFirstPlacementCreator(ctx context.Context, request, occurrence string) (FirstPlacementDispatch, error) {
	f := m.firstAdmission
	if f == nil || !f.started {
		return FirstPlacementDispatch{}, fmt.Errorf("fence_not_started")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return FirstPlacementDispatch{}, fmt.Errorf("dispatch_deadline_required")
	}
	f.activationMu.Lock()
	defer f.activationMu.Unlock()
	f.mu.Lock()
	if old, ok := f.activations[request]; ok {
		f.mu.Unlock()
		if old.AdmissionID != occurrence {
			return old, fmt.Errorf("creator_already_designated")
		}
		if old.Failure != "" {
			return old, fmt.Errorf("send_uncertain_reconciliation_required: %s", old.Failure)
		}
		return old, nil
	}
	r, ok := f.reservations[request]
	held := f.held[occurrence]
	f.mu.Unlock()
	if !ok || r.Status != firstplacement.Reserved || held == nil {
		return FirstPlacementDispatch{}, fmt.Errorf("reserved_creator_missing")
	}
	tx := core.DecodeTx(held.Encode())
	token := make([]byte, 32)
	if _, e := rand.Read(token); e != nil {
		return FirstPlacementDispatch{}, e
	}
	claims := map[string]uint64{}
	for _, c := range r.Request.Claims {
		claims[c.Account] = uint64(c.TargetShard)
	}
	// Resolve a prospective source for connect only. Recheck everything under
	// the route lock after connect; no activation occurs on connect failure.
	f.bindFixedRoute(tx, int(atomic.LoadInt64(&m.observedEpoch))+1)
	if s, ok := claims[nativeAdmissionAccount(tx.Sender)]; ok {
		tx.RoutedSenderShard = s
	}
	source := tx.RoutedSenderShard
	conn, e := (&net.Dialer{}).DialContext(ctx, "tcp", m.IpNodeTable[source][0])
	if e != nil {
		return FirstPlacementDispatch{}, e
	}
	defer conn.Close()
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-finished:
		}
	}()
	if e = conn.SetWriteDeadline(deadline); e != nil {
		return FirstPlacementDispatch{}, e
	}
	f.flowMu.Lock()
	m.admission.lock.Lock()
	result, wire, e := f.activateCreatorLocked(ctx, r, tx, claims, hex.EncodeToString(token), source)
	m.admission.lock.Unlock()
	f.flowMu.Unlock()
	if e != nil {
		return result, e
	}
	f.record("creator_route_active_bound", result)
	if !m.finitePlan.begin("creator_write", tx.IngressSequence) {
		result.Failure = "finite_cutoff_before_write"
		f.record("creator_not_written_cutoff", result)
		return result, context.DeadlineExceeded
	}
	for len(wire) > 0 {
		var n int
		n, e = conn.Write(wire)
		wire = wire[n:]
		if e != nil || n == 0 {
			if e == nil {
				e = fmt.Errorf("zero_write")
			}
			break
		}
	}
	if e != nil {
		result.Failure = e.Error()
	} else {
		result.TransportWritten = true
		result.WriteCompletedAt = time.Now()
	}
	f.mu.Lock()
	f.activations[request] = result
	f.mu.Unlock()
	f.record("creator_transport_result", result)
	f.ledger.MarkSend(result.AdmissionID, result.Failure)
	return result, e
}

// Caller holds flowMu and the single runtime route lock.
func (f *firstAdmissionRuntime) activateCreatorLocked(ctx context.Context, r firstplacement.Reservation, tx *core.Transaction, claims map[string]uint64, token string, connectedSource uint64) (FirstPlacementDispatch, []byte, error) {
	fail := func(s string) (FirstPlacementDispatch, []byte, error) {
		return FirstPlacementDispatch{}, nil, fmt.Errorf("%s", s)
	}
	m := f.module.admission
	if !f.module.finitePlan.begin("creator_bind", tx.IngressSequence) {
		return fail("finite_cutoff_before_bind")
	}
	now := time.Now()
	epoch := int(atomic.LoadInt64(&f.module.observedEpoch)) + 1
	if e := ctx.Err(); e != nil {
		return fail(e.Error())
	}
	if m.softDecisionMode == saaIntegratedDecisionMode || m.dynamicAdmissionEnabled || m.externalActionEnabled || m.mappingBoundaryEnabled {
		return fail("fence_requires_fixed_mapping")
	}
	if m.routeVersion != r.Request.BaseRouteVersion || m.routeVersion == ^uint64(0) {
		return fail("stale_route_version")
	}
	// Native fixed-placement reservations are protected by the exclusive account
	// lease, generation, route version and bounded evidence age checked below.
	// Unrelated shard progress is not a lease expiry clock. Keep epoch scheduling
	// only for legacy reservations without a native presence generation.
	if r.Request.PresenceGeneration == 0 && (epoch != r.Request.EffectiveEpoch || epoch > r.Request.DeadlineEpoch) {
		return fail("reservation_epoch_expired")
	}
	l, remaining, e := f.ledger.Lease(r.Request.ID)
	if e != nil || remaining != 0 || l.Generation != r.Request.PresenceGeneration || l.Session != r.Request.PresenceSession {
		return fail("reservation_generation_mismatch")
	}
	if r.Request.MaxEvidenceAge <= 0 {
		return fail("invalid_evidence_age")
	}
	for _, p := range r.Request.Proofs {
		if p.ReceivedAt.IsZero() || now.Before(p.ReceivedAt) || now.Sub(p.ReceivedAt) > r.Request.MaxEvidenceAge {
			return fail("presence_evidence_expired")
		}
	}
	for a, s := range claims {
		if s >= m.shardNum {
			return fail("invalid_target")
		}
		if _, exists := m.lookupAssignedShardLocked(a); exists {
			return fail("account_already_mapped")
		}
	}
	f.bindFixedRouteLocked(tx, epoch)
	if s, ok := claims[nativeAdmissionAccount(tx.Sender)]; ok {
		tx.RoutedSenderShard = s
	}
	if s, ok := claims[nativeAdmissionAccount(tx.Recipient)]; ok {
		tx.RoutedRecipientShard = s
	}
	if tx.RoutedSenderShard != connectedSource {
		return fail("source_changed_before_activation")
	}
	tx.MappingVersion = m.routeVersion + 1
	tx.FirstPlacement = &core.FirstPlacementBinding{Session: l.Session, RequestID: l.RequestID, Generation: l.Generation, Token: token, AdmissionID: tx.AdmissionID, TxHash: hex.EncodeToString(tx.TxHash), MappingVersion: tx.MappingVersion, SourceShard: tx.RoutedSenderShard, TargetShard: tx.RoutedRecipientShard, Claims: claims}
	if e := tx.FirstPlacement.Validate(tx, f.session); e != nil {
		return fail(e.Error())
	}
	payload, e := json.Marshal(message.InjectTxs{Txs: []*core.Transaction{tx}, ToShardID: tx.RoutedSenderShard})
	if e != nil {
		return fail(e.Error())
	}
	wire := append(message.MergeMessage(message.CInject, payload), '\n')
	result := FirstPlacementDispatch{SourceShard: tx.RoutedSenderShard, TargetShard: tx.RoutedRecipientShard, RequestID: l.RequestID, AdmissionID: tx.AdmissionID, Token: token, MappingVersion: tx.MappingVersion, ActivatedAt: now, BoundAt: now}
	e = f.ledger.RouteCreator(l.RequestID, tx.AdmissionID, int(tx.RoutedSenderShard), int(tx.RoutedRecipientShard), now, func() error {
		if m.runtimeShardByAccount == nil {
			m.runtimeShardByAccount = map[string]uint64{}
		}
		for a, s := range claims {
			m.runtimeShardByAccount[normalizeAdmissionAddress(a)] = s
		}
		m.routeVersion = tx.MappingVersion
		return nil
	})
	if e != nil {
		return fail(e.Error())
	}
	r.Status = firstplacement.RouteActive
	r.ActiveVersion = tx.MappingVersion
	r.ActivatedAt = now
	f.mu.Lock()
	f.reservations[l.RequestID] = r
	if f.creators == nil {
		f.creators = map[string]*core.Transaction{}
	}
	f.creators[l.RequestID] = core.DecodeTx(tx.Encode())
	delete(f.held, tx.AdmissionID)
	if f.activations == nil {
		f.activations = map[string]FirstPlacementDispatch{}
	}
	f.activations[l.RequestID] = result
	f.mu.Unlock()
	return result, wire, nil
}
