package committee

import (
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"blockEmulator/message"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Opt-in single-supervisor RawRelay protocol preparation. Begins only on fresh
// chains. Only explicit creator activation may release one HOLD occurrence.
type firstAdmissionRuntime struct {
	localBatchEnabled bool // explicit default-off experiment adapter
	ordinaryPipeline  bool
	appliedFloors     map[string]map[int]uint64                   // flowMu; trusted report positions
	writeOrdered      func(context.Context, string, []byte) error // optional deterministic transport seam
	creators          map[string]*core.Transaction
	creationReceipts  map[string]map[string]core.FirstCreationReceipt
	readyAccounts     map[string]bool
	activationMu      sync.Mutex
	flowMu            sync.Mutex
	activations       map[string]FirstPlacementDispatch
	mu                sync.Mutex
	module            *RelayCommitteeModule
	ledger            *firstplacement.Fence
	session           string
	started           bool
	audit             *os.File
	held              map[string]*core.Transaction
	reservations      map[string]firstplacement.Reservation
}

func newFirstAdmissionRuntime(m *RelayCommitteeModule) *firstAdmissionRuntime {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		panic(err)
	}
	return &firstAdmissionRuntime{module: m, session: hex.EncodeToString(id), ordinaryPipeline: os.Getenv("SAA_LEGACY_SERIAL_ADMISSION") != "true", localBatchEnabled: os.Getenv("SAA_LEGACY_SERIAL_ADMISSION") == "true" && os.Getenv("SAA_ORDERED_LOCAL_BATCH_V1") == "true", held: map[string]*core.Transaction{}, reservations: map[string]firstplacement.Reservation{}}
}
func (f *firstAdmissionRuntime) record(event string, payload interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.audit == nil {
		return
	} // tests may use an explicit in-memory harness
	if err := json.NewEncoder(f.audit).Encode(map[string]interface{}{"event": event, "received_at": time.Now(), "session": f.session, "data": payload}); err != nil {
		panic(fmt.Errorf("first admission audit: %w", err))
	}
}
func (f *firstAdmissionRuntime) start() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return f.startWithContext(ctx)
}
func (f *firstAdmissionRuntime) startWithContext(ctx context.Context) error {
	m := f.module
	// A changing legacy mapping would introduce another writer during absence
	// verification. This stage supports a fixed initial mapping only.
	if m.admission == nil || m.admission.softDecisionMode == saaIntegratedDecisionMode || m.admission.dynamicAdmissionEnabled || m.admission.externalActionEnabled || m.admission.mappingBoundaryEnabled {
		return fmt.Errorf("fence_requires_fixed_mapping")
	}
	f.mu.Lock()
	if f.started {
		f.mu.Unlock()
		return nil
	}
	f.mu.Unlock()
	path := os.Getenv("SAA_FIRST_PLACEMENT_AUDIT")
	if path == "" {
		return fmt.Errorf("first_placement_audit_path_required")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	f.audit = file
	shards := int(m.admission.shardNum)
	for sid := 0; sid < shards; sid++ {
		q := firstplacement.PresenceQuery{Session: f.session, Shard: sid, Hello: true}
		r, e := firstplacement.QueryPresence(ctx, m.IpNodeTable[uint64(sid)][0], q)
		f.record("startup_presence", r)
		if e != nil {
			return e
		}
		if r.Status != "READY" {
			return fmt.Errorf("fresh startup refused: shard=%d reason=%s", sid, r.Failure)
		}
	}
	f.mu.Lock()
	f.ledger = firstplacement.NewFence(f.session)
	if f.ordinaryPipeline {
		f.ledger = firstplacement.NewPipelinedFence(f.session)
	}
	f.started = true
	f.mu.Unlock()
	f.record("admission_started", map[string]interface{}{"shards": shards, "recovery": "fresh_run_only"})
	return nil
}

// May be called before workload reading to establish the same admission
// registry used by MsgSendingControl. No accounts are automatically selected.
func (m *RelayCommitteeModule) StartFirstPlacementFence(ctx context.Context) error {
	if m.firstAdmission == nil {
		return fmt.Errorf("fence_not_enabled")
	}
	return m.firstAdmission.startWithContext(ctx)
}
func nativeAdmissionAccount(s string) string {
	return strings.TrimPrefix(normalizeAdmissionAddress(s), "0x")
}
func (f *firstAdmissionRuntime) admit(tx *core.Transaction) bool {
	f.flowMu.Lock()
	defer f.flowMu.Unlock()
	if !f.module.finitePlan.begin("admit", tx.IngressSequence) {
		return false
	}
	if !f.started || f.ledger == nil {
		panic("admission fence not started before routing")
	}
	if tx.AdmissionID != "" || tx.Relayed || tx.HasBroker {
		panic("fence requires fresh raw ingress occurrence")
	}
	x, err := f.ledger.Admit(nativeAdmissionAccount(tx.Sender), nativeAdmissionAccount(tx.Recipient), time.Now())
	if err != nil {
		panic(err)
	}
	tx.AdmissionID = x.ID
	f.module.auditFenceJoin(tx)
	if x.Held {
		f.mu.Lock()
		f.held[x.ID] = core.DecodeTx(tx.Encode())
		f.mu.Unlock()
	}
	f.record("admission", x)
	return !x.Held
}
func (f *firstAdmissionRuntime) routed(tx *core.Transaction) {
	if err := f.ledger.Route(tx.AdmissionID, int(tx.RoutedSenderShard), int(tx.RoutedRecipientShard), time.Now()); err != nil {
		panic(err)
	}
	f.record("route_bound_before_send", map[string]interface{}{"id": tx.AdmissionID, "tx_hash": hex.EncodeToString(tx.TxHash), "version": tx.MappingVersion, "source": tx.RoutedSenderShard, "target": tx.RoutedRecipientShard})
}
func (f *firstAdmissionRuntime) report(b *message.BlockInfoMsg) {
	if b == nil {
		return
	}
	f.flowMu.Lock()
	defer f.flowMu.Unlock()
	if err := f.validateCreationReceipts(b); err != nil {
		panic(err)
	}
	for leg, txs := range map[string][]*core.Transaction{"inner": b.InnerShardTxs, "relay1": b.Relay1Txs, "relay2": b.Relay2Txs} {
		for _, tx := range txs {
			if tx == nil || tx.AdmissionID == "" {
				panic("unregistered block occurrence in fenced run")
			}
			if err := f.ledger.Report(tx.AdmissionID, nativeAdmissionAccount(tx.Sender), nativeAdmissionAccount(tx.Recipient), int(tx.RoutedSenderShard), int(tx.RoutedRecipientShard), int(b.SenderShardID), leg, b.CommitTime); err != nil {
				panic(err)
			}
			f.record("block_lifecycle", map[string]interface{}{"id": tx.AdmissionID, "leg": leg, "shard": b.SenderShardID, "height": b.Epoch, "commit_at": b.CommitTime})
			if b.Epoch > 0 {
				if f.appliedFloors == nil {
					f.appliedFloors = map[string]map[int]uint64{}
				}
				for _, a := range []string{nativeAdmissionAccount(tx.Sender), nativeAdmissionAccount(tx.Recipient)} {
					if f.appliedFloors[a] == nil {
						f.appliedFloors[a] = map[int]uint64{}
					}
					if uint64(b.Epoch) > f.appliedFloors[a][int(b.SenderShardID)] {
						f.appliedFloors[a][int(b.SenderShardID)] = uint64(b.Epoch)
					}
				}
			}
		}
	}
	if err := f.acceptCreationReceiptsAndRelease(b); err != nil {
		panic(err)
	}
	f.scheduleReadyLocked()
}

// VerifyAndReserveFirstPlacement stops after producing a RESERVED record. The
// caller supplies a fixed target; release requires the explicit activation API.
func (m *RelayCommitteeModule) VerifyAndReserveFirstPlacement(ctx context.Context, id string, claims []firstplacement.Claim) (firstplacement.Reservation, error) {
	f := m.firstAdmission
	if f == nil || !f.started {
		return firstplacement.Reservation{}, fmt.Errorf("fence_not_started")
	}
	if _, ok := ctx.Deadline(); !ok {
		return firstplacement.Reservation{}, fmt.Errorf("query_deadline_required")
	}
	accounts := make([]string, len(claims))
	for i, c := range claims {
		accounts[i] = c.Account
	}
	l, err := f.ledger.Begin(id, accounts, time.Now())
	if err != nil {
		return firstplacement.Reservation{}, err
	}
	f.record("gate_installed", l)
	l, err = f.ledger.WaitDrained(ctx, id)
	if err != nil {
		f.record("drain_failed_hold_retained", err.Error())
		return firstplacement.Reservation{}, err
	}
	f.record("drained_query_started", l)
	shards := int(m.admission.shardNum)
	type result struct {
		q firstplacement.PresenceQuery
		r firstplacement.PresenceResponse
		e error
	}
	responses := make(chan result, shards*len(accounts))
	f.flowMu.Lock()
	floors := map[string]map[int]uint64{}
	for _, a := range accounts {
		floors[a] = map[int]uint64{}
		for s, h := range f.appliedFloors[a] {
			floors[a][s] = h
		}
	}
	f.flowMu.Unlock()
	f.record("presence_required_applied_floors", floors)
	for _, a := range accounts {
		for s := 0; s < shards; s++ {
			q := firstplacement.PresenceQuery{Session: l.Session, RequestID: id, Generation: l.Generation, Account: a, Shard: s}
			q.MinAppliedHeight = floors[a][s]
			address := m.IpNodeTable[uint64(s)][0]
			go func() { r, e := firstplacement.QueryPresence(ctx, address, q); responses <- result{q, r, e} }()
		}
	}
	proofs := []firstplacement.PresenceResponse{}
	var firstErr error
	for i := 0; i < shards*len(accounts); i++ {
		received := <-responses
		if received.e != nil {
			f.record("presence_query_failed", map[string]interface{}{"query": received.q, "error": received.e.Error(), "action": "retain_hold; no absence proof"})
		} else {
			f.record("presence_response", received.r)
		}
		if received.e != nil && firstErr == nil {
			firstErr = received.e
		}
		proofs = append(proofs, received.r)
	}
	if firstErr != nil {
		f.record("query_failed_hold_retained", firstErr.Error())
		return firstplacement.Reservation{}, firstErr
	}
	if err = firstplacement.ValidateProofSet(l, shards, proofs); err != nil {
		f.record("proofs_rejected_hold_retained", err.Error())
		return firstplacement.Reservation{}, err
	}
	if err = ctx.Err(); err != nil {
		return firstplacement.Reservation{}, err
	}
	// Adapt verified native results to the existing prototype Reserve contract.
	// This temporary validator only reserves; it never activates or writes routes.
	m.admission.lock.Lock()
	defer m.admission.lock.Unlock()
	routes := map[string]int{}
	for a, s := range m.admission.stableShardByAccount {
		routes[nativeAdmissionAccount(a)] = int(s)
	}
	validator := firstplacement.NewAuthority(shards, routes, m.admission.routeVersion)
	epoch := int(atomic.LoadInt64(&m.observedEpoch))
	req := firstplacement.ReservationRequest{ID: id, PresenceSession: l.Session, PresenceGeneration: l.Generation, BaseRouteVersion: m.admission.routeVersion, EffectiveEpoch: epoch + 1, DeadlineEpoch: epoch + 1, SubmittedAt: time.Now(), MaxEvidenceAge: 30 * time.Second, Claims: claims}
	for _, p := range proofs {
		req.Proofs = append(req.Proofs, firstplacement.PresenceProof{Account: p.Query.Account, Shard: p.Query.Shard, Session: p.Query.Session, RequestID: id, Generation: l.Generation, CommittedHeight: p.Height, StateRoot: p.StateRoot, Status: p.Status, ObservedAt: p.ObservedAt, ReceivedAt: p.ReceivedAt})
	}
	r := validator.Reserve(req, epoch)
	f.record("reservation_result", r)
	if r.Status != firstplacement.Reserved {
		return r, fmt.Errorf("reservation rejected: %s", r.Reason)
	}
	f.mu.Lock()
	encoded, _ := json.Marshal(r)
	var saved firstplacement.Reservation
	if err := json.Unmarshal(encoded, &saved); err != nil {
		panic(err)
	}
	f.reservations[id] = saved
	f.mu.Unlock()
	return r, nil
}
