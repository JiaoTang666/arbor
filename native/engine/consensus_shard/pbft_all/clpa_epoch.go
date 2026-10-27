package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// This is an opt-in, fresh-run trusted-transport simulator protocol. Its
// evidence files describe committed installs; they are not restart recovery
// records, signed certificates, or an atomic cross-shard storage transaction.
// Fixed leaders make the intentional barrier pause distinguishable from PBFT
// work; disabling the watchdog here is a scope constraint, not security hardening.
type clpaEpochRuntime struct {
	mu                         sync.Mutex
	epoch, rounds, limit       uint64
	started, draining, resumed bool
	plan                       *message.CLPAEpochPlan
	input                      epochPrefix
	relays                     map[uint64]*epochPrefix
	ready                      map[uint64]message.CLPAEpochReady
	transfers                  map[uint64]message.CLPAEpochTransfer
	sentReady, sentTransfers   bool
	installedHash              string
	admissions                 map[string]string
	asyncPipeline              bool
	sourcePending              []*core.Transaction // Immutable leader objects sealed with the source manifest.
}

type epochPrefix struct {
	prefix  uint64
	seen    map[uint64]string
	pending map[uint64][]byte
}

// Arrival order is not application order: both transport and handlers are
// concurrent. Preserve a contiguous applied prefix and reject conflicting
// duplicates, including duplicates received after an earlier gap closes.
func (q *epochPrefix) accept(sequence uint64, raw []byte, apply func([]byte) error) error {
	if sequence == 0 || sequence > q.prefix+65536 {
		return fmt.Errorf("epoch sequence outside bounded receive window")
	}
	h := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(h[:])
	if q.seen == nil {
		q.seen = map[uint64]string{}
		q.pending = map[uint64][]byte{}
	}
	if old, ok := q.seen[sequence]; ok {
		if old != fingerprint {
			return fmt.Errorf("conflicting epoch batch replay")
		}
		return nil
	}
	q.seen[sequence] = fingerprint
	q.pending[sequence] = append([]byte(nil), raw...)
	for {
		next, ok := q.pending[q.prefix+1]
		if !ok {
			break
		}
		if err := apply(next); err != nil {
			return err
		}
		delete(q.pending, q.prefix+1)
		q.prefix++
	}
	return nil
}

func newCLPAEpochRuntime() *clpaEpochRuntime {
	if os.Getenv("BLOCK_EMULATOR_CLPA_EPOCH_V1") != "1" {
		return nil
	}
	limit := uint64(20)
	if raw := os.Getenv("BLOCK_EMULATOR_CLPA_EPOCH_ROUNDS"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || parsed == 0 {
			panic("invalid CLPA epoch round count")
		}
		limit = parsed
	}
	return &clpaEpochRuntime{limit: limit, resumed: true, relays: map[uint64]*epochPrefix{}, ready: map[uint64]message.CLPAEpochReady{}, transfers: map[uint64]message.CLPAEpochTransfer{}, admissions: map[string]string{}, asyncPipeline: os.Getenv("BLOCK_EMULATOR_CLPA_ASYNC_PIPELINE") == "1"}
}

func (e *clpaEpochRuntime) ordinaryAllowedLocked() bool {
	return e.plan == nil && e.resumed && e.started && (e.asyncPipeline || e.draining || e.rounds < e.limit)
}

func (e *clpaEpochRuntime) shouldPropose() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.plan != nil || (e.resumed && (e.started || e.draining))
}

func epochJSON(value interface{}) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}

func epochFingerprint(value interface{}) string {
	digest := sha256.Sum256(epochJSON(value))
	return hex.EncodeToString(digest[:])
}

func (n *PbftConsensusNode) epochSend(kind message.MessageType, value interface{}, address string) {
	n.sendEpochFrame(message.MergeMessage(kind, epochJSON(value)), address)
}

func epochTxValid(tx *core.Transaction, relay bool) error {
	if tx == nil || tx.AdmissionID == "" || tx.Relayed != relay || tx.RoutingFrozen || tx.HasBroker || tx.SenderIsBroker || tx.OriginalSender != "" || tx.FinalRecipient != "" || tx.Value == nil || tx.Value.Sign() < 0 {
		return fmt.Errorf("invalid CLPA epoch transaction or unsupported frozen/broker route")
	}
	for _, account := range []string{tx.Sender, tx.Recipient} {
		decoded, err := hex.DecodeString(account)
		if err != nil || len(decoded) != 20 || hex.EncodeToString(decoded) != account {
			return fmt.Errorf("noncanonical epoch account")
		}
	}
	return nil
}

func epochTxOwner(tx *core.Transaction, owner func(string) uint64) uint64 {
	if tx.Relayed {
		return owner(tx.Recipient)
	}
	return owner(tx.Sender)
}

func (e *clpaEpochRuntime) admit(n *PbftConsensusNode, txs []*core.Transaction, relay bool) error {
	for _, tx := range txs {
		if err := epochTxValid(tx, relay); err != nil {
			return err
		}
		if e.asyncPipeline {
			if err := epochAsyncTxValid(tx); err != nil {
				return err
			}
			// Pool admission initializes a missing arrival timestamp. Bind that
			// timestamp before caching the immutable pending-leg fingerprint.
			if tx.Time.IsZero() {
				tx.Time = time.Now()
			}
		}
		if epochTxOwner(tx, n.CurChain.Get_PartitionMap) != n.ShardID {
			return fmt.Errorf("epoch ingress routed to wrong owner")
		}
		key := fmt.Sprintf("%s/%t", tx.AdmissionID, tx.Relayed)
		fingerprint := epochFingerprint(tx)
		if prior, ok := e.admissions[key]; ok {
			if prior != fingerprint {
				return fmt.Errorf("conflicting epoch admission leg")
			}
			return fmt.Errorf("admission leg repeated in different epoch batches")
		}
		e.admissions[key] = fingerprint
	}
	n.CurChain.Txpool.AddTxs2Pool(txs)
	n.noteWorkloadArrival(len(txs), "clpa_epoch")
	return nil
}

func (c *CLPARelayOutsideModule) handleEpochMessage(kind message.MessageType, raw []byte) error {
	e, n := c.epoch, c.pbftNode
	e.mu.Lock()
	defer e.mu.Unlock()
	switch kind {
	case message.CCLPAEpochInput:
		var in message.CLPAEpochInput
		if err := json.Unmarshal(raw, &in); err != nil {
			return err
		}
		if in.Epoch < e.epoch {
			return nil
		}
		if in.Epoch != e.epoch || in.ToShard != n.ShardID {
			return fmt.Errorf("unexpected epoch input identity")
		}
		if e.plan != nil && in.Batch > e.plan.InputCuts[n.ShardID] {
			return fmt.Errorf("input exceeds closed epoch prefix")
		}
		e.started = true
		return e.input.accept(in.Batch, epochJSON(in), func(b []byte) error {
			var ordered message.CLPAEpochInput
			if err := json.Unmarshal(b, &ordered); err != nil {
				return err
			}
			return e.admit(n, ordered.Txs, false)
		})
	case message.CCLPAEpochRelay:
		var in message.CLPAEpochRelay
		if err := json.Unmarshal(raw, &in); err != nil {
			return err
		}
		if in.Epoch < e.epoch {
			return nil
		}
		if in.Epoch != e.epoch || in.FromShard == n.ShardID || in.FromShard >= n.pbftChainConfig.ShardNums {
			return fmt.Errorf("unexpected epoch relay identity")
		}
		q := e.relays[in.FromShard]
		if q == nil {
			q = &epochPrefix{}
			e.relays[in.FromShard] = q
		}
		if r, ok := e.ready[in.FromShard]; ok && in.Sequence > r.RelayCut {
			return fmt.Errorf("relay exceeds closed epoch prefix")
		}
		return q.accept(in.Sequence, epochJSON(in), func(b []byte) error {
			var ordered message.CLPAEpochRelay
			if err := json.Unmarshal(b, &ordered); err != nil {
				return err
			}
			return e.admit(n, ordered.Txs, true)
		})
	case message.CCLPAEpochPlan:
		var plan message.CLPAEpochPlan
		if err := json.Unmarshal(raw, &plan); err != nil {
			return err
		}
		if err := plan.Validate(n.pbftChainConfig.ShardNums); err != nil {
			return err
		}
		if plan.Epoch <= e.epoch {
			return nil
		}
		if plan.BaseEpoch != e.epoch || (e.plan != nil && e.plan.PlanHash != plan.PlanHash) {
			return fmt.Errorf("conflicting or noncontiguous epoch plan")
		}
		if plan.InputCuts[n.ShardID] < e.input.prefix {
			return fmt.Errorf("plan input cut precedes admitted input")
		}
		for seq := range e.input.pending {
			if seq > plan.InputCuts[n.ShardID] {
				return fmt.Errorf("plan cuts off already received input")
			}
		}
		for _, ready := range e.ready {
			if ready.PlanHash != plan.PlanHash {
				return fmt.Errorf("early ready disagrees with plan")
			}
		}
		for _, transfer := range e.transfers {
			if transfer.PlanHash != plan.PlanHash {
				return fmt.Errorf("early transfer disagrees with plan")
			}
		}
		e.plan = &plan
		e.started = true
	case message.CCLPAEpochReady:
		var ready message.CLPAEpochReady
		if err := json.Unmarshal(raw, &ready); err != nil {
			return err
		}
		if ready.Epoch <= e.epoch {
			return nil
		}
		if ready.Epoch != e.epoch+1 || ready.FromShard >= n.pbftChainConfig.ShardNums || (!e.asyncPipeline && ready.RelayCut != e.limit) || ready.PlanHash == "" {
			return fmt.Errorf("invalid epoch ready")
		}
		if e.plan != nil && ready.PlanHash != e.plan.PlanHash {
			return fmt.Errorf("ready plan mismatch")
		}
		if old, ok := e.ready[ready.FromShard]; ok && old != ready {
			return fmt.Errorf("conflicting ready replay")
		}
		if q := e.relays[ready.FromShard]; q != nil {
			if q.prefix > ready.RelayCut {
				return fmt.Errorf("ready cut precedes applied relay")
			}
			for seq := range q.pending {
				if seq > ready.RelayCut {
					return fmt.Errorf("ready cuts off already received relay")
				}
			}
		}
		e.ready[ready.FromShard] = ready
	case message.CCLPAEpochTransfer:
		var transfer message.CLPAEpochTransfer
		if err := json.Unmarshal(raw, &transfer); err != nil {
			return err
		}
		if transfer.Epoch <= e.epoch {
			return nil
		}
		if transfer.Epoch != e.epoch+1 || transfer.FromShard >= n.pbftChainConfig.ShardNums || transfer.FromShard == n.ShardID || transfer.ToShard != n.ShardID || transfer.PlanHash == "" {
			return fmt.Errorf("invalid epoch transfer identity")
		}
		if e.plan != nil && transfer.PlanHash != e.plan.PlanHash {
			return fmt.Errorf("transfer plan mismatch")
		}
		if e.asyncPipeline {
			if ready, ok := e.ready[transfer.FromShard]; ok && ready.RelayCut != transfer.SourceRounds {
				return fmt.Errorf("transfer source cut disagrees with ready")
			}
		}
		if old, ok := e.transfers[transfer.FromShard]; ok && !bytes.Equal(epochJSON(old), epochJSON(transfer)) {
			return fmt.Errorf("conflicting transfer replay")
		}
		e.transfers[transfer.FromShard] = transfer
	case message.CCLPAEpochResume:
		var resume message.CLPAEpochResume
		if err := json.Unmarshal(raw, &resume); err != nil {
			return err
		}
		if resume.Epoch < e.epoch {
			return nil
		}
		if resume.Epoch != e.epoch || resume.PlanHash != e.installedHash || resume.PlanHash == "" {
			return fmt.Errorf("resume before matching local epoch install")
		}
		e.resumed = true
		e.started = true
	case message.CCLPAEpochDrain:
		var drain message.CLPAEpochDrain
		if err := json.Unmarshal(raw, &drain); err != nil {
			return err
		}
		if drain.Epoch < e.epoch {
			return nil
		}
		if drain.Epoch != e.epoch {
			return fmt.Errorf("unexpected epoch drain")
		}
		e.draining = true
		e.started = true
	case message.CInject, message.CRelay, message.CRelayWithProof, message.CPartitionMsg, message.CPartitionReady, message.AccountState_and_TX:
		return fmt.Errorf("legacy ingress is forbidden in CLPA epoch mode")
	default:
		return fmt.Errorf("unsupported CLPA epoch outside message %s", kind)
	}
	return nil
}

type clpaEpochInstallPayload struct {
	Plan      message.CLPAEpochPlan
	Transfers []message.CLPAEpochTransfer // One bundle per source. Async local work is bound by metadata only.
}

func (c *CLPAPbftInsideExtraHandleMod) epochPropose() (bool, *message.Request) {
	e, n := c.epoch, c.pbftNode
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.plan == nil {
		if !e.ordinaryAllowedLocked() {
			return false, nil
		}
		block := n.CurChain.GenerateBlock(int32(n.NodeID))
		return true, &message.Request{RequestType: message.BlockRequest, ReqTime: time.Now(), Msg: message.RawMessage{Content: block.Encode()}}
	}
	if (!e.asyncPipeline && e.rounds != e.limit) || e.input.prefix != e.plan.InputCuts[n.ShardID] {
		return false, nil
	}
	if !e.sentReady {
		ready := message.CLPAEpochReady{Epoch: e.plan.Epoch, PlanHash: e.plan.PlanHash, FromShard: n.ShardID, RelayCut: e.rounds}
		e.ready[n.ShardID] = ready
		e.sentReady = true
		for sid := uint64(0); sid < n.pbftChainConfig.ShardNums; sid++ {
			if sid != n.ShardID {
				n.epochSend(message.CCLPAEpochReady, ready, n.ip_nodeTable[sid][0])
			}
		}
	}
	if len(e.ready) != int(n.pbftChainConfig.ShardNums) {
		return false, nil
	}
	for sid := uint64(0); sid < n.pbftChainConfig.ShardNums; sid++ {
		if sid == n.ShardID {
			continue
		}
		q := e.relays[sid]
		if (q == nil && (!e.asyncPipeline || e.ready[sid].RelayCut != 0)) || (q != nil && q.prefix != e.ready[sid].RelayCut) {
			return false, nil
		}
	}
	if !e.sentTransfers {
		if err := c.epochBuildTransfersLocked(); err != nil {
			panic(err)
		}
		e.sentTransfers = true
	}
	if len(e.transfers) != int(n.pbftChainConfig.ShardNums) {
		return false, nil
	}
	payload := clpaEpochInstallPayload{Plan: *e.plan}
	for sid := uint64(0); sid < n.pbftChainConfig.ShardNums; sid++ {
		payload.Transfers = append(payload.Transfers, e.transfers[sid])
	}
	if err := c.validateEpochInstallLocked(payload); err != nil {
		panic(err)
	}
	return true, &message.Request{RequestType: message.CLPAEpochInstall, ReqTime: time.Now(), Msg: message.RawMessage{Content: epochJSON(payload)}}
}

func (c *CLPAPbftInsideExtraHandleMod) epochBuildTransfersLocked() error {
	e, n := c.epoch, c.pbftNode
	var pending []*core.Transaction
	var err error
	if e.asyncPipeline {
		pending, err = n.CurChain.Txpool.SnapshotCLPAEpochPendingReferences()
		e.sourcePending = pending
	} else {
		pending, err = n.CurChain.Txpool.SnapshotCLPAEpochPending()
	}
	if err != nil {
		return err
	}
	bundles := make(map[uint64]*message.CLPAEpochTransfer)
	for sid := uint64(0); sid < n.pbftChainConfig.ShardNums; sid++ {
		bundles[sid] = &message.CLPAEpochTransfer{Epoch: e.plan.Epoch, PlanHash: e.plan.PlanHash, FromShard: n.ShardID, ToShard: sid}
		if e.asyncPipeline {
			bundles[sid].SourceRounds = e.rounds
		}
	}
	var accounts []string
	for account, target := range e.plan.Mapping {
		if target != n.ShardID && n.CurChain.Get_PartitionMap(account) == n.ShardID {
			accounts = append(accounts, account)
		}
	}
	sort.Strings(accounts)
	states, err := n.CurChain.ExportCLPAEpochAccounts(accounts)
	if err != nil {
		return err
	}
	for i, account := range accounts {
		b := bundles[e.plan.Mapping[account]]
		b.Addrs = append(b.Addrs, account)
		b.AccountState = append(b.AccountState, states[i])
	}
	owner := func(account string) uint64 {
		if sid, ok := e.plan.Mapping[account]; ok {
			return sid
		}
		return n.CurChain.Get_PartitionMap(account)
	}
	for _, tx := range pending {
		if err := epochTxValid(tx, tx.Relayed); err != nil {
			return err
		}
		if epochTxOwner(tx, n.CurChain.Get_PartitionMap) != n.ShardID {
			return fmt.Errorf("pending leg is not owned by source shard")
		}
		sid := epochTxOwner(tx, owner)
		if e.asyncPipeline {
			leg, err := e.epochPendingLeg(tx, sid)
			if err != nil {
				return err
			}
			bundles[n.ShardID].PendingManifest = append(bundles[n.ShardID].PendingManifest, leg)
			if sid != n.ShardID {
				bundles[sid].Txs = append(bundles[sid].Txs, tx)
				bundles[sid].PendingManifest = append(bundles[sid].PendingManifest, leg)
			}
		} else {
			bundles[sid].Txs = append(bundles[sid].Txs, tx)
		}
	}
	for sid := uint64(0); sid < n.pbftChainConfig.ShardNums; sid++ {
		if sid == n.ShardID {
			e.transfers[sid] = *bundles[sid]
		} else {
			n.epochSend(message.CCLPAEpochTransfer, bundles[sid], n.ip_nodeTable[sid][0])
		}
	}
	return nil
}

func (c *CLPAPbftInsideExtraHandleMod) validateEpochInstallLocked(payload clpaEpochInstallPayload) error {
	e, n := c.epoch, c.pbftNode
	if err := payload.Plan.Validate(n.pbftChainConfig.ShardNums); err != nil {
		return err
	}
	if payload.Plan.BaseEpoch != e.epoch || payload.Plan.Epoch != e.epoch+1 || (!e.asyncPipeline && e.rounds != e.limit) {
		return fmt.Errorf("epoch install at wrong ordinary commit boundary")
	}
	if e.plan != nil && e.plan.PlanHash != payload.Plan.PlanHash {
		return fmt.Errorf("epoch install disagrees with accepted plan")
	}
	if len(payload.Transfers) != int(n.pbftChainConfig.ShardNums) {
		return fmt.Errorf("epoch install missing source bundle")
	}
	seenSources, seenAccounts, seenLegs := map[uint64]bool{}, map[string]bool{}, map[string]bool{}
	owner := func(account string) uint64 {
		if sid, ok := payload.Plan.Mapping[account]; ok {
			return sid
		}
		return n.CurChain.Get_PartitionMap(account)
	}
	for _, b := range payload.Transfers {
		if b.Epoch != payload.Plan.Epoch || b.PlanHash != payload.Plan.PlanHash || b.ToShard != n.ShardID || b.FromShard >= n.pbftChainConfig.ShardNums || seenSources[b.FromShard] || len(b.Addrs) != len(b.AccountState) {
			return fmt.Errorf("invalid source bundle identity/shape")
		}
		seenSources[b.FromShard] = true
		if e.asyncPipeline {
			if err := c.validateEpochManifestLocked(b, owner, seenLegs); err != nil {
				return err
			}
		}
		for i, account := range b.Addrs {
			state := b.AccountState[i]
			target, changed := payload.Plan.Mapping[account]
			if seenAccounts[account] || !changed || target != n.ShardID || b.FromShard == n.ShardID || n.CurChain.Get_PartitionMap(account) != b.FromShard || state == nil || state.Balance == nil || state.Balance.Sign() < 0 || len(state.StorageRoot) != 0 || len(state.CodeHash) != 0 || state.AcAddress != "" {
				return fmt.Errorf("invalid imported EOA state")
			}
			seenAccounts[account] = true
		}
		for _, tx := range b.Txs {
			if err := epochTxValid(tx, tx.Relayed); err != nil {
				return err
			}
			key := fmt.Sprintf("%s/%t", tx.AdmissionID, tx.Relayed)
			if (!e.asyncPipeline && seenLegs[key]) || epochTxOwner(tx, owner) != n.ShardID || epochTxOwner(tx, n.CurChain.Get_PartitionMap) != b.FromShard {
				return fmt.Errorf("duplicate or wrongly routed installed pending leg")
			}
			seenLegs[key] = true
		}
	}
	for account, target := range payload.Plan.Mapping {
		if target == n.ShardID && n.CurChain.Get_PartitionMap(account) != n.ShardID && !seenAccounts[account] {
			return fmt.Errorf("epoch install missing moved account %s", account)
		}
	}
	if e.asyncPipeline {
		if _, _, err := c.epochDeltaPendingLocked(payload); err != nil {
			return err
		}
	}
	return nil
}

func (c *CLPAPbftInsideExtraHandleMod) validateEpochRequest(r *message.Request) bool {
	e := c.epoch
	e.mu.Lock()
	defer e.mu.Unlock()
	if r.RequestType == message.CLPAEpochInstall {
		var payload clpaEpochInstallPayload
		return json.Unmarshal(r.Msg.Content, &payload) == nil && c.validateEpochInstallLocked(payload) == nil
	}
	// The fixed leader alone gates Resume/Drain and the round ceiling. Control
	// messages and its next PBFT proposal can arrive at a follower in either
	// order; rejecting that proposal would stall consensus without a resend.
	if r.RequestType != message.BlockRequest {
		return false
	}
	block := core.DecodeB(r.Msg.Content)
	for _, tx := range block.Body {
		if tx == nil || epochTxValid(tx, tx.Relayed) != nil || epochTxOwner(tx, c.pbftNode.CurChain.Get_PartitionMap) != c.pbftNode.ShardID {
			return false
		}
	}
	return c.pbftNode.CurChain.IsValidBlock(block) == nil
}

// Existing history transport matches one outstanding request identity. It
// trusts its configured peer and carries no Byzantine commit certificate.
func (c *CLPAPbftInsideExtraHandleMod) epochSequentialRequest(som *message.SendOldMessage) bool {
	if !validHistoryEnvelope(som) {
		return false
	}
	for i, r := range som.OldRequest {
		sequence := som.SeqStartHeight + uint64(i)
		if sequence != c.pbftNode.sequenceID || sequence != c.pbftNode.CurChain.CurrentBlock.Header.Number+1 {
			return false
		}
		if r.RequestType == message.CLPAEpochInstall {
			if err := c.commitEpochInstall(r, sequence, getDigest(r)); err != nil {
				return false
			}
		} else if r.RequestType == message.BlockRequest {
			if !c.validateEpochRequest(r) {
				return false
			}
			block := core.DecodeB(r.Msg.Content)
			if err := c.pbftNode.CurChain.AddBlock(block); err != nil {
				c.pbftNode.pl.Plog.Printf("historical block application rejected: %v", err)
				return false
			}
			c.epochBlockCommitted(block)
		} else {
			return false
		}
		c.pbftNode.recordHistoricalProgress(r, sequence)
	}
	return true
}

func (c *CLPAPbftInsideExtraHandleMod) commitEpochInstall(r *message.Request, sequence uint64, digest []byte) error {
	e, n := c.epoch, c.pbftNode
	e.mu.Lock()
	defer e.mu.Unlock()
	if sequence != n.sequenceID || sequence != n.CurChain.CurrentBlock.Header.Number+1 {
		return fmt.Errorf("epoch install sequence %d is not next consensus/ledger position", sequence)
	}
	var payload clpaEpochInstallPayload
	if err := json.Unmarshal(r.Msg.Content, &payload); err != nil {
		return err
	}
	if err := c.validateEpochInstallLocked(payload); err != nil {
		return err
	}
	var accounts []string
	var states []*core.AccountState
	var pending []*core.Transaction
	var incoming, sourceSnapshot, retained []*core.Transaction
	for _, b := range payload.Transfers {
		accounts = append(accounts, b.Addrs...)
		states = append(states, b.AccountState...)
		pending = append(pending, b.Txs...)
	}
	if e.asyncPipeline {
		incoming = pending
		var err error
		sourceSnapshot, retained, err = c.epochDeltaPendingLocked(payload)
		if err != nil {
			return err
		}
		pending = append(append([]*core.Transaction{}, retained...), incoming...)
	}
	movedOut := 0
	for account, target := range payload.Plan.Mapping {
		if target != n.ShardID && n.CurChain.Get_PartitionMap(account) == n.ShardID {
			movedOut++
		}
	}
	if err := n.CurChain.InstallCLPAEpochAccounts(payload.Plan.Mapping, accounts, states, n.view.Load()); err != nil {
		return err
	}
	if e.asyncPipeline {
		if err := n.CurChain.Txpool.InstallCLPAEpochPendingDelta(sourceSnapshot, retained, incoming); err != nil {
			return err
		}
	} else {
		if err := n.CurChain.Txpool.ReplaceCLPAEpochPending(pending); err != nil {
			return err
		}
	}
	if err := writeCLPAEpochInstallEvidence(n, payload.Plan.Epoch, sequence, digest, r.Msg.Content); err != nil {
		return err
	}
	// EOF is a one-way policy: if it raced an already accepted plan, finish
	// installing that plan and keep the ceiling disabled after global Resume.
	localRounds := e.rounds
	e.epoch = payload.Plan.Epoch
	e.rounds = 0
	e.plan = nil
	e.resumed = false
	e.started = false
	e.input = epochPrefix{}
	e.relays = map[uint64]*epochPrefix{}
	e.ready = map[uint64]message.CLPAEpochReady{}
	e.transfers = map[uint64]message.CLPAEpochTransfer{}
	e.sentReady = false
	e.sentTransfers = false
	e.sourcePending = nil
	e.installedHash = payload.Plan.PlanHash
	e.admissions = map[string]string{}
	if e.asyncPipeline {
		source, _, arrived := epochPayloadManifests(payload, n.ShardID)
		byKey := map[string]string{}
		for _, leg := range append(source, arrived...) {
			byKey[epochLegKey(leg.AdmissionID, leg.Relayed)] = leg.PayloadHash
		}
		for _, tx := range pending {
			key := epochLegKey(tx.AdmissionID, tx.Relayed)
			e.admissions[key] = byKey[key]
		}
	} else {
		for _, tx := range pending {
			e.admissions[fmt.Sprintf("%s/%t", tx.AdmissionID, tx.Relayed)] = epochFingerprint(tx)
		}
	}
	c.cdm.AccountTransferRound = e.epoch
	ordinary, relayed := 0, 0
	for _, tx := range pending {
		if tx.Relayed {
			relayed++
		} else {
			ordinary++
		}
	}
	// Legacy reports the complete replicated queue. Async reports only migrated
	// payload and separately counts O(all source pending) manifest metadata.
	pendingArray := append([]*core.Transaction{}, pending...)
	if e.asyncPipeline {
		pendingArray = append([]*core.Transaction{}, incoming...)
	}
	applied := message.CLPAEpochApplied{Epoch: e.epoch, PlanHash: e.installedHash, Shard: n.ShardID, Node: n.NodeID, Height: n.CurChain.CurrentBlock.Header.Number, StateRoot: hex.EncodeToString(n.CurChain.CurrentBlock.Header.StateRoot), Pending: len(pending), PendingOrdinary: ordinary, PendingRelayed: relayed, PendingPayloadBytes: uint64(len(epochJSON(pendingArray))), MovedIn: len(accounts), MovedOut: movedOut}
	if e.asyncPipeline {
		source, kept, arrived := epochPayloadManifests(payload, n.ShardID)
		ledger := []message.CLPAEpochPendingLeg{}
		byKey := map[string]message.CLPAEpochPendingLeg{}
		for _, leg := range append(append([]message.CLPAEpochPendingLeg{}, source...), arrived...) {
			byKey[epochLegKey(leg.AdmissionID, leg.Relayed)] = leg
		}
		for _, tx := range pending {
			ledger = append(ledger, byKey[epochLegKey(tx.AdmissionID, tx.Relayed)])
		}
		applied.AsyncPipeline = true
		applied.ReplicaRole = message.CLPAEpochFollowerRole
		if n.NodeID == uint64(n.view.Load()) {
			applied.ReplicaRole = message.CLPAEpochFixedLeaderRole
		}
		applied.LocalRounds = localRounds
		applied.InputCut = payload.Plan.InputCuts[n.ShardID]
		applied.RetainedPending = len(kept)
		applied.RetainedAvailable = len(retained)
		applied.RetainedManifestHash = epochManifestHash(kept)
		applied.IncomingPending = len(arrived)
		applied.IncomingManifestHash = epochManifestHash(arrived)
		applied.SourceManifestHash = epochManifestHash(source)
		applied.PendingLedgerHash = epochManifestHash(ledger)
		installHash := sha256.Sum256(r.Msg.Content)
		applied.InstallPayloadHash = hex.EncodeToString(installHash[:])
		applied.InstallPayloadBytes = uint64(len(r.Msg.Content))
		applied.PendingManifestBytes = uint64(len(epochJSON(source)) + len(epochJSON(arrived)))
	}
	n.epochSend(message.CCLPAEpochApplied, applied, n.ip_nodeTable[params.SupervisorShard][0])
	n.pl.Plog.Printf("CLPA_EPOCH_INSTALLED epoch=%d shard=%d node=%d pending=%d moved_in=%d moved_out=%d", e.epoch, n.ShardID, n.NodeID, len(pending), len(accounts), movedOut)
	return nil
}

func writeCLPAEpochInstallEvidence(n *PbftConsensusNode, epoch, sequence uint64, digest, payload []byte) error {
	dir := filepath.Join(params.DatabaseWrite_path, "clpa_epoch_installs", fmt.Sprintf("S%d_N%d", n.ShardID, n.NodeID))
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	record := epochJSON(struct {
		Epoch, Sequence uint64
		Digest          string
		Payload         json.RawMessage
	}{epoch, sequence, hex.EncodeToString(digest), payload})
	path := filepath.Join(dir, fmt.Sprintf("epoch_%020d.json", epoch))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if os.IsExist(err) {
		old, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if !bytes.Equal(old, record) {
			return fmt.Errorf("conflicting existing epoch evidence")
		}
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	written, err := f.Write(record)
	if err != nil {
		return err
	}
	if written != len(record) {
		return io.ErrShortWrite
	}
	if err = f.Sync(); err != nil {
		return err
	}
	// Persist both the evidence entry and the two newly created directory
	// entries. This still supplies no cross-store crash/restart protocol.
	for _, directory := range []string{dir, filepath.Dir(dir), filepath.Dir(filepath.Dir(dir))} {
		d, err := os.Open(directory)
		if err != nil {
			return err
		}
		err = d.Sync()
		closeErr := d.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func (c *CLPAPbftInsideExtraHandleMod) epochBlockCommitted(block *core.Block) {
	c.pbftNode.CurChain.Txpool.RemoveCLPAEpochCommitted(block.Body)
	c.epoch.mu.Lock()
	c.epoch.rounds++
	c.epoch.mu.Unlock()
}

func (c *CLPAPbftInsideExtraHandleMod) epochRelaySend() {
	e, n := c.epoch, c.pbftNode
	e.mu.Lock()
	epoch, sequence := e.epoch, e.rounds
	e.mu.Unlock()
	n.CurChain.Txpool.GetLocked()
	for sid := uint64(0); sid < n.pbftChainConfig.ShardNums; sid++ {
		if sid != n.ShardID {
			n.epochSend(message.CCLPAEpochRelay, message.CLPAEpochRelay{Epoch: epoch, Sequence: sequence, FromShard: n.ShardID, Txs: n.CurChain.Txpool.RelayPool[sid]}, n.ip_nodeTable[sid][0])
		}
	}
	n.CurChain.Txpool.GetUnlocked()
	n.CurChain.Txpool.ClearRelayPool()
}
