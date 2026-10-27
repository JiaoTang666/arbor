package committee

import (
	"blockEmulator/core"
	"blockEmulator/initialledger"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

// CLPAEpochPlanProvider is the compatible full-mapping planning seam. It receives the completed
// epoch's graph and current cumulative mapping while dispatch is paused.
// Forest also exposes PlanDelta for the native sparse runtime path and is
// explicitly named in audit. Full-map providers return
// the cumulative desired mapping; only its changed assignments go on the wire.
type CLPAEpochPlanProvider interface {
	Name() string
	Plan(epoch uint64, graph *partition.CLPAState, current map[string]uint64) (map[string]uint64, error)
}

type nativeCLPAProvider struct{}

func (nativeCLPAProvider) Name() string { return "clpa" }
func (nativeCLPAProvider) Plan(_ uint64, graph *partition.CLPAState, current map[string]uint64) (map[string]uint64, error) {
	result := copyEpochMapping(current)
	changes, _ := graph.CLPA_Partition()
	for account, shard := range changes {
		result[account] = shard
	}
	return result, nil
}

// Explicit test fixture for repeated/remigration transport coverage; never a
// performance or CLPA quality result. Only committed graph accounts are moved.
type rotatingFixtureProvider struct{}

func (rotatingFixtureProvider) Name() string { return "fixture-rotate" }
func (rotatingFixtureProvider) Plan(_ uint64, graph *partition.CLPAState, current map[string]uint64) (map[string]uint64, error) {
	result := copyEpochMapping(current)
	for vertex := range graph.NetGraph.VertexSet {
		result[vertex.Addr] = uint64((graph.PartitionMap[vertex] + 1) % params.ShardNum)
	}
	return result, nil
}

func copyEpochMapping(source map[string]uint64) map[string]uint64 {
	result := make(map[string]uint64, len(source))
	for account, shard := range source {
		result[account] = shard
	}
	return result
}

func (ccm *CLPACommitteeModule) clpaEpochDelta(desired map[string]uint64) (map[string]uint64, map[string]map[string]uint64) {
	delta := map[string]uint64{}
	moves := map[string]map[string]uint64{}
	for account, target := range desired {
		if source := ccm.fetchModifiedMap(account); source != target {
			delta[account] = target
			moves[account] = map[string]uint64{"from": source, "to": target}
		}
	}
	return delta, moves
}

type clpaEpochController struct {
	async              *forestAsyncPipeline
	ccm                *CLPACommitteeModule
	mu                 sync.Mutex
	auditMu            sync.Mutex
	audit              io.Writer
	provider           CLPAEpochPlanProvider
	rounds             int
	timeout            time.Duration
	epoch              uint64
	counts             map[uint64]int
	reportedRounds     map[uint64]map[uint64]int // Audit late block reports without advancing current cadence.
	blocks             map[string]bool
	cuts               map[uint64]uint64
	plan               *message.CLPAEpochPlan
	applied            map[[2]uint64]message.CLPAEpochApplied
	appliedHistory     map[uint64]map[[2]uint64]message.CLPAEpochApplied
	planDispatchAt     time.Time
	firstAppliedAt     time.Time
	hashes             map[uint64]string
	terminal           map[uint64]bool
	inputHash          string
	start              time.Time
	nativeCompleteAt   time.Time
	auditOrigin        time.Time // One monotonic supervisor instance; never inferred from UTC.
	dispatched         int
	drain              bool
	runFinished        bool // Async stop may not precede observer/worker close and final audit sync.
	failure            error
	initialRoutes      map[string]uint64 // Immutable ledger-backed eligibility/base routing.
	initialLedgerHash  string
	created            map[string]bool   // Additional eligibility only after a verified terminal occurrence.
	committedOverrides map[string]uint64 // Forest only: sparse deviations from immutable initial/hash owners.
}

func epochPositiveEnv(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		panic("invalid positive " + name)
	}
	return value
}

func newCLPAEpochController(ccm *CLPACommitteeModule) *clpaEpochController {
	path := os.Getenv("BLOCK_EMULATOR_CLPA_AUDIT_PATH")
	if path == "" {
		panic("CLPA epoch audit path required")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	input := os.Getenv("BLOCK_EMULATOR_CLPA_INPUT_SHA256")
	decoded, err := hex.DecodeString(input)
	if err != nil || len(decoded) != 32 {
		panic("CLPA input SHA256 required")
	}
	e := &clpaEpochController{ccm: ccm, audit: f, provider: nativeCLPAProvider{},
		auditOrigin: time.Now(),
		rounds:      epochPositiveEnv("BLOCK_EMULATOR_CLPA_EPOCH_ROUNDS", 20),
		timeout:     time.Duration(epochPositiveEnv("BLOCK_EMULATOR_CLPA_BARRIER_TIMEOUT_SECONDS", 180)) * time.Second,
		counts:      map[uint64]int{}, blocks: map[string]bool{}, cuts: map[uint64]uint64{},
		applied: map[[2]uint64]message.CLPAEpochApplied{}, appliedHistory: map[uint64]map[[2]uint64]message.CLPAEpochApplied{}, hashes: map[uint64]string{},
		terminal: map[uint64]bool{}, created: map[string]bool{}, inputHash: input}
	ledger, err := initialledger.FromEnv(uint64(params.ShardNum))
	if err != nil {
		panic(err)
	}
	if ledger != nil {
		e.initialRoutes = ledger.Routes
		e.initialLedgerHash = ledger.FileHash
		ccm.modifiedMap = copyEpochMapping(ledger.Routes)
		if os.Getenv("BLOCK_EMULATOR_CLPA_PLAN_PROVIDER") != "forest" {
			ccm.clpaReset()
		}
	}
	switch os.Getenv("BLOCK_EMULATOR_CLPA_PLAN_PROVIDER") {
	case "", "clpa":
	case "fixture-rotate":
		e.provider = rotatingFixtureProvider{}
	case "forest":
		forest, err := newForestEpochProvider(e.initialRoutes, e.initialLedgerHash)
		if err != nil {
			panic(err)
		}
		e.provider = forest
	default:
		panic("unsupported epoch plan provider")
	}
	if os.Getenv("BLOCK_EMULATOR_CLPA_ASYNC_PIPELINE") == "1" {
		forest, ok := e.provider.(*forestEpochProvider)
		if !ok {
			panic("async pipeline requires forest provider")
		}
		e.async, err = newForestAsyncPipeline(e, forest, path+".observations.jsonl")
		if err != nil {
			panic(err)
		}
	}
	return e
}

func (e *clpaEpochController) record(event string, fields map[string]interface{}) {
	if fields == nil {
		fields = map[string]interface{}{}
	}
	fields["schema"], fields["event"], fields["recorded_at"] = "clpa_epoch_audit_v1", event, time.Now().UTC()
	if !e.auditOrigin.IsZero() {
		fields["clock_instance"] = fmt.Sprintf("supervisor/%d/%d", os.Getpid(), e.auditOrigin.UnixNano())
		fields["monotonic_offset_ns"] = time.Since(e.auditOrigin).Nanoseconds()
	}
	e.auditMu.Lock()
	defer e.auditMu.Unlock()
	if err := json.NewEncoder(e.audit).Encode(fields); err != nil {
		panic(err)
	}
}

func (e *clpaEpochController) failLocked(err error) {
	if e.failure == nil {
		e.failure = err
		e.record("failure", map[string]interface{}{"reason": err.Error()})
	}
}

func (e *clpaEpochController) check() {
	e.mu.Lock()
	err := e.failure
	e.mu.Unlock()
	if err != nil {
		panic(err)
	}
}

func (e *clpaEpochController) send(kind message.MessageType, value interface{}, address string) {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	networks.TcpDialObserved(message.MergeMessage(kind, raw), address, func(event string, _ time.Time, failure error) {
		if failure != nil {
			e.mu.Lock()
			defer e.mu.Unlock()
			e.failLocked(fmt.Errorf("CLPA transport %s to %s: %w", event, address, failure))
		}
	})
}

func (e *clpaEpochController) broadcast(kind message.MessageType, value interface{}, leadersOnly bool) {
	for shard := uint64(0); shard < uint64(params.ShardNum); shard++ {
		for node := uint64(0); node < uint64(params.NodesInShard); node++ {
			if leadersOnly && node != 0 {
				continue
			}
			e.send(kind, value, e.ccm.IpNodeTable[shard][node])
		}
	}
}

func (e *clpaEpochController) handleOther(raw []byte) {
	if len(raw) < 30 {
		panic("short CLPA supervisor message")
	}
	kind, body := message.SplitMessage(raw)
	if kind != message.CCLPAEpochApplied {
		return
	}
	var applied message.CLPAEpochApplied
	if err := json.Unmarshal(body, &applied); err != nil {
		panic(err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.acceptAppliedLocked(applied); err != nil {
		e.failLocked(err)
	}
}

func (e *clpaEpochController) acceptAppliedLocked(applied message.CLPAEpochApplied) error {
	if applied.Shard >= uint64(params.ShardNum) || applied.Node >= uint64(params.NodesInShard) {
		return fmt.Errorf("unexpected CLPA replica apply identity")
	}
	root, err := hex.DecodeString(applied.StateRoot)
	if err != nil || len(root) != 32 || applied.Pending < 0 || applied.PendingOrdinary < 0 || applied.PendingRelayed < 0 ||
		applied.PendingOrdinary+applied.PendingRelayed != applied.Pending {
		return fmt.Errorf("invalid CLPA replica state evidence")
	}
	key := [2]uint64{applied.Shard, applied.Node}
	if applied.Epoch <= e.epoch {
		if old, ok := e.appliedHistory[applied.Epoch][key]; ok && old == applied {
			e.record("stale_applied_duplicate", map[string]interface{}{"epoch": applied.Epoch,
				"plan_hash": applied.PlanHash, "shard": applied.Shard, "node": applied.Node})
			return nil
		}
		return fmt.Errorf("conflicting or unrecognized past CLPA replica apply")
	}
	if e.plan == nil || applied.Epoch != e.plan.Epoch || applied.PlanHash != e.plan.PlanHash {
		return fmt.Errorf("unexpected CLPA replica apply identity")
	}
	if old, ok := e.applied[key]; ok {
		if old != applied {
			return fmt.Errorf("conflicting CLPA replica apply")
		}
		return nil
	}
	if e.async != nil {
		role := message.CLPAEpochFollowerRole
		if applied.Node == 0 {
			role = message.CLPAEpochFixedLeaderRole
		}
		if !applied.AsyncPipeline || applied.ReplicaRole != role || applied.InputCut != e.plan.InputCuts[applied.Shard] ||
			applied.RetainedPending < 0 || applied.RetainedAvailable < 0 || applied.RetainedAvailable > applied.RetainedPending ||
			applied.IncomingPending < 0 || applied.Pending != applied.RetainedAvailable+applied.IncomingPending ||
			(role == message.CLPAEpochFixedLeaderRole && applied.RetainedAvailable != applied.RetainedPending) {
			return fmt.Errorf("async replica role/queue evidence mismatch")
		}
		for _, hash := range []string{applied.RetainedManifestHash, applied.IncomingManifestHash, applied.SourceManifestHash, applied.PendingLedgerHash, applied.InstallPayloadHash} {
			if !forestHashValid(hash) {
				return fmt.Errorf("async replica omitted manifest hash")
			}
		}
	}
	for _, other := range e.applied {
		if e.async != nil {
			if other.Shard == applied.Shard && (other.Height != applied.Height || other.StateRoot != applied.StateRoot ||
				other.InstallPayloadHash != applied.InstallPayloadHash || other.SourceManifestHash != applied.SourceManifestHash ||
				other.RetainedManifestHash != applied.RetainedManifestHash || other.IncomingManifestHash != applied.IncomingManifestHash ||
				other.RetainedPending != applied.RetainedPending || other.IncomingPending != applied.IncomingPending ||
				other.LocalRounds != applied.LocalRounds || other.InputCut != applied.InputCut) {
				return fmt.Errorf("async replicas disagree on installed state/manifest")
			}
			continue
		}
		if other.Shard == applied.Shard && (other.Height != applied.Height || other.StateRoot != applied.StateRoot ||
			other.Pending != applied.Pending || other.PendingOrdinary != applied.PendingOrdinary ||
			other.PendingRelayed != applied.PendingRelayed || other.PendingPayloadBytes != applied.PendingPayloadBytes) {
			return fmt.Errorf("CLPA replicas disagree on installed state/pending checkpoint")
		}
	}
	if e.firstAppliedAt.IsZero() {
		e.firstAppliedAt = time.Now()
	}
	e.applied[key] = applied
	return nil
}

func (e *clpaEpochController) handleBlock(block *message.BlockInfoMsg) {
	receivedAt := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	_, forest := e.provider.(*forestEpochProvider)
	if block.SenderShardID >= uint64(params.ShardNum) || block.Epoch < 0 || uint64(block.Epoch) > e.epoch {
		e.failLocked(fmt.Errorf("invalid CLPA block shard/epoch"))
		return
	}
	if block.CommittedBlockHash == "" {
		e.failLocked(fmt.Errorf("missing committed CLPA block hash"))
		return
	}
	key := fmt.Sprintf("%d/%s", block.SenderShardID, block.CommittedBlockHash)
	if e.blocks[key] {
		return
	}
	e.blocks[key] = true
	if e.reportedRounds == nil {
		e.reportedRounds = map[uint64]map[uint64]int{}
	}
	blockEpoch := uint64(block.Epoch)
	if e.reportedRounds[blockEpoch] == nil {
		e.reportedRounds[blockEpoch] = map[uint64]int{}
	}
	e.reportedRounds[blockEpoch][block.SenderShardID]++
	if uint64(block.Epoch) == e.epoch {
		e.counts[block.SenderShardID]++
		if e.async == nil && !e.drain && e.counts[block.SenderShardID] > e.rounds {
			e.failLocked(fmt.Errorf("CLPA ordinary round ceiling exceeded"))
			return
		}
	}
	if os.Getenv("BLOCK_EMULATOR_BASELINE_CLOCK_V1") == "1" {
		for _, tx := range block.Relay1Txs {
			if !tx.IngressSequenceAssigned || e.hashes[tx.IngressSequence] != hex.EncodeToString(tx.TxHash) {
				e.failLocked(fmt.Errorf("unrecognized CLPA first-stage occurrence"))
				return
			}
			e.record("stage_receipt", map[string]interface{}{"sequence": tx.IngressSequence,
				"epoch": block.Epoch, "shard": block.SenderShardID, "leg": "relay1",
				"tx_hash": hex.EncodeToString(tx.TxHash), "registered_elapsed_ns": time.Since(e.start).Nanoseconds()})
		}
	}
	for group, list := range [][]*core.Transaction{block.InnerShardTxs, block.Relay2Txs} {
		for _, tx := range list {
			seq := tx.IngressSequence
			want := "clpa/" + e.inputHash + "/" + strconv.FormatUint(seq, 10)
			if !tx.IngressSequenceAssigned || tx.AdmissionID != want || e.hashes[seq] != hex.EncodeToString(tx.TxHash) {
				e.failLocked(fmt.Errorf("unrecognized CLPA terminal occurrence %d", seq))
				continue
			}
			if e.terminal[seq] {
				e.failLocked(fmt.Errorf("duplicate CLPA terminal occurrence %d", seq))
				continue
			}
			e.terminal[seq] = true
			// A route entry or observation is not proof of account creation.
			// Initial ledger accounts are already eligible; add only genuinely
			// terminal new endpoints, after occurrence validation and dedup.
			if e.created == nil {
				e.created = map[string]bool{}
			}
			for _, account := range []string{tx.Sender, tx.Recipient} {
				if _, initial := e.initialRoutes[account]; !initial {
					e.created[account] = true
				}
			}
			e.record("terminal", map[string]interface{}{"sequence": seq, "epoch": block.Epoch,
				"shard": block.SenderShardID, "tx_hash": hex.EncodeToString(tx.TxHash), "committed_at": block.CommitTime,
				"offered_at": tx.Time, "latency_seconds": block.CommitTime.Sub(tx.Time).Seconds(),
				"received_at": receivedAt, "receipt_elapsed_ns": receivedAt.Sub(e.start).Nanoseconds(),
				"scheduled_offset_ns": tx.Time.Sub(e.start).Nanoseconds(),
				"leg":                 []string{"inner", "relay2"}[group], "registered_elapsed_ns": time.Since(e.start).Nanoseconds()})
			if !forest && !e.drain && uint64(block.Epoch) == e.epoch {
				e.ccm.clpaGraph.AddEdge(partition.Vertex{Addr: tx.Sender}, partition.Vertex{Addr: tx.Recipient})
			}
		}
	}
	// Installation ACKs and block reports use independent connections. A late
	// old-epoch report remains evidence of that cut, not a new-epoch round.
	e.record("ordinary_block", map[string]interface{}{"epoch": block.Epoch, "shard": block.SenderShardID,
		"round": e.reportedRounds[blockEpoch][block.SenderShardID], "block_hash": block.CommittedBlockHash,
		"transactions": block.BlockBodyLength, "late_report": blockEpoch < e.epoch,
		"direct": len(block.InnerShardTxs), "relay1": len(block.Relay1Txs), "relay2": len(block.Relay2Txs),
		"proposed_at": block.ProposeTime, "committed_at": block.CommitTime,
		"received_elapsed_ns": receivedAt.Sub(e.start).Nanoseconds()})
	if e.ccm != nil && e.ccm.dataTotalNum > 0 && len(e.terminal) == e.ccm.dataTotalNum && e.nativeCompleteAt.IsZero() && !e.start.IsZero() {
		e.nativeCompleteAt = time.Now()
		e.record("native_transactions_complete", map[string]interface{}{"at": e.nativeCompleteAt,
			"elapsed_seconds": e.nativeCompleteAt.Sub(e.start).Seconds(), "clock": "supervisor receives final verified terminal"})
	}
}

func (e *clpaEpochController) readyLocked() bool {
	if e.async != nil {
		return e.plan == nil && e.async.installReadyLocked()
	}
	if e.drain || e.plan != nil {
		return false
	}
	for shard := uint64(0); shard < uint64(params.ShardNum); shard++ {
		if e.counts[shard] != e.rounds {
			return false
		}
	}
	return true
}

func (e *clpaEpochController) offeredDue(at time.Time) int {
	if e.start.IsZero() || at.Before(e.start) {
		return 0
	}
	due := int(at.Sub(e.start).Seconds()*float64(params.InjectSpeed)) + 1
	if os.Getenv("BLOCK_EMULATOR_COMMON_BATCH_CLOCK_V1") == "1" {
		due = (int(at.Sub(e.start).Seconds()*float64(params.InjectSpeed)/float64(e.ccm.batchDataNum)) + 1) * e.ccm.batchDataNum
	}
	if due > e.ccm.dataTotalNum {
		due = e.ccm.dataTotalNum
	}
	return due
}

type epochMappingProposal struct {
	mapping                                            map[string]uint64 // Full desired mapping only for CLPA/fixture.
	delta                                              map[string]uint64
	moves                                              map[string]map[string]uint64
	snapshotSeconds, plannerSeconds, validationSeconds float64
}

// Legacy mode plans at the paused exact-prefix boundary. Async mode only
// validates the already computed immutable result against current ownership.
func (e *clpaEpochController) planMappingLocked() (proposal epochMappingProposal, err error) {
	if e.async != nil {
		return e.async.proposalLocked()
	}
	started := time.Now()
	forest, sparse := e.provider.(*forestEpochProvider)
	var previous map[string]uint64
	if sparse {
		if forest.observed != uint64(e.dispatched) {
			return proposal, fmt.Errorf("forest plan prefix differs from dispatched epoch cut")
		}
		previous = copyEpochMapping(e.committedOverrides)
		eligible := make([]string, 0, len(e.created))
		for account := range e.created {
			eligible = append(eligible, account)
		}
		forest.SetEligible(eligible)
	} else {
		previous = copyEpochMapping(e.ccm.modifiedMap)
	}
	proposal.snapshotSeconds = time.Since(started).Seconds()
	started = time.Now()
	if sparse {
		proposal.delta, err = forest.PlanDelta(e.epoch+1, previous)
	} else {
		proposal.mapping, err = e.provider.Plan(e.epoch+1, e.ccm.clpaGraph, previous)
	}
	proposal.plannerSeconds = time.Since(started).Seconds()
	if err != nil {
		return proposal, err
	}
	started = time.Now()
	if sparse {
		proposal.moves = make(map[string]map[string]uint64, len(proposal.delta))
		for account, target := range proposal.delta {
			source := e.ccm.fetchModifiedMap(account)
			owner, ok := previous[account]
			if !ok {
				owner = forest.baseOwner(account)
			}
			if source != owner || source == target {
				return proposal, fmt.Errorf("forest committed override disagrees with actual routing")
			}
			proposal.moves[account] = map[string]uint64{"from": source, "to": target}
		}
	} else {
		proposal.delta, proposal.moves = e.ccm.clpaEpochDelta(proposal.mapping)
	}
	proposal.validationSeconds = time.Since(started).Seconds()
	return proposal, nil
}

// No routing or committed override changes are allowed at proposal time or
// after only a subset of replicas report installation. Caller holds e.mu.
func (e *clpaEpochController) applyPlannedMappingLocked(mapping map[string]uint64) error {
	if e.failure != nil {
		return e.failure
	}
	if e.plan == nil || e.plan.BaseEpoch != e.epoch || e.plan.Epoch != e.epoch+1 || len(e.applied) != params.ShardNum*params.NodesInShard {
		return fmt.Errorf("mapping apply requires every replica's installation")
	}
	for shard := uint64(0); shard < uint64(params.ShardNum); shard++ {
		for node := uint64(0); node < uint64(params.NodesInShard); node++ {
			applied, ok := e.applied[[2]uint64{shard, node}]
			if !ok || applied.Epoch != e.plan.Epoch || applied.PlanHash != e.plan.PlanHash {
				return fmt.Errorf("mapping apply lacks matching replica installation")
			}
		}
	}
	if forest, ok := e.provider.(*forestEpochProvider); ok {
		if e.committedOverrides == nil {
			e.committedOverrides = map[string]uint64{}
		}
		for account, target := range e.plan.Mapping {
			e.ccm.modifiedMap[account] = target
			if target == forest.baseOwner(account) {
				delete(e.committedOverrides, account)
			} else {
				e.committedOverrides[account] = target
			}
		}
	} else {
		if mapping == nil {
			return fmt.Errorf("CLPA mapping missing at apply")
		}
		e.ccm.modifiedMap = copyEpochMapping(mapping)
		e.ccm.clpaReset()
	}
	return nil
}

func (e *clpaEpochController) repartition() {
	if e.async != nil {
		e.async.tick()
	}
	started := time.Now()
	e.mu.Lock()
	if !e.readyLocked() {
		e.mu.Unlock()
		return
	}
	proposal, err := e.planMappingLocked()
	if err != nil {
		e.mu.Unlock()
		panic(err)
	}
	cuts := make(map[uint64]uint64, params.ShardNum)
	for shard := uint64(0); shard < uint64(params.ShardNum); shard++ {
		cuts[shard] = e.cuts[shard]
	}
	delta, moves := proposal.delta, proposal.moves
	plan := message.CLPAEpochPlan{Epoch: e.epoch + 1, BaseEpoch: e.epoch, Mapping: delta, InputCuts: cuts}
	plan.PlanHash = plan.Digest()
	if err := plan.Validate(uint64(params.ShardNum)); err != nil {
		e.mu.Unlock()
		panic(err)
	}
	e.plan = &plan
	e.applied = map[[2]uint64]message.CLPAEpochApplied{}
	e.firstAppliedAt = time.Time{}
	e.planDispatchAt = time.Now()
	planFields := map[string]interface{}{"epoch": plan.Epoch, "base_epoch": plan.BaseEpoch,
		"provider": e.provider.Name(), "plan_hash": plan.PlanHash, "moved_accounts": len(moves), "moves": moves,
		"mapping": delta, "mapping_semantics": "changed assignments only; cumulative mapping retained locally",
		"input_cuts": cuts, "rounds_per_shard": e.rounds,
		"graph_vertices":      len(e.ccm.clpaGraph.NetGraph.VertexSet),
		"planning_started_at": started, "plan_dispatch_at": e.planDispatchAt,
		"planning_seconds": e.planDispatchAt.Sub(started).Seconds(), "offered_due": e.offeredDue(time.Now()),
		"plan_map_snapshot_seconds": proposal.snapshotSeconds, "planner_call_seconds": proposal.plannerSeconds,
		"plan_validation_seconds": proposal.validationSeconds, "native_clpa_graph_enabled": e.provider.Name() != "forest",
		"dispatched": e.dispatched, "deferred": e.offeredDue(time.Now()) - e.dispatched,
		"planner_report": e.plannerReport()}
	if e.async != nil {
		planFields["async_pipeline"] = true
		planFields["proposal_generation"] = e.async.ready.job.generation
		planFields["base_mapping_epoch"] = e.async.ready.job.baseEpoch
		planFields["evidence_prefix"] = e.async.ready.reply.ObservedPrefix
		planFields["evidence_prefix_sha256"] = e.async.ready.reply.PrefixSHA256
		planFields["observation_to_switch_gap"] = uint64(e.dispatched) - e.async.ready.reply.ObservedPrefix
		planFields["reason"] = e.async.ready.job.reason
	}
	e.record("epoch_plan", planFields)
	e.mu.Unlock()
	e.broadcast(message.CCLPAEpochPlan, plan, true)
	deadline := time.Now().Add(e.timeout)
	for {
		e.check()
		e.mu.Lock()
		if len(e.applied) == params.ShardNum*params.NodesInShard {
			allAppliedAt := time.Now()
			reports := make([]map[string]interface{}, 0, len(e.applied))
			for shard := uint64(0); shard < uint64(params.ShardNum); shard++ {
				for node := uint64(0); node < uint64(params.NodesInShard); node++ {
					applied := e.applied[[2]uint64{shard, node}]
					report := map[string]interface{}{"shard": shard, "node": node,
						"height": applied.Height, "state_root": applied.StateRoot, "pending": applied.Pending,
						"pending_ordinary": applied.PendingOrdinary, "pending_relayed": applied.PendingRelayed,
						"pending_payload_bytes": applied.PendingPayloadBytes,
						"moved_in":              applied.MovedIn, "moved_out": applied.MovedOut}
					if e.async != nil {
						report["async_pipeline"], report["replica_role"] = applied.AsyncPipeline, applied.ReplicaRole
						report["local_rounds"], report["input_cut"] = applied.LocalRounds, applied.InputCut
						report["retained_pending"], report["retained_manifest_hash"] = applied.RetainedPending, applied.RetainedManifestHash
						report["retained_available"] = applied.RetainedAvailable
						report["incoming_pending"], report["incoming_manifest_hash"] = applied.IncomingPending, applied.IncomingManifestHash
						report["source_manifest_hash"], report["install_payload_hash"] = applied.SourceManifestHash, applied.InstallPayloadHash
						report["pending_ledger_hash"], report["pending_manifest_bytes"] = applied.PendingLedgerHash, applied.PendingManifestBytes
						report["install_payload_bytes"] = applied.InstallPayloadBytes
					}
					reports = append(reports, report)
				}
			}
			mapApplyStarted := time.Now()
			if err := e.applyPlannedMappingLocked(proposal.mapping); err != nil {
				e.mu.Unlock()
				panic(err)
			}
			mapApplySeconds := time.Since(mapApplyStarted).Seconds()
			e.epoch = plan.Epoch
			if e.appliedHistory == nil {
				e.appliedHistory = map[uint64]map[[2]uint64]message.CLPAEpochApplied{}
			}
			e.appliedHistory[plan.Epoch] = e.applied
			e.counts, e.cuts = map[uint64]int{}, map[uint64]uint64{}
			e.plan = nil
			e.record("epoch_complete", map[string]interface{}{"epoch": e.epoch, "plan_hash": plan.PlanHash,
				"applied": reports, "elapsed_seconds": time.Since(started).Seconds(),
				"planning_started_at": started, "plan_dispatch_at": e.planDispatchAt,
				"first_applied_at": e.firstAppliedAt, "all_applied_at": allAppliedAt,
				"planning_seconds":          e.planDispatchAt.Sub(started).Seconds(),
				"plan_map_snapshot_seconds": proposal.snapshotSeconds, "planner_call_seconds": proposal.plannerSeconds,
				"plan_validation_seconds": proposal.validationSeconds, "map_apply_seconds": mapApplySeconds,
				"committed_override_count": len(e.committedOverrides),
				"install_barrier_seconds":  allAppliedAt.Sub(e.planDispatchAt).Seconds(),
				"offered_due":              e.offeredDue(time.Now()), "dispatched": e.dispatched,
				"deferred": e.offeredDue(time.Now()) - e.dispatched})
			if e.async != nil {
				e.async.installed()
			}
			e.mu.Unlock()
			e.broadcast(message.CCLPAEpochResume, message.CLPAEpochResume{Epoch: plan.Epoch, PlanHash: plan.PlanHash}, false)
			e.record("epoch_resume_dispatched", map[string]interface{}{"epoch": plan.Epoch, "plan_hash": plan.PlanHash,
				"at": time.Now(), "meaning": "resume queued to transport for every replica after all apply reports"})
			return
		}
		e.mu.Unlock()
		if time.Now().After(deadline) {
			panic("CLPA all-replica apply barrier deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (e *clpaEpochController) dispatch(txs []*core.Transaction) {
	e.mu.Lock()
	byShard := map[uint64][]*core.Transaction{}
	for _, tx := range txs {
		shard := e.ccm.fetchModifiedMap(tx.Sender)
		tx.RoutingEpoch, tx.MappingVersion = int(e.epoch), e.epoch
		tx.RoutedSenderShard, tx.RoutedRecipientShard = shard, e.ccm.fetchModifiedMap(tx.Recipient)
		byShard[shard] = append(byShard[shard], tx)
		e.hashes[tx.IngressSequence] = hex.EncodeToString(tx.TxHash)
		e.record("dispatched", map[string]interface{}{"sequence": tx.IngressSequence, "epoch": e.epoch,
			"shard": shard, "tx_hash": hex.EncodeToString(tx.TxHash), "offered_at": tx.Time,
			"sender": tx.Sender, "recipient": tx.Recipient, "recipient_shard": tx.RoutedRecipientShard,
			"dispatch_delay_seconds": time.Since(tx.Time).Seconds(), "dispatch_elapsed_ns": time.Since(e.start).Nanoseconds()})
	}
	inputs := []message.CLPAEpochInput{}
	for shard := uint64(0); shard < uint64(params.ShardNum); shard++ {
		e.cuts[shard]++
		inputs = append(inputs, message.CLPAEpochInput{Epoch: e.epoch, Batch: e.cuts[shard], ToShard: shard, Txs: byShard[shard]})
	}
	e.dispatched += len(txs)
	e.mu.Unlock()
	for _, input := range inputs {
		e.send(message.CCLPAEpochInput, input, e.ccm.IpNodeTable[input.ToShard][0])
	}
	e.ccm.Ss.StopGap_Reset()
}

func (e *clpaEpochController) run() {
	if forest, ok := e.provider.(*forestEpochProvider); ok {
		defer func() {
			if err := forest.Close(); err != nil {
				e.record("forest_worker_close_error", map[string]interface{}{"error": err.Error()})
			}
		}()
	}
	if e.async != nil {
		defer e.async.Close()
	}
	if e.ccm.dataTotalNum <= 0 || params.InjectSpeed <= 0 || e.ccm.batchDataNum <= 0 {
		panic("positive CLPA input configuration required")
	}
	f, err := os.Open(e.ccm.csvPath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	e.start = time.Now()
	journalMode := "not_enabled"
	if e.async != nil {
		journalMode = "background_grouped"
	}
	e.record("run_start", map[string]interface{}{"at": e.start, "rounds_per_epoch": e.rounds,
		"provider": e.provider.Name(), "total": e.ccm.dataTotalNum, "input_sha256": e.inputHash,
		"journal_durability": journalMode,
		"clock":              "scheduled fixed offered rate; timestamps continue through repartition; audit emission may follow scheduled arrival"})
	accepted, rejected := 0, 0
	for accepted < e.ccm.dataTotalNum {
		e.check()
		e.repartition()
		due := e.offeredDue(time.Now())
		count := due - accepted
		if count < e.ccm.batchDataNum && due < e.ccm.dataTotalNum {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if count > e.ccm.batchDataNum {
			count = e.ccm.batchDataNum
		}
		if count <= 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		txs := make([]*core.Transaction, 0, count)
		observations := make([]forestEpochObservation, 0, count)
		for len(txs) < count {
			row, err := reader.Read()
			if err == io.EOF {
				panic(fmt.Sprintf("CLPA source ended early: accepted=%d expected=%d", accepted, e.ccm.dataTotalNum))
			}
			if err != nil {
				panic(err)
			}
			tx, ok := data2tx(row, uint64(accepted))
			if !ok {
				rejected++
				continue
			}
			tx.Time = e.start.Add(time.Duration(float64(accepted) / float64(params.InjectSpeed) * float64(time.Second)))
			if os.Getenv("BLOCK_EMULATOR_COMMON_BATCH_CLOCK_V1") == "1" {
				tx.Time = e.start.Add(time.Duration(float64((accepted/e.ccm.batchDataNum)*e.ccm.batchDataNum) / float64(params.InjectSpeed) * float64(time.Second)))
			}
			// Recompute because the scheduled offered timestamp is part of the
			// transaction hash; ingress metadata is set only after hashing.
			tx = core.NewTransaction(tx.Sender, tx.Recipient, tx.Value, uint64(accepted), tx.Time)
			tx.IngressSequence, tx.IngressSequenceAssigned = uint64(accepted), true
			tx.AdmissionID = "clpa/" + e.inputHash + "/" + strconv.Itoa(accepted)
			if _, forest := e.provider.(*forestEpochProvider); forest {
				observation, err := forestObservationFromCSV(row, uint64(accepted), tx.Sender, tx.Recipient)
				if err != nil {
					panic(err)
				}
				observations = append(observations, observation)
			}
			e.record("offered", map[string]interface{}{"sequence": accepted, "offered_at": tx.Time,
				"tx_hash": hex.EncodeToString(tx.TxHash), "deferred": due - e.dispatched})
			txs = append(txs, tx)
			accepted++
		}
		if e.async != nil {
			if err := e.async.append(observations); err != nil {
				panic(err)
			}
		} else if err := e.observeForestRows(observations); err != nil {
			panic(err)
		}
		e.dispatch(txs)
		if e.async != nil {
			e.async.wake()
		}
	}
	// Legacy mode has acknowledged every observation before dispatch. Async
	// mode drains its independent journal and final plan below instead.
	if forest, ok := e.provider.(*forestEpochProvider); ok && e.async == nil {
		if err := e.observeForestRows(nil); err != nil {
			panic(err)
		}
		e.record("forest_observation_end", map[string]interface{}{"observed_count": forest.observed, "prefix_sha256": forest.prefixHash, "input_sha256": e.inputHash})
		if err := forest.Close(); err != nil {
			panic(err)
		}
	}
	if e.async != nil {
		e.async.finishInput()
	}
	e.mu.Lock()
	e.drain = true
	epoch := e.epoch
	e.mu.Unlock()
	e.record("input_end", map[string]interface{}{"accepted": accepted, "rejected": rejected, "epoch": epoch})
	e.broadcast(message.CCLPAEpochDrain, message.CLPAEpochDrain{Epoch: epoch}, false)
	deadline := time.Now().Add(e.timeout)
	for {
		e.check()
		if e.async != nil {
			e.repartition()
		}
		e.mu.Lock()
		terminal := len(e.terminal)
		e.mu.Unlock()
		if terminal == accepted && (e.async == nil || e.async.finished()) {
			break
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("CLPA terminal drain incomplete: %d/%d", terminal, accepted))
		}
		time.Sleep(20 * time.Millisecond)
	}
	if e.async != nil {
		if err := e.async.Close(); err != nil {
			panic(err)
		}
		if err := e.provider.(*forestEpochProvider).Close(); err != nil {
			panic(err)
		}
	}
	e.mu.Lock()
	epoch = e.epoch
	e.mu.Unlock()
	e.record("run_complete", map[string]interface{}{"accepted": accepted, "unique_terminal": accepted,
		"epochs": epoch, "elapsed_seconds": time.Since(e.start).Seconds()})
	if f, ok := e.audit.(*os.File); ok {
		if err := f.Sync(); err != nil {
			panic(err)
		}
	}
	e.mu.Lock()
	e.runFinished = true
	e.mu.Unlock()
}

func (e *clpaEpochController) plannerReport() map[string]interface{} {
	if e.async != nil && e.async.ready != nil {
		return e.async.ready.report
	}
	return epochPlannerReport(e.provider)
}

// Keep the generic supervisor's existing stop loop, but forbid an incomplete
// occurrence set from satisfying its idle heuristic.
func (ccm *CLPACommitteeModule) GroupCompletionReady() bool {
	if ccm.epochV1 == nil {
		return true
	}
	e := ccm.epochV1
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.failure == nil && e.drain && len(e.terminal) == ccm.dataTotalNum && (e.async == nil || (e.async.finishedLocked() && e.runFinished))
}

func (ccm *CLPACommitteeModule) NativeCloseoutError() error {
	if ccm.epochV1 == nil {
		return nil
	}
	e := ccm.epochV1
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.failure
}

// Keep an explicit digest helper available for provider contract tests.
func clpaEpochMappingDigest(mapping map[string]uint64) string {
	// No deviations and an empty deviations table represent the same layout.
	// Keep Go's nil map distinct from JSON null on the Python wire contract.
	if mapping == nil {
		mapping = map[string]uint64{}
	}
	raw, err := json.Marshal(mapping)
	if err != nil {
		panic(err)
	}
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}
