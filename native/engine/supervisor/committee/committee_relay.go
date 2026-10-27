package committee

import (
	"blockEmulator/core"
	"blockEmulator/initialledger"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/ownership"
	"blockEmulator/params"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type RelayCommitteeModule struct {
	ownership                   *ownership.View    // Pinned public authorization, not Planner signing authority.
	livePlan                    *livePlacementPlan // Causal batch plan; only the source producer mutates it.
	groupControl                *nativeGroupControl
	nativePlacementVersion      uint64
	nativeBatch                 bool
	nativeFixedRoutes           bool // fixed history+hash routes; batch transport is not a placement policy
	nativeRoutes                map[string]uint64
	nativeClock                 time.Time
	nativeCount                 int
	nativeInputHash             string // Input identity pinned by the native producer.
	newNewWork                  *newNewWorkLedger
	placementSignals            *placementSignals
	newNewPolicy                *newNewPlacementPolicy // optional NN-only scorer; never owns old routes
	placementSequence           uint64                 // current occurrence, owned by the input producer
	forestFacts                 *ForestFactsV2         // facts-only v2; does not select routes
	forestJoint                 *forestJointCacheV2
	forestSource                []int64 // independently pinned source clock, never a placement policy
	nativeAudit                 *batchAudit
	finiteHook                  *finiteAcceptanceHook // opt-in acceptance instrumentation, source producer owns pointer
	finitePlan                  *finitePlan           // immutable pointer published by constructor
	streamFirstPlacement        bool
	streamPending               []*core.Transaction // single source producer owns queue
	initialLedger               *initialledger.Ledger
	firstAdmission              *firstAdmissionRuntime
	csvPath                     string
	dataTotalNum                int
	nowDataNum                  int
	batchDataNum                int
	dispatchRound               int
	IpNodeTable                 map[uint64]map[uint64]string
	sl                          *supervisor_log.SupervisorLog
	Ss                          *signal.StopSignal // to control the stop message sending
	admission                   *RuntimeUnknownAdmissionManager
	observedEpoch               int64
	shardEpochMu                sync.Mutex
	shardEpoch                  map[uint64]int64
	offeredLoadTPS              float64
	arrivalClock                *arrivalSchedule
	lifecycleMu                 sync.Mutex
	lifecycle                   map[string]*ordinaryLifecycle
	offeredStart                time.Time
	offeredSent                 int64
	offeredAuditPath            string
	epochAwareDispatchEnabled   bool
	epochAwareDispatchTimeout   time.Duration
	epochAwareDispatchAuditPath string
}

func NewRelayCommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, slog *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum int) *RelayCommitteeModule {
	module := &RelayCommitteeModule{
		csvPath:       csvFilePath,
		dataTotalNum:  dataNum,
		batchDataNum:  batchNum,
		nowDataNum:    0,
		IpNodeTable:   Ip_nodeTable,
		Ss:            Ss,
		sl:            slog,
		dispatchRound: 0,
		admission:     NewRuntimeUnknownAdmissionManager(uint64(params.ShardNum), slog),
		shardEpoch:    make(map[uint64]int64, params.ShardNum),
	}
	if os.Getenv("SAA_FIRST_PLACEMENT_FENCE") == "true" {
		module.firstAdmission = newFirstAdmissionRuntime(module)
	}
	ledger, err := initialledger.FromEnv(uint64(params.ShardNum))
	if err != nil {
		panic(err)
	}
	if ledger != nil && (module.admission.Enabled() || module.admission.dynamicAdmissionEnabled || module.admission.externalActionEnabled || module.admission.mappingBoundaryEnabled || module.admission.softDecisionMode == saaIntegratedDecisionMode) {
		panic("initial ledger v1 requires fixed mapping control")
	}
	module.initialLedger = ledger
	module.streamFirstPlacement = os.Getenv("SAA_STREAM_FIRST_PLACEMENT") == "true"
	if module.streamFirstPlacement && (ledger == nil || module.firstAdmission == nil) {
		panic("stream first placement requires initial ledger and admission fence")
	}
	module.offeredLoadTPS = parseOfferedLoadTPS(os.Getenv("BLOCK_EMULATOR_OFFERED_LOAD_TPS"))
	module.offeredAuditPath = strings.TrimSpace(os.Getenv("BLOCK_EMULATOR_OFFERED_LOAD_AUDIT_PATH"))
	if module.streamFirstPlacement && (module.offeredLoadTPS <= 0 || module.offeredAuditPath == "") {
		panic("stream first placement requires paced audited input")
	}
	module.epochAwareDispatchEnabled = parseEpochAwareDispatchEnabled(
		os.Getenv("BLOCK_EMULATOR_EPOCH_AWARE_DISPATCH_ENABLED"),
	)
	module.epochAwareDispatchTimeout = parseEpochAwareDispatchTimeout(
		os.Getenv("BLOCK_EMULATOR_EPOCH_AWARE_DISPATCH_TIMEOUT_MS"),
	)
	module.epochAwareDispatchAuditPath = strings.TrimSpace(
		os.Getenv("BLOCK_EMULATOR_EPOCH_AWARE_DISPATCH_AUDIT_PATH"),
	)
	effectiveConfiguration := module.admission.effectiveConfiguration()
	effectiveConfiguration["epoch_aware_dispatch_enabled"] = module.epochAwareDispatchEnabled
	if ledger != nil {
		effectiveConfiguration["initial_ledger_sha256"] = ledger.FileHash
		effectiveConfiguration["initial_logical_ledger_sha256"] = ledger.LogicalHash
	}
	effectiveConfiguration["offered_load_tps"] = module.offeredLoadTPS
	effectiveConfiguration["offered_load_contract"] = "pre_admission_per_occurrence_v1"
	effectiveConfiguration["epoch_aware_dispatch_timeout_ms"] = module.epochAwareDispatchTimeout.Milliseconds()
	writeRuntimeEffectiveConfiguration("relay_committee", effectiveConfiguration)
	if module.offeredLoadTPS > 0 && module.sl != nil {
		module.sl.Slog.Printf("Explicit wall-clock offered-load target enabled: %.3f tx/s\n", module.offeredLoadTPS)
	}
	if module.epochAwareDispatchEnabled && module.sl != nil {
		module.sl.Slog.Printf(
			"Epoch-aware canary dispatch enabled: timeout=%s audit=%s\n",
			module.epochAwareDispatchTimeout,
			module.epochAwareDispatchAuditPath,
		)
	}
	if err := module.configureFinitePlan(); err != nil {
		panic(err)
	}
	module.nativeBatch = os.Getenv("SAA_NATIVE_BATCH_AUDIT_V1") == "true"
	module.nativeFixedRoutes, err = fixedNativeRoutesFromEnv()
	if err != nil {
		panic(err)
	}
	if module.nativeFixedRoutes && !module.nativeBatch {
		panic("fixed native routes require the existing native batch input")
	}
	if module.nativeBatch {
		if module.firstAdmission != nil || module.streamFirstPlacement || module.finitePlan != nil || module.epochAwareDispatchEnabled || ledger == nil || module.offeredLoadTPS <= 0 {
			panic("native batch requires fixed ledger, positive offered rate, and no admission fence/finite gate/epoch wait")
		}
		module.nativeRoutes = make(map[string]uint64)
		module.nativeAudit = newBatchAudit(module.offeredAuditPath+".lifecycle.v1.jsonl", os.Getenv("SAA_NATIVE_INPUT_SHA256"))
	}
	if err := module.configureGroupControl(); err != nil {
		panic(err)
	}
	if module.nativeFixedRoutes && module.groupControl != nil {
		panic("fixed native routes cannot be combined with a migration controller")
	}
	if err := module.configureOwnershipSecurity(); err != nil {
		panic(err)
	}
	module.logOwnershipStartup()
	if module.nativeBatch {
		module.initializeNewNewWork()
	}
	if os.Getenv("SAA_PLACEMENT_TELEMETRY_V1") == "true" {
		if !module.nativeBatch {
			panic("placement telemetry requires audited native batch input")
		}
		module.placementSignals = newPlacementSignals(params.ShardNum)
	}
	if err := module.configureNewNewPolicy(); err != nil {
		panic(err)
	}
	return module
}

func (rthm *RelayCommitteeModule) appendOfferedLoadAudit(chunkSize int, sentAt time.Time) {
	if rthm.offeredAuditPath == "" || rthm.offeredStart.IsZero() {
		return
	}
	file, err := os.OpenFile(rthm.offeredAuditPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	if info, err := file.Stat(); err == nil && info.Size() == 0 {
		_ = writer.Write([]string{"sent_at_utc", "chunk_tx_count", "cumulative_tx_count", "elapsed_seconds", "realized_cumulative_tps", "target_tps"})
	}
	elapsed := sentAt.Sub(rthm.offeredStart).Seconds()
	realized := 0.0
	if elapsed > 0 {
		realized = float64(rthm.offeredSent) / elapsed
	}
	_ = writer.Write([]string{
		sentAt.UTC().Format(time.RFC3339Nano),
		strconv.Itoa(chunkSize),
		strconv.FormatInt(rthm.offeredSent, 10),
		strconv.FormatFloat(elapsed, 'f', 6, 64),
		strconv.FormatFloat(realized, 'f', 6, 64),
		strconv.FormatFloat(rthm.offeredLoadTPS, 'f', 6, 64),
	})
	writer.Flush()
}

// parseOfferedLoadTPS returns zero when explicit pacing is disabled.
func parseOfferedLoadTPS(raw string) float64 {
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value
}

func parseEpochAwareDispatchEnabled(raw string) bool {
	enabled, err := strconv.ParseBool(strings.TrimSpace(raw))
	return err == nil && enabled
}

func parseEpochAwareDispatchTimeout(raw string) time.Duration {
	const defaultTimeout = 60000
	milliseconds, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || milliseconds < 1000 {
		milliseconds = defaultTimeout
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func (rthm *RelayCommitteeModule) appendEpochAwareDispatchAudit(
	dispatchRound int,
	batchSize int,
	startEpoch int64,
	targetEpoch int64,
	observedEpoch int64,
	status string,
	waited time.Duration,
) {
	if rthm.epochAwareDispatchAuditPath == "" {
		return
	}
	file, err := os.OpenFile(rthm.epochAwareDispatchAuditPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	if info, err := file.Stat(); err == nil && info.Size() == 0 {
		_ = writer.Write([]string{
			"dispatch_round", "batch_tx_count", "start_observed_epoch", "target_observed_epoch",
			"observed_epoch", "status", "waited_ms", "recorded_at_utc",
		})
	}
	_ = writer.Write([]string{
		strconv.Itoa(dispatchRound),
		strconv.Itoa(batchSize),
		strconv.FormatInt(startEpoch, 10),
		strconv.FormatInt(targetEpoch, 10),
		strconv.FormatInt(observedEpoch, 10),
		status,
		strconv.FormatInt(waited.Milliseconds(), 10),
		time.Now().UTC().Format(time.RFC3339Nano),
	})
	writer.Flush()
}

// waitForObservedEpochAtLeast waits for an all-shard complete commit watermark.
// It is canary-only: formal performance runs keep their explicit exogenous pacing.
func (rthm *RelayCommitteeModule) waitForObservedEpochAtLeast(
	dispatchRound int,
	batchSize int,
	startEpoch int64,
) bool {
	if !rthm.epochAwareDispatchEnabled {
		return true
	}
	targetEpoch := startEpoch + 1
	started := time.Now()
	deadline := started.Add(rthm.epochAwareDispatchTimeout)
	for {
		observedEpoch := atomic.LoadInt64(&rthm.observedEpoch)
		if observedEpoch >= targetEpoch {
			rthm.appendEpochAwareDispatchAudit(
				dispatchRound, batchSize, startEpoch, targetEpoch, observedEpoch,
				"COMPLETE_WATERMARK_REACHED", time.Since(started),
			)
			return true
		}
		if !time.Now().Before(deadline) {
			rthm.appendEpochAwareDispatchAudit(
				dispatchRound, batchSize, startEpoch, targetEpoch, observedEpoch,
				"TIMEOUT", time.Since(started),
			)
			if rthm.sl != nil {
				rthm.sl.Slog.Printf(
					"Epoch-aware dispatch timeout: round=%d batchTx=%d startEpoch=%d targetEpoch=%d observedEpoch=%d timeout=%s\n",
					dispatchRound, batchSize, startEpoch, targetEpoch, observedEpoch,
					rthm.epochAwareDispatchTimeout,
				)
			}
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// paceOfferedLoad makes the exogenous arrival schedule independent of routing
// and mapping-decision compute time. Without this limiter, a blocking control
// path silently throttles the workload and invalidates cross-method comparisons.
func (rthm *RelayCommitteeModule) paceOfferedLoad(sent int) {
	if rthm.offeredLoadTPS <= 0 || sent <= 0 {
		time.Sleep(time.Second)
		return
	}
	if rthm.offeredStart.IsZero() {
		rthm.offeredStart = time.Now()
	}
	rthm.offeredSent += int64(sent)
	rthm.appendOfferedLoadAudit(sent, time.Now())
	deadline := rthm.offeredStart.Add(time.Duration(float64(rthm.offeredSent) / rthm.offeredLoadTPS * float64(time.Second)))
	if remaining := time.Until(deadline); remaining > 0 {
		time.Sleep(remaining)
	}
}

func isValidNativeEOA(value string) bool {
	if len(value) != 42 || !strings.EqualFold(value[:2], "0x") {
		return false
	}
	_, err := hex.DecodeString(value[2:])
	return err == nil
}

// transfrom, data to transaction
// check whether it is a legal txs meesage. if so, read txs and put it into the txlist
func data2tx(data []string, nonce uint64) (*core.Transaction, bool) {
	if len(data) < 9 || data[6] != "0" || data[7] != "0" {
		return &core.Transaction{}, false
	}
	if !isValidNativeEOA(data[3]) || !isValidNativeEOA(data[4]) || strings.EqualFold(data[3], data[4]) {
		return &core.Transaction{}, false
	}
	val, ok := new(big.Int).SetString(data[8], 10)
	if !ok || val.Sign() < 0 {
		return &core.Transaction{}, false
	}
	tx := core.NewTransaction(data[3][2:], data[4][2:], val, nonce, time.Now())
	return tx, true
}
func (rthm *RelayCommitteeModule) HandleOtherMessage(raw []byte) {
	t, content := message.SplitMessage(raw)
	if t == message.CPlacementTelemetryV2 { rthm.handleLightTelemetryV2(content) }
}

func (rthm *RelayCommitteeModule) txSending(txlist []*core.Transaction) {
	if len(txlist) == 0 {
		return
	}
	defer func() { rthm.dispatchRound++ }()
	if rthm.nativeBatch {
		rthm.sendNativeBatch(txlist)
		return
	}
	if rthm.offeredLoadTPS > 0 {
		rthm.submitScheduled(txlist, rthm.txSendingUnpaced)
		return
	}
	rthm.txSendingUnpaced(txlist)
}

func (rthm *RelayCommitteeModule) txSendingUnpaced(txlist []*core.Transaction) {
	if rthm.streamFirstPlacement {
		rthm.submitFirstPlacementStream(txlist)
		return
	}
	if rthm.firstAdmission != nil {
		rthm.firstAdmission.submitOrdered(txlist)
		return
	}
	// the txs will be sent
	sendToShard := make(map[uint64][]*core.Transaction)
	chunkSize := params.InjectSpeed
	if rthm.offeredLoadTPS > 0 && int(rthm.offeredLoadTPS) < chunkSize {
		chunkSize = int(rthm.offeredLoadTPS)
	}
	if chunkSize < 1 {
		chunkSize = 1
	}
	chunkStart := 0

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%chunkSize == 0 || idx == len(txlist)) {
			// send to shard
			for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
				if rthm.firstAdmission != nil && len(sendToShard[sid]) == 0 {
					continue
				}
				it := message.InjectTxs{
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				send_msg := message.MergeMessage(message.CInject, itByte)
				batch := append([]*core.Transaction(nil), sendToShard[sid]...)
				if rthm.offeredAuditPath != "" && len(batch) > 0 {
					go networks.TcpDialObserved(send_msg, rthm.IpNodeTable[sid][0], func(event string, at time.Time, err error) {
						rthm.auditTransport(batch, event, at, err)
					})
				} else {
					go networks.TcpDial(send_msg, rthm.IpNodeTable[sid][0])
				}
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			if rthm.offeredLoadTPS <= 0 {
				rthm.paceOfferedLoad(idx - chunkStart)
			}
			chunkStart = idx
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		rthm.auditAccepted(tx)
		if rthm.firstAdmission != nil && !rthm.firstAdmission.admit(tx) {
			continue
		}
		sendersid := uint64(utils.Addr2Shard(tx.Sender))
		recipientsid := uint64(utils.Addr2Shard(tx.Recipient))
		if rthm.initialLedger != nil {
			if s, ok := rthm.initialLedger.Routes[nativeAdmissionAccount(tx.Sender)]; ok {
				sendersid = s
				tx.SenderRequiresExistingState = true
			}
			if s, ok := rthm.initialLedger.Routes[nativeAdmissionAccount(tx.Recipient)]; ok {
				recipientsid = s
				tx.RecipientRequiresExistingState = true
			}
		}
		// Transactions are assigned to the routing version following the last
		if rthm.nativeBatch {
			rthm.placementSequence = tx.IngressSequence
			sendersid, recipientsid = rthm.nativePlacement(tx.Sender, tx.Recipient)
		}
		// epoch completed by every shard. A fast shard must not advance the
		// global mapping boundary on behalf of slower shards.
		observedEpoch := int(atomic.LoadInt64(&rthm.observedEpoch)) + 1
		if rthm.firstAdmission == nil && rthm.admission != nil && rthm.admission.Enabled() {
			if admittedShard, ok := rthm.admission.RouteSender(tx, observedEpoch); ok && admittedShard < uint64(params.ShardNum) {
				sendersid = admittedShard
			}
			if admittedShard, ok := rthm.admission.CurrentShard(tx.Recipient, observedEpoch); ok && admittedShard < uint64(params.ShardNum) {
				recipientsid = admittedShard
			}
		}
		// Freeze both route endpoints at injection. Mapping M_{t+1} may be
		// committed while this transaction is still queued, but it must not
		// retroactively change the transaction's already selected execution path.
		tx.RoutingFrozen = true
		tx.MappingVersion = 0
		if rthm.nativeBatch {
			tx.MappingVersion = rthm.nativePlacementVersion
		}
		if rthm.admission != nil && rthm.admission.Enabled() {
			tx.MappingVersion = rthm.admission.CurrentRouteVersion()
		}
		tx.RoutingEpoch = observedEpoch
		tx.RoutedSenderShard = sendersid
		tx.RoutedRecipientShard = recipientsid
		if rthm.firstAdmission != nil {
			rthm.firstAdmission.bindFixedRoute(tx, observedEpoch)
			sendersid = tx.RoutedSenderShard
		}
		if rthm.admission != nil && rthm.admission.Enabled() {
			if err := rthm.admission.recordForecastRoutedInput(tx); err != nil {
				panic(fmt.Sprintf("forecast input stream failed: %v", err))
			}
		}
		if rthm.firstAdmission != nil {
			rthm.firstAdmission.routed(tx)
		}
		sendToShard[sendersid] = append(sendToShard[sendersid], tx)
		rthm.auditRoute(tx)
	}
}

// read transactions, the Number of the transactions is - batchDataNum
func (rthm *RelayCommitteeModule) MsgSendingControl() {
	if rthm.firstAdmission != nil {
		if err := rthm.firstAdmission.start(); err != nil {
			panic(err)
		}
	}
	txfile, err := os.Open(rthm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)

	for {
		if rthm.finitePlan.closed() {
			return
		}
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		if tx, ok := data2tx(data, uint64(rthm.nowDataNum)); ok {
			tx.IngressSequence = uint64(rthm.nowDataNum)
			txlist = append(txlist, tx)
			rthm.nowDataNum++
		}

		// re-shard condition, enough edges
		if shouldFlushTxBatch(len(txlist), rthm.nowDataNum, rthm.dataTotalNum, rthm.batchDataNum, rthm.dispatchRound) {
			if rthm.firstAdmission != nil && os.Getenv("SAA_FINITE_ACCEPTANCE_V1") == "true" {
				rthm.firstAdmission.record("finite_input_batch", map[string]interface{}{"batch": rthm.dispatchRound, "count": len(txlist), "read_total": rthm.nowDataNum})
			}
			if rthm.dispatchRound == 0 && len(txlist) < rthm.batchDataNum {
				rthm.sl.Slog.Printf("Supervisor bootstrap dispatch: txs=%d target=%d steadyBatch=%d\n", len(txlist), bootstrapDispatchTarget(rthm.batchDataNum), rthm.batchDataNum)
			}
			dispatchStartEpoch := atomic.LoadInt64(&rthm.observedEpoch)
			dispatchRound := rthm.dispatchRound
			isFinalBatch := rthm.nowDataNum == rthm.dataTotalNum
			rthm.txSending(txlist)
			if rthm.finitePlan.closed() {
				return
			}
			if rthm.epochAwareDispatchEnabled && !isFinalBatch &&
				!rthm.waitForObservedEpochAtLeast(dispatchRound, len(txlist), dispatchStartEpoch) {
				panic(fmt.Sprintf(
					"epoch-aware dispatch failed after round %d: start complete watermark %d did not advance",
					dispatchRound,
					dispatchStartEpoch,
				))
			}
			if isFinalBatch && rthm.admission != nil {
				// The final batch must first cross its routing boundary. Closing the
				// channel before injection suppresses its pending READY/apply path.
				finalLogicalRound := int(atomic.LoadInt64(&rthm.observedEpoch)) + 1
				rthm.admission.CloseMappingDecisions(finalLogicalRound)
			}
			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			rthm.Ss.StopGap_Reset()
		}

		if rthm.nowDataNum == rthm.dataTotalNum {
			break
		}
	}
	if rthm.streamFirstPlacement {
		rthm.drainFirstPlacementStream()
	}
	if rthm.finitePlan.closed() {
		return
	}
	if rthm.firstAdmission != nil && rthm.firstAdmission.ledger.HeldCount() > 0 {
		panic("first placement HOLD remains: workload is not complete")
	}
	if rthm.admission != nil {
		if err := rthm.admission.finishForecastInput(uint64(rthm.nowDataNum)); err != nil {
			panic(fmt.Sprintf("forecast input completion failed: %v", err))
		}
		if err := rthm.admission.drainForecastConsumers(int(atomic.LoadInt64(&rthm.observedEpoch))+1, 30*time.Second); err != nil {
			panic(fmt.Sprintf("forecast consumer completion failed: %v", err))
		}
	}
}

// no operation here
func (rthm *RelayCommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	if rthm.placementSignals != nil {
		received := rthm.placementSignals.clock.now()
		if rthm.placementSignals.observe(b, received) && rthm.nativeAudit != nil {
			rthm.nativeAudit.write(map[string]interface{}{"event": "placement_queue_sample", "at": received.Wall.UTC(),
				"clock_schema": placementClockSchema, "clock_id": received.ClockID, "received_mono_ns": received.MonoNS,
				"shard": b.SenderShardID, "height": b.Epoch, "block_hash": b.CommittedBlockHash,
				"queue": b.TxPoolSize, "captured_at": b.TxPoolObservedAt, "commit_at": b.CommitTime,
				"body_length": b.BlockBodyLength, "scope": "leader-local pending queue; overlaps unconfirmed work"})
		}
	}
	if rthm.firstAdmission == nil {
		rthm.auditBlock(b)
	}
	if rthm.firstAdmission != nil {
		rthm.firstAdmission.report(b)
	}
	if rthm.admission != nil {
		rthm.admission.observeForecastCommit(b)
	}
	rthm.shardEpochMu.Lock()
	if int64(b.Epoch) > rthm.shardEpoch[b.SenderShardID] {
		rthm.shardEpoch[b.SenderShardID] = int64(b.Epoch)
	}
	globalCompleted := int64(0)
	if len(rthm.shardEpoch) == params.ShardNum {
		globalCompleted = int64(^uint64(0) >> 1)
		for shardID := uint64(0); shardID < uint64(params.ShardNum); shardID++ {
			epoch := rthm.shardEpoch[shardID]
			if epoch < globalCompleted {
				globalCompleted = epoch
			}
		}
	}
	rthm.shardEpochMu.Unlock()
	for {
		current := atomic.LoadInt64(&rthm.observedEpoch)
		if globalCompleted <= current {
			break
		}
		if atomic.CompareAndSwapInt64(&rthm.observedEpoch, current, globalCompleted) {
			break
		}
	}
	rthm.admission.pollNativeControlAtObservedEpoch(int(atomic.LoadInt64(&rthm.observedEpoch)))
	rthm.sl.Slog.Printf(
		"received from shard %d in epoch %d; global completed epoch %d.\n",
		b.SenderShardID,
		b.Epoch,
		atomic.LoadInt64(&rthm.observedEpoch),
	)
}
