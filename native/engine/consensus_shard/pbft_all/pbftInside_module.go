// addtional module for new consensus
package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"
)

// simple implementation of pbftHandleModule interface ...
// only for block request and use transaction relay
type RawRelayPbftExtraHandleMod struct {
	pbftNode *PbftConsensusNode
	// pointer to pbft data
}

// propose request with different types
func (rphm *RawRelayPbftExtraHandleMod) HandleinPropose() (bool, *message.Request) {
	proposeStart := time.Now()
	currentRound := int(rphm.pbftNode.sequenceID)
	stageStart := time.Now()
	if err := rphm.pbftNode.observeRequestedQueue(); err != nil {
		rphm.pbftNode.pl.Plog.Printf("one-shot queue observation unavailable: %v", err)
	}
	observeMs := time.Since(stageStart).Milliseconds()
	stageStart = time.Now()
	if r, err := rphm.pbftNode.groupProposal(); err != nil {
		rphm.pbftNode.pl.Plog.Printf("group control paused: %v", err)
	} else if r != nil {
		return true, r
	}
	groupMs := time.Since(stageStart).Milliseconds()
	stageStart = time.Now()
	if err := rphm.pbftNode.pollSourceHandoff(); err != nil {
		rphm.pbftNode.pl.Plog.Printf("source handoff paused: %v", err)
	}
	pollMs := time.Since(stageStart).Milliseconds()
	stageStart = time.Now()
	if r, err := rphm.pbftNode.groupRetirementProposal(); err != nil {
		rphm.pbftNode.pl.Plog.Printf("group retirement paused: %v", err)
	} else if r != nil {
		return true, r
	}
	retirementMs := time.Since(stageStart).Milliseconds()
	stageStart = time.Now()
	if r, err := rphm.pbftNode.handoffProposal(); err != nil {
		rphm.pbftNode.pl.Plog.Printf("handoff proposal rejected: %v", err)
	} else if r != nil {
		return true, r
	}
	handoffMs := time.Since(stageStart).Milliseconds()
	stageStart = time.Now()
	if r, err := rphm.pbftNode.sourceRetirementProposal(); err != nil {
		rphm.pbftNode.pl.Plog.Printf("source retirement paused: %v", err)
	} else if r != nil {
		return true, r
	}
	sourceRetirementMs := time.Since(stageStart).Milliseconds()
	stageStart = time.Now()
	if r, err := rphm.pbftNode.targetActivationProposal(); err != nil {
		rphm.pbftNode.pl.Plog.Printf("target activation rejected: %v", err)
	} else if r != nil {
		return true, r
	}
	activationMs := time.Since(stageStart).Milliseconds()
	stageStart = time.Now()
	rphm.pbftNode.CurChain.SeedPartitionMapFromClusterAssignmentIfEnabled()
	if request, err := rphm.pbftNode.targetInstallProposal(); err != nil {
		rphm.pbftNode.pl.Plog.Printf("SEF target installation rejected: %v", err)
	} else if request != nil {
		return true, request
	}
	installMs := time.Since(stageStart).Milliseconds()
	stageStart = time.Now()
	if request, err := rphm.pbftNode.sourcePrepareProposal(); err != nil {
		rphm.pbftNode.pl.Plog.Printf("SEF source preparation rejected: %v", err)
	} else if request != nil {
		return true, request
	}
	prepareMs := time.Since(stageStart).Milliseconds()
	seedElapsed := time.Since(proposeStart)
	if seedElapsed > time.Second {
		rphm.pbftNode.pl.Plog.Printf("proposal_seed_timing round=%d observeMs=%d groupMs=%d pollMs=%d retireMs=%d handoffMs=%d sourceRetireMs=%d activateMs=%d installMs=%d prepareMs=%d totalMs=%d", currentRound, observeMs, groupMs, pollMs, retirementMs, handoffMs, sourceRetirementMs, activationMs, installMs, prepareMs, seedElapsed.Milliseconds())
	}
	applyStart := time.Now()
	rphm.applyExternalMappingUpdatesAtCommitBoundary(currentRound)
	applyElapsed := time.Since(applyStart)
	snapshotStart := time.Now()
	rphm.pbftNode.CurChain.ExportNativeEpochSnapshotIfEnabled(
		currentRound,
		rphm.pbftNode.ShardID,
		rphm.pbftNode.NodeID,
		rphm.pbftNode.NodeID == uint64(rphm.pbftNode.view.Load()),
	)
	snapshotElapsed := time.Since(snapshotStart)
	// new blocks
	blockStart := time.Now()
	block := rphm.pbftNode.CurChain.GenerateBlock(int32(rphm.pbftNode.NodeID))
	blockElapsed := time.Since(blockStart)
	rphm.pbftNode.pl.Plog.Printf(
		"S%dN%d proposal pipeline round=%d seedMs=%d applyMs=%d snapshotMs=%d generateBlockMs=%d totalMs=%d\n",
		rphm.pbftNode.ShardID,
		rphm.pbftNode.NodeID,
		currentRound,
		seedElapsed.Milliseconds(),
		applyElapsed.Milliseconds(),
		snapshotElapsed.Milliseconds(),
		blockElapsed.Milliseconds(),
		time.Since(proposeStart).Milliseconds(),
	)
	r := &message.Request{
		RequestType: message.BlockRequest,
		ReqTime:     time.Now(),
	}
	rphm.pbftNode.notePlacementProposalV2(uint64(currentRound), r.ReqTime)
	r.Msg.Content = block.Encode()

	return true, r
}

// the DIY operation in preprepare
func (rphm *RawRelayPbftExtraHandleMod) HandleinPrePrepare(ppmsg *message.PrePrepare) bool {
	if redirectRequest(ppmsg.RequestMsg) {
		if string(getDigest(ppmsg.RequestMsg)) != string(ppmsg.Digest) || rphm.pbftNode.validateRedirect(ppmsg.RequestMsg) != nil {
			return false
		}
		rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
		return true
	}
	if groupKind(ppmsg.RequestMsg.RequestType) {
		if rphm.pbftNode.validateGroupRequest(ppmsg.RequestMsg, ppmsg.SeqID, ppmsg.Digest) != nil {
			return false
		}
		rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
		return true
	}
	if ppmsg.RequestMsg.RequestType == sourceRetirementRequest {
		if _, err := rphm.pbftNode.validateSourceRetirementRequest(ppmsg.RequestMsg); err != nil || string(getDigest(ppmsg.RequestMsg)) != string(ppmsg.Digest) {
			return false
		}
		rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
		return true
	}
	if auxiliaryBatch(ppmsg.RequestMsg) {
		if string(getDigest(ppmsg.RequestMsg)) != string(ppmsg.Digest) || rphm.pbftNode.validateAuxiliaryBatch(ppmsg.RequestMsg) != nil {
			return false
		}
		rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
		return true
	}
	if ppmsg.RequestMsg.RequestType == inboundDebitRequest {
		c, err := decodeInboundDebit(ppmsg.RequestMsg)
		if err != nil || string(getDigest(ppmsg.RequestMsg)) != string(ppmsg.Digest) || rphm.pbftNode.CurChain.ValidateInboundDebit(c.Transaction, c.MappingVersion) != nil {
			return false
		}
		rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
		return true
	}
	if ppmsg.RequestMsg.RequestType == handoffRelayRequest {
		first, err := decodeHandoffRelay(ppmsg.RequestMsg)
		if err != nil || string(getDigest(ppmsg.RequestMsg)) != string(ppmsg.Digest) || rphm.pbftNode.CurChain.ValidateHandoffRelay(first) != nil {
			return false
		}
		rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
		return true
	}
	if ppmsg.RequestMsg.RequestType == migrationHandoffBatchRequest {
		hs, err := decodeMigrationHandoffBatch(ppmsg.RequestMsg)
		if err != nil || string(getDigest(ppmsg.RequestMsg)) != string(ppmsg.Digest) || rphm.pbftNode.CurChain.ValidateMigrationHandoffBatch(hs) != nil {
			return false
		}
		rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
		return true
	}
	if ppmsg.RequestMsg.RequestType == migrationHandoffRequest {
		h, err := decodeMigrationHandoff(ppmsg.RequestMsg)
		if err != nil || string(getDigest(ppmsg.RequestMsg)) != string(ppmsg.Digest) || rphm.pbftNode.CurChain.ValidateMigrationHandoffExecution(h) != nil {
			return false
		}
		rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
		return true
	}
	if ppmsg.RequestMsg.RequestType == targetActivationRequest {
		c, err := decodeTargetActivation(ppmsg.RequestMsg)
		if err != nil || c.TargetShard != rphm.pbftNode.ShardID || string(getDigest(ppmsg.RequestMsg)) != string(ppmsg.Digest) || rphm.pbftNode.CurChain.ValidateTargetActivation(c) != nil {
			return false
		}
		rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
		return true
	}
	if ppmsg.RequestMsg.RequestType == targetInstallRequest {
		p, err := decodeTargetInstall(ppmsg.RequestMsg)
		if err != nil || string(getDigest(ppmsg.RequestMsg)) != string(ppmsg.Digest) || rphm.pbftNode.CurChain.ValidatePreparedTarget(p, ppmsg.SeqID, ppmsg.Digest) != nil {
			return false
		}
		rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
		return true
	}
	if ppmsg.RequestMsg.RequestType == sourcePrepareRequest {
		c, err := decodeSourcePrepare(ppmsg.RequestMsg)
		if err != nil || rphm.pbftNode.CurChain.ValidateSourcePreparation(c) != nil {
			return false
		}
		rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
		return true
	}
	if rphm.pbftNode.CurChain.IsValidBlock(core.DecodeB(ppmsg.RequestMsg.Msg.Content)) != nil {
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : not a valid block\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
		return false
	}
	rphm.pbftNode.pl.Plog.Printf("S%dN%d : the pre-prepare message is correct, putting it into the RequestPool. \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
	rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
	// merge to be a prepare message
	return true
}

// the operation in prepare, and in pbft + tx relaying, this function does not need to do any.
func (rphm *RawRelayPbftExtraHandleMod) HandleinPrepare(pmsg *message.Prepare) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation in commit.
func (rphm *RawRelayPbftExtraHandleMod) HandleinCommit(cmsg *message.Commit) bool {
	r := rphm.pbftNode.requestPool[string(cmsg.Digest)]
	if redirectRequest(r) {
		if err := rphm.pbftNode.commitRedirect(r, cmsg.Digest); err != nil {
			panic(err)
		}
		return true
	}
	if groupKind(r.RequestType) {
		if err := rphm.pbftNode.commitGroupRequest(r, cmsg.SeqID, cmsg.Digest); err != nil {
			panic(err)
		}
		return true
	}
	if r.RequestType == sourceRetirementRequest {
		if err := rphm.pbftNode.commitSourceRetirement(r, cmsg.SeqID, cmsg.Digest); err != nil {
			panic(err)
		}
		return true
	}
	if auxiliaryBatch(r) {
		if err := rphm.pbftNode.commitAuxiliaryBatch(r, cmsg.Digest); err != nil {
			panic(err)
		}
		return true
	}
	if r.RequestType == inboundDebitRequest {
		if err := rphm.pbftNode.commitInboundDebit(r, cmsg.Digest); err != nil {
			panic(err)
		}
		return true
	}
	if r.RequestType == handoffRelayRequest {
		if err := rphm.pbftNode.commitHandoffRelay(r, cmsg.Digest); err != nil {
			panic(err)
		}
		return true
	}
	if r.RequestType == migrationHandoffBatchRequest {
		if err := rphm.pbftNode.commitMigrationHandoffBatch(r, cmsg.Digest); err != nil {
			panic(err)
		}
		return true
	}
	if r.RequestType == migrationHandoffRequest {
		if err := rphm.pbftNode.commitMigrationHandoff(r, cmsg.Digest); err != nil {
			panic(err)
		}
		return true
	}
	if r.RequestType == targetActivationRequest {
		if err := rphm.pbftNode.commitTargetActivation(r, cmsg.Digest); err != nil {
			panic(err)
		}
		return true
	}
	if r.RequestType == targetInstallRequest {
		if err := rphm.pbftNode.commitTargetInstall(r, cmsg.SeqID, cmsg.Digest); err != nil {
			panic(err)
		}
		return true
	}
	if r.RequestType == sourcePrepareRequest {
		if err := rphm.pbftNode.commitSourcePrepare(r, cmsg.SeqID, cmsg.Digest); err != nil {
			panic(err)
		}
		return true
	}
	// requestType ...
	block := core.DecodeB(r.Msg.Content)
	rphm.pbftNode.pl.Plog.Printf("S%dN%d : adding the block %d...now height = %d \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, block.Header.Number, rphm.pbftNode.CurChain.CurrentBlock.Header.Number)
	commitRound := int(block.Header.Number)
	rphm.pbftNode.CurChain.SeedPartitionMapFromClusterAssignmentIfEnabled()
	if err := rphm.pbftNode.CurChain.AddBlock(block); err != nil {
		panic(fmt.Errorf("native committed block application failed: %w", err))
	}
	creationReceipts, creationErr := rphm.pbftNode.CurChain.FirstCreationReceipts(block, time.Now())
	if creationErr != nil {
		panic(creationErr)
	}
	rphm.pbftNode.pl.Plog.Printf("S%dN%d : added the block %d... \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, block.Header.Number)
	rphm.pbftNode.CurChain.PrintBlockChain()
	postBlockStart := time.Now()

	// now try to relay txs to other shards (for main nodes)
	if rphm.pbftNode.NodeID == uint64(rphm.pbftNode.view.Load()) {
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : main node is trying to send relay txs at height = %d \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, block.Header.Number)
		// generate relay pool and collect txs excuted
		relayGroups := make(map[uint64][]*core.Transaction)
		interShardTxs := make([]*core.Transaction, 0)
		relay1Txs := make([]*core.Transaction, 0)
		relay2Txs := make([]*core.Transaction, 0)
		executedTxs, executionErr := rphm.pbftNode.CurChain.ExecutedNativeTransactions(block)
		if executionErr != nil {
			panic(executionErr)
		}
		for _, tx := range executedTxs {
			ssid := rphm.pbftNode.CurChain.ResolveTxSenderShard(tx)
			rsid := rphm.pbftNode.CurChain.ResolveTxRecipientShard(tx)
			if !tx.Relayed && ssid != rphm.pbftNode.ShardID {
				rphm.pbftNode.pl.Plog.Printf(
					"S%dN%d accepts stale in-flight direct tx under old route: sender=%s recipient=%s currentSenderShard=%d currentRecipientShard=%d localShard=%d\n",
					rphm.pbftNode.ShardID,
					rphm.pbftNode.NodeID,
					tx.Sender,
					tx.Recipient,
					ssid,
					rsid,
					rphm.pbftNode.ShardID,
				)
			}
			if tx.Relayed && rsid != rphm.pbftNode.ShardID {
				rphm.pbftNode.pl.Plog.Printf(
					"S%dN%d accepts stale in-flight relayed tx under old route: sender=%s recipient=%s senderShard=%d currentRecipientShard=%d localShard=%d\n",
					rphm.pbftNode.ShardID,
					rphm.pbftNode.NodeID,
					tx.Sender,
					tx.Recipient,
					ssid,
					rsid,
					rphm.pbftNode.ShardID,
				)
			}
			if !tx.Relayed && rsid != rphm.pbftNode.ShardID {
				relay1Txs = append(relay1Txs, tx)
				tx.Relayed = true
				relayGroups[rsid] = append(relayGroups[rsid], tx)
			} else {
				if tx.Relayed {
					relay2Txs = append(relay2Txs, tx)
				} else {
					interShardTxs = append(interShardTxs, tx)
				}
			}
		}
		rphm.pbftNode.CurChain.Txpool.ReplaceRelayPool(relayGroups)
		relayClassifyDone := time.Now()

		// send relay txs
		if params.RelayWithMerkleProof == 1 {
			rphm.pbftNode.RelayWithProofSend(block)
		} else {
			rphm.pbftNode.RelayMsgSend()
		}
		relaySendDone := time.Now()

		// send txs excuted in this block to the listener
		// add more message to measure more metrics
		bim := message.BlockInfoMsg{
			FirstCreationReceipts: creationReceipts,
			CommittedBlockHash:    hex.EncodeToString(block.Hash),
			CommittedStateRoot:    hex.EncodeToString(block.Header.StateRoot),
			BlockBodyLength:       len(block.Body), // Occupied block slots, not successful effect count.
			InnerShardTxs:         interShardTxs,
			Epoch:                 int(block.Header.Number),

			Relay1Txs: relay1Txs,
			Relay2Txs: relay2Txs,

			SenderShardID: rphm.pbftNode.ShardID,
			ProposeTime:   r.ReqTime,
			CommitTime:    time.Now(),
		}
		rphm.pbftNode.notePlacementCommitV2(block.Header.Number, bim.CommittedBlockHash, len(block.Body), bim.CommitTime)
		bim.AccountStateBytes, bim.AccountStateRoot, bim.AccountStateSizeStatus = rphm.pbftNode.CurChain.ForecastBlockAccountStateSizes(block)
		rphm.pbftNode.CurChain.Txpool.GetLocked()
		queueSize := rphm.pbftNode.CurChain.Txpool.PendingCountLocked()
		bim.TxPoolSize, bim.TxPoolObservedAt = &queueSize, time.Now()
		rphm.pbftNode.CurChain.Txpool.GetUnlocked()
		rphm.pbftNode.observeQueueService(block.Header.Number, len(block.Body), bim.CommitTime)
		rphm.pbftNode.CurChain.ExportNativeCommitSnapshotIfEnabled(
			int(block.Header.Number)+1,
			rphm.pbftNode.ShardID,
			rphm.pbftNode.NodeID,
			rphm.pbftNode.NodeID == uint64(rphm.pbftNode.view.Load()),
			block,
			bim.CommitTime,
			interShardTxs,
			relay1Txs,
			relay2Txs,
		)
		bByte, err := json.Marshal(bim)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CBlockInfo, bByte)
		go networks.TcpDial(msg_send, rphm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : sended excuted txs\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
		rphm.pbftNode.CurChain.Txpool.GetLocked()
		metricName := []string{
			"Block Height",
			"EpochID of this block",
			"TxPool Size",
			"# of all Txs in this block",
			"# of Relay1 Txs in this block",
			"# of Relay2 Txs in this block",
			"TimeStamp - Propose (unixMill)",
			"TimeStamp - Commit (unixMill)",

			"SUM of confirm latency (ms, All Txs)",
			"SUM of confirm latency (ms, Relay1 Txs) (Duration: Relay1 proposed -> Relay1 Commit)",
			"SUM of confirm latency (ms, Relay2 Txs) (Duration: Relay1 proposed -> Relay2 Commit)",
		}
		metricVal := []string{
			strconv.Itoa(int(block.Header.Number)),
			strconv.Itoa(bim.Epoch),
			strconv.Itoa(rphm.pbftNode.CurChain.Txpool.PendingCountLocked()),
			strconv.Itoa(len(executedTxs)), // Successful unique effects; body occupancy is reported separately above.
			strconv.Itoa(len(relay1Txs)),
			strconv.Itoa(len(relay2Txs)),
			strconv.FormatInt(bim.ProposeTime.UnixMilli(), 10),
			strconv.FormatInt(bim.CommitTime.UnixMilli(), 10),

			strconv.FormatInt(computeTCL(executedTxs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay1Txs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay2Txs, bim.CommitTime), 10),
		}
		rphm.pbftNode.writeCSVline(metricName, metricVal)
		rphm.pbftNode.CurChain.Txpool.GetUnlocked()
		if elapsed := time.Since(postBlockStart); elapsed > time.Second {
			rphm.pbftNode.pl.Plog.Printf("post_commit_timing block=%d classifyMs=%d relaySendMs=%d reportMs=%d totalMs=%d",
				block.Header.Number, relayClassifyDone.Sub(postBlockStart).Milliseconds(),
				relaySendDone.Sub(relayClassifyDone).Milliseconds(), time.Since(relaySendDone).Milliseconds(), elapsed.Milliseconds())
		}
	}
	rphm.applyExternalMappingUpdatesAtCommitBoundary(commitRound)
	return true
}

func (rphm *RawRelayPbftExtraHandleMod) applyExternalMappingUpdatesAtCommitBoundary(currentRound int) {
	if rphm == nil || rphm.pbftNode == nil || rphm.pbftNode.CurChain == nil {
		return
	}
	// Transactions now carry immutable sender/recipient routes selected at
	// injection. A newly committed mapping can therefore be absorbed at every
	// commit boundary without retroactively changing queued transactions.
	rphm.pbftNode.CurChain.ApplyExternalProposalIngressIfDue(currentRound)
	rphm.pbftNode.CurChain.ApplyExternalObjectMappingUpdatesIfDue(currentRound)
}

func (rphm *RawRelayPbftExtraHandleMod) HandleReqestforOldSeq(*message.RequestOldMessage) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation for sequential requests
func (rphm *RawRelayPbftExtraHandleMod) HandleforSequentialRequest(som *message.SendOldMessage) bool {
	if !validHistoryEnvelope(som) {
		return false
	}
	if int(som.SeqEndHeight-som.SeqStartHeight+1) != len(som.OldRequest) {
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : the SendOldMessage message is not enough\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
	} else { // add the block into the node pbft blockchain
		for height := som.SeqStartHeight; height <= som.SeqEndHeight; height++ {
			r := som.OldRequest[height-som.SeqStartHeight]
			if redirectRequest(r) {
				if err := rphm.pbftNode.commitRedirect(r, getDigest(r)); err != nil {
					return false
				}
				rphm.pbftNode.recordHistoricalProgress(r, height)
				continue
			}
			if r.RequestType == sourceRetirementRequest {
				if err := rphm.pbftNode.commitSourceRetirement(r, height, getDigest(r)); err != nil {
					return false
				}
				rphm.pbftNode.recordHistoricalProgress(r, height)
				continue
			}
			if auxiliaryBatch(r) {
				if err := rphm.pbftNode.commitAuxiliaryBatch(r, getDigest(r)); err != nil {
					return false
				}
				rphm.pbftNode.recordHistoricalProgress(r, height)
				continue
			}
			if r.RequestType == inboundDebitRequest {
				if err := rphm.pbftNode.commitInboundDebit(r, getDigest(r)); err != nil {
					return false
				}
				rphm.pbftNode.recordHistoricalProgress(r, height)
				continue
			}
			if r.RequestType == handoffRelayRequest {
				if err := rphm.pbftNode.commitHandoffRelay(r, getDigest(r)); err != nil {
					return false
				}
				rphm.pbftNode.recordHistoricalProgress(r, height)
				continue
			}
			if r.RequestType == migrationHandoffBatchRequest {
				if err := rphm.pbftNode.commitMigrationHandoffBatch(r, getDigest(r)); err != nil {
					return false
				}
				rphm.pbftNode.recordHistoricalProgress(r, height)
				continue
			}
			if r.RequestType == migrationHandoffRequest {
				if err := rphm.pbftNode.commitMigrationHandoff(r, getDigest(r)); err != nil {
					return false
				}
				rphm.pbftNode.recordHistoricalProgress(r, height)
				continue
			}
			if r.RequestType == targetActivationRequest {
				if err := rphm.pbftNode.commitTargetActivation(r, getDigest(r)); err != nil {
					return false
				}
				rphm.pbftNode.recordHistoricalProgress(r, height)
				continue
			}
			if r.RequestType == targetInstallRequest {
				if err := rphm.pbftNode.commitTargetInstall(r, height, getDigest(r)); err != nil {
					return false
				}
				rphm.pbftNode.recordHistoricalProgress(r, height)
				continue
			}
			if r.RequestType == sourcePrepareRequest {
				if !rphm.pbftNode.validatePreparedHistoryIdentity(r, height) {
					return false
				}
				if err := rphm.pbftNode.commitSourcePrepare(r, height, getDigest(r)); err != nil {
					return false
				}
				rphm.pbftNode.recordHistoricalProgress(r, height)
				continue
			}
			if r.RequestType == message.BlockRequest {
				b := core.DecodeB(r.Msg.Content)
				if err := rphm.pbftNode.CurChain.AddBlock(b); err != nil {
					return false
				}
				rphm.pbftNode.recordHistoricalProgress(r, height)
			} else {
				return false
			}
		}
		rphm.pbftNode.sequenceID = som.SeqEndHeight + 1
		rphm.pbftNode.CurChain.PrintBlockChain()
	}
	return true
}
