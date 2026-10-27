package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// this func is only invoked by main node
func (p *PbftConsensusNode) Propose() {
	// wait other nodes to start TCPlistening, sleep 5 sec.
	time.Sleep(5 * time.Second)

	nextRoundBeginSignal := make(chan bool)
	lastEmptyProposalDelayLogAt := time.Time{}

	go func() {
		// go into the next round
		for {
			time.Sleep(time.Duration(int64(p.pbftChainConfig.BlockInterval)) * time.Millisecond)
			// send a signal to another GO-Routine. It will block until a GO-Routine try to fetch data from this channel.
			for p.pbftStage.Load() != 1 {
				time.Sleep(time.Millisecond * 100)
			}
			nextRoundBeginSignal <- true
		}
	}()

	go func() {
		// check whether to view change
		for {
			time.Sleep(time.Second)
			if inside, ok := p.ihm.(*CLPAPbftInsideExtraHandleMod); ok && inside.epoch != nil {
				// Epoch v1 deliberately uses fixed leaders. Its closed-round pause
				// is not a stalled PBFT proposal, and ingress is not failover-ready.
				continue
			}
			if !p.hasCommittedBlock.Load() {
				continue
			}
			if p.node_nums <= 1 {
				// A 1-node shard has no alternate leader. Starting a view
				// change only abandons the already packed proposal and can
				// permanently drop its transactions from the local txpool.
				continue
			}
			nowMs := time.Now().UnixMilli()
			pendingWorkload := 0
			if p.CurChain != nil && p.CurChain.Txpool != nil {
				pendingWorkload = p.CurChain.Txpool.GetPendingWorkloadCount()
			}
			if pendingWorkload <= 0 {
				p.lastCommitTime.Store(nowMs)
				continue
			}
			if nowMs-p.lastCommitTime.Load() > int64(params.PbftViewChangeTimeOut) {
				// One watchdog request at a time; the callee rechecks progress
				// after acquiring the commit lock. A timer tick is not a commit.
				p.viewChangePropose()
			}
		}
	}()

	for {
		select {
		case <-nextRoundBeginSignal:
			if p.stopSignal.Load() || p.historyRecoveryFailed.Load() {
				return
			}
			// if this node is not leader, do not propose.
			if uint64(p.view.Load()) != p.NodeID {
				continue
			}
			if shouldDelay, reason := p.shouldDelayEmptyProposal(); shouldDelay {
				if lastEmptyProposalDelayLogAt.IsZero() || time.Since(lastEmptyProposalDelayLogAt) >= 10*time.Second {
					p.pl.Plog.Printf(
						"S%dN%d delaying empty proposal while %s.\n",
						p.ShardID,
						p.NodeID,
						reason,
					)
					lastEmptyProposalDelayLogAt = time.Now()
				}
				continue
			}
			lastEmptyProposalDelayLogAt = time.Time{}

			p.sequenceLock.Lock()
			p.sequenceLockHeld.Store(true)
			p.pbftLock.Lock()
			if p.stopSignal.Load() || p.historyRecoveryFailed.Load() {
				p.pbftLock.Unlock()
				p.releaseSequenceLockIfHeld("stopped_before_propose")
				return
			}
			if uint64(p.view.Load()) != p.NodeID || p.pbftStage.Load() != 1 {
				p.pbftLock.Unlock()
				p.releaseSequenceLockIfHeld("stale_propose_after_lock")
				continue
			}
			p.pl.Plog.Printf("S%dN%d get sequenceLock locked, now trying to propose...\n", p.ShardID, p.NodeID)
			// propose
			// implement interface to generate propose
			ok, r := p.ihm.HandleinPropose()
			if !ok || r == nil {
				p.pbftLock.Unlock()
				p.releaseSequenceLockIfHeld("proposal_deferred")
				continue
			}

			digest := getDigest(r)
			proposalSeqID := p.sequenceID
			p.requestPool[string(digest)] = r
			p.pl.Plog.Printf("S%dN%d put the request into the pool ...\n", p.ShardID, p.NodeID)
			p.height2Digest[proposalSeqID] = string(digest)
			p.pbftStage.Store(2)
			p.conditionalVarpbftLock.Broadcast()
			p.pbftLock.Unlock()

			ppmsg := message.PrePrepare{
				RequestMsg: r,
				Digest:     digest,
				SeqID:      proposalSeqID,
			}
			// marshal and broadcast
			ppbyte, err := json.Marshal(ppmsg)
			if err != nil {
				log.Panic()
			}
			msg_send := message.MergeMessage(message.CPrePrepare, ppbyte)
			p.broadcastConsensus(msg_send)

		case <-p.pStop:
			p.pl.Plog.Printf("S%dN%d get stopSignal in Propose Routine, now stop...\n", p.ShardID, p.NodeID)
			return
		}
	}
}

// Handle pre-prepare messages here.
// If you want to do more operations in the pre-prepare stage, you can implement the interface "ExtraOpInConsensus",
// and call the function: **ExtraOpInConsensus.HandleinPrePrepare**
func (p *PbftConsensusNode) handlePrePrepare(content []byte) {
	p.RunningNode.PrintNode()
	fmt.Println("received the PrePrepare ...")
	// decode the message
	ppmsg := new(message.PrePrepare)
	err := json.Unmarshal(content, ppmsg)
	if err != nil {
		log.Panic(err)
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	if p.stopSignal.Load() || p.historyRecoveryFailed.Load() {
		return
	}
	for p.pbftStage.Load() < 1 && ppmsg.SeqID >= p.sequenceID && p.view.Load() == curView {
		p.conditionalVarpbftLock.Wait()
		if p.stopSignal.Load() || p.historyRecoveryFailed.Load() {
			return
		}
	}
	defer p.conditionalVarpbftLock.Broadcast()

	// if this message is out of date, return.
	if ppmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	if ppmsg.RequestMsg == nil || string(getDigest(ppmsg.RequestMsg)) != string(ppmsg.Digest) {
		p.pl.Plog.Printf("S%dN%d : the digest is not consistent, so refuse to prepare. \n", p.ShardID, p.NodeID)
	} else if p.sequenceID < ppmsg.SeqID {
		if old, ok := p.height2Digest[ppmsg.SeqID]; ok && old != string(ppmsg.Digest) {
			return
		}
		p.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg
		p.height2Digest[ppmsg.SeqID] = string(ppmsg.Digest)
		p.pl.Plog.Printf("cached future PrePrepare sequence=%d current=%d; execution deferred", ppmsg.SeqID, p.sequenceID)
	} else {
		p.acceptPrePrepareLocked(ppmsg)
	}
}

// Handle prepare messages here.
// If you want to do more operations in the prepare stage, you can implement the interface "ExtraOpInConsensus",
// and call the function: **ExtraOpInConsensus.HandleinPrepare**
func (p *PbftConsensusNode) handlePrepare(content []byte) {
	p.pl.Plog.Printf("S%dN%d : received the Prepare ...\n", p.ShardID, p.NodeID)
	// decode the message
	pmsg := new(message.Prepare)
	err := json.Unmarshal(content, pmsg)
	if err != nil {
		log.Panic(err)
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	if p.stopSignal.Load() || p.historyRecoveryFailed.Load() || !p.validConsensusMember(pmsg.SenderNode) {
		return
	}
	for pmsg.SeqID >= p.sequenceID && p.view.Load() == curView && (pmsg.SeqID > p.sequenceID || p.pbftStage.Load() < 2 || p.currentPreparedSequence != p.sequenceID) {
		p.conditionalVarpbftLock.Wait()
		if p.stopSignal.Load() || p.historyRecoveryFailed.Load() {
			return
		}
	}
	defer p.conditionalVarpbftLock.Broadcast()

	// if this message is out of date, return.
	if pmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	if _, ok := p.requestPool[string(pmsg.Digest)]; !ok || !p.preparedCurrent(pmsg.Digest) {
		p.pl.Plog.Printf("S%dN%d : doesn't have the digest in the requst pool, refuse to commit\n", p.ShardID, p.NodeID)
	} else if p.sequenceID < pmsg.SeqID {
		p.pl.Plog.Printf("S%dN%d : inconsistent sequence ID, refuse to commit\n", p.ShardID, p.NodeID)
	} else {
		// if needed more operations, implement interfaces
		if !p.ihm.HandleinPrepare(pmsg) {
			return
		}

		key := p.voteKey(pmsg.SeqID, pmsg.Digest)
		p.set2DMap(true, key, pmsg.SenderNode)
		cnt := len(p.cntPrepareConfirm[key])

		// if the node has received 2f messages (itself included), and it haven't committed, then it commit
		p.lock.Lock()
		defer p.lock.Unlock()
		if uint64(cnt) >= 2*p.malicious_nums+1 && !p.isCommitBordcast[string(pmsg.Digest)] {
			p.pl.Plog.Printf("S%dN%d : is going to commit\n", p.ShardID, p.NodeID)
			// generate commit and broadcast
			c := message.Commit{
				Digest:     pmsg.Digest,
				SeqID:      pmsg.SeqID,
				SenderNode: p.RunningNode,
			}
			commitByte, err := json.Marshal(c)
			if err != nil {
				log.Panic()
			}
			msg_send := message.MergeMessage(message.CCommit, commitByte)
			p.broadcastConsensus(msg_send)
			p.isCommitBordcast[string(pmsg.Digest)] = true
			p.pl.Plog.Printf("S%dN%d : commit is broadcast\n", p.ShardID, p.NodeID)

			p.pbftStage.Store(3)
		}
	}
}

// Handle commit messages here.
// If you want to do more operations in the commit stage, you can implement the interface "ExtraOpInConsensus",
// and call the function: **ExtraOpInConsensus.HandleinCommit**
func (p *PbftConsensusNode) handleCommit(content []byte) {
	// decode the message
	cmsg := new(message.Commit)
	err := json.Unmarshal(content, cmsg)
	if err != nil {
		log.Panic(err)
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	if p.stopSignal.Load() || p.historyRecoveryFailed.Load() || !p.validConsensusMember(cmsg.SenderNode) {
		return
	}
	defer p.conditionalVarpbftLock.Broadcast()
	if cmsg.SeqID < p.sequenceID {
		return
	}
	key := p.voteKey(cmsg.SeqID, cmsg.Digest)
	p.set2DMap(false, key, cmsg.SenderNode)
	if cmsg.SeqID > p.sequenceID {
		if uint64(len(p.cntCommitConfirm[key])) >= 2*p.malicious_nums+1 {
			p.requestCommittedHistoryLocked(cmsg.SeqID)
		}
		return
	}
	if (!p.preparedCurrent(cmsg.Digest) || p.pbftStage.Load() < 3) && uint64(len(p.cntCommitConfirm[key])) >= 2*p.malicious_nums+1 {
		p.requestCommittedHistoryLocked(cmsg.SeqID)
		return
	}
	for p.pbftStage.Load() < 3 && cmsg.SeqID == p.sequenceID && p.view.Load() == curView {
		p.conditionalVarpbftLock.Wait()
		if p.stopSignal.Load() || p.historyRecoveryFailed.Load() {
			return
		}
	}

	if cmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	p.pl.Plog.Printf("S%dN%d received the Commit from ...%d\n", p.ShardID, p.NodeID, cmsg.SenderNode.NodeID)
	cnt := len(p.cntCommitConfirm[key])

	p.lock.Lock()
	defer p.lock.Unlock()

	if uint64(cnt) >= 2*p.malicious_nums+1 && !p.isReply[string(cmsg.Digest)] {
		p.pl.Plog.Printf("S%dN%d : has received 2f + 1 commits ... \n", p.ShardID, p.NodeID)
		// if this node is left behind, so it need to requst blocks
		if _, ok := p.requestPool[string(cmsg.Digest)]; !ok || !p.preparedCurrent(cmsg.Digest) {
			p.requestCommittedHistoryLocked(cmsg.SeqID)
			return
		}
		if !p.ihm.HandleinCommit(cmsg) {
			p.pl.Plog.Printf("COMMIT_APPLICATION_REJECTED sequence=%d; consensus progress unchanged", cmsg.SeqID)
			return
		}
		p.isReply[string(cmsg.Digest)] = true
		p.pl.Plog.Printf("S%dN%d: this round of pbft %d is end \n", p.ShardID, p.NodeID, cmsg.SeqID)
		p.sequenceID = cmsg.SeqID + 1
		p.refreshHistoryProgress()
		p.currentPreparedSequence, p.currentPreparedDigest = 0, ""

		p.pbftStage.Store(1)
		p.lastCommitTime.Store(time.Now().UnixMilli())
		p.hasCommittedBlock.Store(true)

		// if this node is a main node, then unlock the sequencelock
		if p.NodeID == uint64(p.view.Load()) {
			p.releaseSequenceLockIfHeld("commit_complete")
		}
		p.resumeCachedPrePrepareLocked()
	}
}

// this func is only invoked by the main node,
// if the request is correct, the main node will send
// block back to the message sender.
// now this function can send both block and partition
func (p *PbftConsensusNode) handleRequestOldSeq(content []byte) {
	if uint64(p.view.Load()) != p.NodeID {
		return
	}

	rom := new(message.RequestOldMessage)
	err := json.Unmarshal(content, rom)
	if err != nil {
		log.Panic()
	}
	p.pl.Plog.Printf("S%dN%d : received the old message requst from ...", p.ShardID, p.NodeID)
	rom.SenderNode.PrintNode()

	p.pbftLock.Lock()
	if p.stopSignal.Load() || p.historyRecoveryFailed.Load() {
		p.pbftLock.Unlock()
		return
	}
	oldR := p.committedHistory(rom.SeqStartHeight, rom.SeqEndHeight)
	p.pl.Plog.Printf("S%dN%d : has generated the message to be sent\n", p.ShardID, p.NodeID)

	p.ihm.HandleReqestforOldSeq(rom)
	p.pbftLock.Unlock()

	// send the block back
	sb := message.SendOldMessage{
		RequestID:      rom.RequestID,
		SeqStartHeight: rom.SeqStartHeight,
		SeqEndHeight:   rom.SeqEndHeight,
		OldRequest:     oldR,
		SenderNode:     p.RunningNode,
	}
	sbByte, err := json.Marshal(sb)
	if err != nil {
		log.Panic()
	}
	msg_send := message.MergeMessage(message.CSendOldrequest, sbByte)
	if err := networks.SendHistoryBounded(msg_send, rom.SenderNode.IPaddr, 5*time.Second); err != nil {
		p.pl.Plog.Printf("history response send failed: %v", err)
	}
	p.pl.Plog.Printf("S%dN%d : send blocks\n", p.ShardID, p.NodeID)
}

// node requst blocks and receive blocks from the main node
func (p *PbftConsensusNode) handleSendOldSeq(content []byte) {
	som := new(message.SendOldMessage)
	err := json.Unmarshal(content, som)
	if err != nil {
		return
	}
	p.pl.Plog.Printf("S%dN%d : has received the SendOldMessage message\n", p.ShardID, p.NodeID)

	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	defer p.conditionalVarpbftLock.Broadcast()
	if p.stopSignal.Load() || p.historyRecoveryFailed.Load() {
		return
	}
	if !p.matchesHistoryAttempt(som, time.Now()) {
		return
	}
	// Retire this identity on success, rejection or partial failure. Later
	// attempts start from the actual next sequence, never the original prefix.
	p.historyPending = nil
	if !validHistoryEnvelope(som) || som.SeqStartHeight > p.sequenceID {
		p.pl.Plog.Printf("reject invalid or non-contiguous history response")
		return
	}
	if som.SeqStartHeight < p.sequenceID {
		// Normal commits may advance while this response waits for pbftLock.
		// Never reapply that prefix; request a fresh, fully validated suffix.
		p.pl.Plog.Printf("HISTORY_STALE_RESPONSE_DROP start=%d end=%d next=%d", som.SeqStartHeight, som.SeqEndHeight, p.sequenceID)
		p.refreshHistoryProgress()
		if j := p.historyRecovery; j != nil {
			j.next = time.Now()
		}
		return
	}

	// implement interface for new consensus
	if !p.ihm.HandleforSequentialRequest(som) {
		return
	}
	beginSeq := som.SeqStartHeight
	for idx, r := range som.OldRequest {
		p.requestPool[string(getDigest(r))] = r
		p.height2Digest[uint64(idx)+beginSeq] = string(getDigest(r))
		p.isReply[string(getDigest(r))] = true
		p.pl.Plog.Printf("this round of pbft %d is end \n", uint64(idx)+beginSeq)
	}
	if len(som.OldRequest) > 0 {
		p.hasCommittedBlock.Store(true)
		p.lastCommitTime.Store(time.Now().UnixMilli())
	}
	p.sequenceID = som.SeqEndHeight + 1
	p.refreshHistoryProgress()
	p.currentPreparedSequence, p.currentPreparedDigest = 0, ""
	p.pbftStage.Store(1)
	if j := p.historyRecovery; j != nil && p.sequenceID <= j.end {
		j.attempts = 0 // a fully applied contiguous range made real progress
		p.requestCommittedHistoryLocked(j.end)
		return
	}
	p.historyRecovery = nil
	p.resumeCachedPrePrepareLocked()
}
