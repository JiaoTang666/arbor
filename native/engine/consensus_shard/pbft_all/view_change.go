package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/json"
	"log"
	"time"
)

type ViewChangeData struct {
	NextView, SeqID int
}

// propose a view change request
func (p *PbftConsensusNode) viewChangePropose() {
	if inside, ok := p.ihm.(*CLPAPbftInsideExtraHandleMod); ok && inside.epoch != nil {
		return
	}
	p.pbftLock.Lock()
	if p.stopSignal.Load() || p.historyRecoveryFailed.Load() || p.pbftStage.Load() == 5 {
		p.pbftLock.Unlock()
		return
	}
	// A completed commit may have refreshed progress while this request waited
	// for pbftLock. Never act on the watchdog's stale pre-lock observation.
	if !p.viewChangeTimeoutDue(time.Now()) {
		p.pbftLock.Unlock()
		return
	}
	// load pbftStage as 5, i.e., making a view change
	p.pbftStage.Store(5)
	p.conditionalVarpbftLock.Broadcast()
	vcmsg := message.ViewChangeMsg{
		CurView:  int(p.view.Load()),
		NextView: int(p.view.Load()+1) % int(p.node_nums),
		SeqID:    int(p.sequenceID),
		FromNode: p.NodeID,
	}
	p.pbftLock.Unlock()
	p.pl.Plog.Println("Main node is time out, now trying to view change. ")
	// marshal and broadcast
	vcbyte, err := json.Marshal(vcmsg)
	if err != nil {
		log.Panic()
	}
	msg_send := message.MergeMessage(message.ViewChangePropose, vcbyte)
	networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
	networks.TcpDial(msg_send, p.RunningNode.IPaddr)

	p.pl.Plog.Println("View change message is broadcasted. ")
}

// Called under pbftLock immediately before transitioning the state machine.
func (p *PbftConsensusNode) viewChangeTimeoutDue(now time.Time) bool {
	return p.hasCommittedBlock.Load() && p.node_nums > 1 &&
		now.UnixMilli()-p.lastCommitTime.Load() > int64(params.PbftViewChangeTimeOut)
}

// handle view change messages.
func (p *PbftConsensusNode) handleViewChangeMsg(content []byte) {
	if inside, ok := p.ihm.(*CLPAPbftInsideExtraHandleMod); ok && inside.epoch != nil {
		panic("CLPA epoch v1 does not support leader failover")
	}
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	if p.stopSignal.Load() || p.historyRecoveryFailed.Load() {
		return
	}
	vcmsg := new(message.ViewChangeMsg)
	err := json.Unmarshal(content, vcmsg)
	if err != nil {
		log.Panic(err)
	}
	vcData := ViewChangeData{vcmsg.NextView, vcmsg.SeqID}
	if _, ok := p.viewChangeMap[vcData]; !ok {
		p.viewChangeMap[vcData] = make(map[uint64]bool)
	}
	p.viewChangeMap[vcData][vcmsg.FromNode] = true

	p.pl.Plog.Println("Received view change message from Node", vcmsg.FromNode)

	// if cnt = 2*f+1, then broadcast newView msg
	if len(p.viewChangeMap[vcData]) == 2*int(p.malicious_nums)+1 {
		nvmsg := message.NewViewMsg{
			CurView:  int(p.view.Load()),
			NextView: int(p.view.Load()+1) % int(p.node_nums),
			NewSeqID: int(p.sequenceID),
			FromNode: p.NodeID,
		}
		nvbyte, err := json.Marshal(nvmsg)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.NewChange, nvbyte)
		networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
		networks.TcpDial(msg_send, p.RunningNode.IPaddr)
	}
}

func (p *PbftConsensusNode) handleNewViewMsg(content []byte) {
	if inside, ok := p.ihm.(*CLPAPbftInsideExtraHandleMod); ok && inside.epoch != nil {
		panic("CLPA epoch v1 does not support leader failover")
	}
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	if p.stopSignal.Load() || p.historyRecoveryFailed.Load() {
		return
	}
	nvmsg := new(message.NewViewMsg)
	err := json.Unmarshal(content, nvmsg)
	if err != nil {
		log.Panic(err)
	}
	vcData := ViewChangeData{nvmsg.NextView, nvmsg.NewSeqID}
	if _, ok := p.newViewMap[vcData]; !ok {
		p.newViewMap[vcData] = make(map[uint64]bool)
	}
	p.newViewMap[vcData][nvmsg.FromNode] = true

	p.pl.Plog.Println("Received new view message from Node", nvmsg.FromNode)

	// if cnt = 2*f+1, then step into the next view.
	if len(p.newViewMap[vcData]) == 2*int(p.malicious_nums)+1 {
		defer p.conditionalVarpbftLock.Broadcast()
		p.releaseSequenceLockIfHeld("new_view_transition")
		p.historyPending = nil
		p.view.Store(int32(vcData.NextView))
		p.sequenceID = uint64(nvmsg.NewSeqID)
		p.pbftStage.Store(1)
		p.currentPreparedSequence, p.currentPreparedDigest = 0, ""
		p.lastCommitTime.Store(time.Now().UnixMilli())
		p.pl.Plog.Println("New view is updated.")
	}
}
