// The pbft consensus process

package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"bufio"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
)

type PbftConsensusNode struct {
	placementService      placementServiceV2
	queueObserver         queueObserverState
	presenceSession       string // pbftLock; only fresh-start exclusive supervisor
	presenceCache         map[string]presenceCached
	admissionLegSeen      map[string]string     // pbftLock; duplicate transports never re-enter pool
	sourceQueueReady      bool                  // commit lock / startup only
	historyRecoveryFailed atomic.Bool           // sticky fail-closed latch; not a storage-close signal
	historyRecovery       *historyRecoveryState // protected by pbftLock
	historyPending        *historyAttempt       // protected by pbftLock
	historyGeneration     uint64
	sourcePrepareSeen     sync.Map            // immutable inbox fingerprints already attempted
	retirementAudit       retirementAuditLane // read-only transport audit; no chain mutation
	// the local config about pbft
	RunningNode *shard.Node // the node information
	ShardID     uint64      // denote the ID of the shard (or pbft), only one pbft consensus in a shard
	NodeID      uint64      // denote the ID of the node in the pbft (shard)

	// the data structure for blockchain
	CurChain *chain.BlockChain // all node in the shard maintain the same blockchain
	db       ethdb.Database    // to save the mpt

	// the global config about pbft
	pbftChainConfig *params.ChainConfig          // the chain config in this pbft
	ip_nodeTable    map[uint64]map[uint64]string // denote the ip of the specific node
	node_nums       uint64                       // the number of nodes in this pfbt, denoted by N
	malicious_nums  uint64                       // f, 3f + 1 = N

	// view change
	view              atomic.Int32 // denote the view of this pbft, the main node can be inferred from this variant
	lastCommitTime    atomic.Int64 // the time since last commit.
	hasCommittedBlock atomic.Bool
	hasSeenWorkload   atomic.Bool
	lastWorkloadAtMs  atomic.Int64
	viewChangeMap     map[ViewChangeData]map[uint64]bool
	newViewMap        map[ViewChangeData]map[uint64]bool

	// the control message and message checking utils in pbft
	sequenceID              uint64                          // the message sequence id of the pbft
	stopSignal              atomic.Bool                     // send stop signal
	pStop                   chan uint64                     // channle for stopping consensus
	chainCloseOnce          sync.Once                       // close the chain only once during shutdown
	chainClosed             atomic.Bool                     // records completion of the close critical section
	shutdownDoneOnce        sync.Once                       // lazy initialization also supports unit-test nodes
	shutdownDone            chan struct{}                   // audit, storage close and stop handling have finished
	requestPool             map[string]*message.Request     // RequestHash to Request
	cntPrepareConfirm       map[string]map[*shard.Node]bool // count the prepare confirm message, [messageHash][Node]bool
	cntCommitConfirm        map[string]map[*shard.Node]bool // count the commit confirm message, [messageHash][Node]bool
	isCommitBordcast        map[string]bool                 // denote whether the commit is broadcast
	isReply                 map[string]bool                 // denote whether the message is reply
	height2Digest           map[uint64]string               // sequence (block height) -> request, fast read
	currentPreparedSequence uint64                          // pbftLock; locally validated PrePrepare, not merely a cached proposal
	currentPreparedDigest   string

	// pbft stage wait
	pbftStage              atomic.Int32 // 1->Preprepare, 2->Prepare, 3->Commit, 4->Done
	pbftLock               sync.Mutex
	conditionalVarpbftLock sync.Cond

	// locks about pbft
	sequenceLock     sync.Mutex // the lock of sequence
	sequenceLockHeld atomic.Bool
	lock             sync.Mutex // lock the stage
	askForLock       sync.Mutex // lock for asking for a serise of requests

	// seqID of other Shards, to synchronize
	seqIDMap   map[uint64]uint64
	seqMapLock sync.Mutex

	// logger
	pl *pbft_log.PbftLog
	// tcp control
	tcpln       net.Listener
	tcpPoolLock sync.Mutex

	// to handle the message in the pbft
	ihm ExtraOpInConsensus

	// to handle the message outside of pbft
	ohm OpInterShards

	emptyProposalGrace time.Duration
}

// generate a pbft consensus for a node
func NewPbftNode(shardID, nodeID uint64, pcc *params.ChainConfig, messageHandleType string) *PbftConsensusNode {
	p := new(PbftConsensusNode)
	p.ip_nodeTable = params.IPmap_nodeTable
	p.node_nums = pcc.Nodes_perShard
	p.ShardID = shardID
	p.NodeID = nodeID
	p.pbftChainConfig = pcc
	fp := params.DatabaseWrite_path + "mptDB/ldb/s" + strconv.FormatUint(shardID, 10) + "/n" + strconv.FormatUint(nodeID, 10)
	var err error
	p.db, err = openStateDB(fp)
	if err != nil {
		log.Panic(err)
	}
	p.CurChain, err = chain.NewBlockChain(pcc, p.db)
	if err != nil {
		log.Panic("cannot new a blockchain")
	}

	p.RunningNode = &shard.Node{
		NodeID:  nodeID,
		ShardID: shardID,
		IPaddr:  p.ip_nodeTable[shardID][nodeID],
	}

	p.stopSignal.Store(false)
	p.sequenceID = p.CurChain.CurrentBlock.Header.Number + 1
	// The stop path must not wait for the proposer select loop to receive this
	// one notification. A buffered signal preserves the existing stop contract.
	p.pStop = make(chan uint64, 1)
	p.requestPool = make(map[string]*message.Request)
	p.cntPrepareConfirm = make(map[string]map[*shard.Node]bool)
	p.cntCommitConfirm = make(map[string]map[*shard.Node]bool)
	p.isCommitBordcast = make(map[string]bool)
	p.isReply = make(map[string]bool)
	p.height2Digest = make(map[uint64]string)
	p.malicious_nums = (p.node_nums - 1) / 3

	// init view & last commit time
	p.view.Store(0)
	p.lastCommitTime.Store(time.Now().Add(time.Second * 5).UnixMilli())
	p.hasCommittedBlock.Store(false)
	if err := p.restorePreparedRequestProgress(); err != nil {
		log.Panic(err)
	}
	if err := p.restoreSourceQueue(); err != nil {
		log.Panic(err)
	}
	p.hasSeenWorkload.Store(false)
	p.lastWorkloadAtMs.Store(time.Now().UnixMilli())
	p.sequenceLockHeld.Store(false)
	p.viewChangeMap = make(map[ViewChangeData]map[uint64]bool)
	p.newViewMap = make(map[ViewChangeData]map[uint64]bool)

	p.seqIDMap = make(map[uint64]uint64)

	p.pl = pbft_log.NewPbftLog(shardID, nodeID)

	// choose how to handle the messages in pbft or beyond pbft
	switch string(messageHandleType) {
	case "CLPA_Broker":
		ncdm := dataSupport.NewCLPADataSupport()
		p.ihm = &CLPAPbftInsideExtraHandleMod_forBroker{
			pbftNode: p,
			cdm:      ncdm,
		}
		p.ohm = &CLPABrokerOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
		}
	case "CLPA":
		ncdm := dataSupport.NewCLPADataSupport()
		epoch := newCLPAEpochRuntime()
		if epoch != nil && p.CurChain.CurrentBlock.Header.Number != 0 {
			panic("CLPA epoch v1 requires a fresh database; restart is unsupported")
		}
		p.ihm = &CLPAPbftInsideExtraHandleMod{
			pbftNode: p,
			cdm:      ncdm,
			epoch:    epoch,
		}
		p.ohm = &CLPARelayOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
			epoch:    epoch,
		}
	case "Broker":
		p.ihm = &RawBrokerPbftExtraHandleMod{
			pbftNode: p,
		}
		p.ohm = &RawBrokerOutsideModule{
			pbftNode: p,
		}
	default:
		p.ihm = &RawRelayPbftExtraHandleMod{
			pbftNode: p,
		}
		p.ohm = &RawRelayOutsideModule{
			pbftNode: p,
		}
	}

	// set pbft stage now
	p.conditionalVarpbftLock = *sync.NewCond(&p.pbftLock)
	p.pbftStage.Store(1)
	p.emptyProposalGrace = durationFromEnvMs("BLOCK_EMULATOR_EMPTY_PROPOSAL_GRACE_MS", maxDuration(15*time.Second, time.Duration(p.pbftChainConfig.BlockInterval)*time.Millisecond*4))
	p.logOwnershipStartup()

	return p
}

func (p *PbftConsensusNode) releaseSequenceLockIfHeld(reason string) {
	if p.sequenceLockHeld.CompareAndSwap(true, false) {
		p.sequenceLock.Unlock()
		p.pl.Plog.Printf(
			"S%dN%d released sequenceLock during %s.\n",
			p.ShardID,
			p.NodeID,
			reason,
		)
	}
}

func durationFromEnvMs(name string, fallback time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return time.Duration(value) * time.Millisecond
}

func maxDuration(a time.Duration, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func (p *PbftConsensusNode) noteWorkloadArrival(txCount int, source string) {
	if txCount <= 0 {
		return
	}
	nowMs := time.Now().UnixMilli()
	firstSeen := p.hasSeenWorkload.CompareAndSwap(false, true)
	p.lastWorkloadAtMs.Store(nowMs)
	if firstSeen {
		p.pl.Plog.Printf(
			"S%dN%d observed first workload arrival from %s, txs=%d.\n",
			p.ShardID,
			p.NodeID,
			source,
			txCount,
		)
	}
}

func (p *PbftConsensusNode) shouldDelayEmptyProposal() (bool, string) {
	if inside, ok := p.ihm.(*CLPAPbftInsideExtraHandleMod); ok && inside.epoch != nil {
		return !inside.epoch.shouldPropose(), "waiting_for_epoch_control"
	}
	if p.hasUnattemptedSourcePreparation() {
		return false, ""
	}
	pendingWorkload := 0
	if p.CurChain != nil && p.CurChain.Txpool != nil {
		pendingWorkload = p.CurChain.Txpool.GetPendingWorkloadCount()
	}
	if pendingWorkload > 0 {
		return false, ""
	}
	if !p.hasSeenWorkload.Load() {
		return true, "waiting_for_first_workload"
	}
	lastWorkloadAt := p.lastWorkloadAtMs.Load()
	if lastWorkloadAt <= 0 {
		return true, "waiting_for_recent_workload"
	}
	if time.Since(time.UnixMilli(lastWorkloadAt)) < p.emptyProposalGrace {
		return true, "waiting_for_followup_workload"
	}
	return false, ""
}

// handle the raw message, send it to corresponded interfaces
func (p *PbftConsensusNode) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	switch msgType {
	// pbft inside message type
	case message.CPrePrepare:
		// use "go" to start a go routine to handle this message, so that a pre-arrival message will not be aborted.
		go p.handlePrePrepare(content)
	case message.CPrepare:
		// use "go" to start a go routine to handle this message, so that a pre-arrival message will not be aborted.
		go p.handlePrepare(content)
	case message.CCommit:
		// use "go" to start a go routine to handle this message, so that a pre-arrival message will not be aborted.
		go p.handleCommit(content)

	case message.ViewChangePropose:
		p.handleViewChangeMsg(content)
	case message.NewChange:
		p.handleNewViewMsg(content)

	case message.CRequestOldrequest:
		go p.handleRequestOldSeq(content)
	case message.CSendOldrequest:
		go p.handleSendOldSeq(content)

	case message.CStop:
		p.WaitToStop()

	// handle the message from outside
	default:
		go p.ohm.HandleMessageOutsidePBFT(msgType, content)
	}
}

func (p *PbftConsensusNode) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		if p.stopSignal.Load() {
			return
		}
		switch err {
		case nil:
			if p.tryPresenceRequest(con, clientRequest) {
				return
			}
			p.tcpPoolLock.Lock()
			p.handleMessage(clientRequest)
			p.tcpPoolLock.Unlock()
		case io.EOF:
			log.Println("client closed the connection by terminating the process")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}
}

// A consensus node starts tcp-listen.
func (p *PbftConsensusNode) TcpListen() {
	p.startPlacementTelemetryV2()
	go p.runHistoryRecovery()
	ln, err := net.Listen("tcp", p.RunningNode.IPaddr)
	p.tcpln = ln
	if err != nil {
		log.Panic(err)
	}
	for {
		conn, err := p.tcpln.Accept()
		if err != nil {
			return
		}
		go p.handleClientRequest(conn)
	}
}

// When receiving a stop message, this node try to stop.
func (p *PbftConsensusNode) WaitToStop() {
	p.pl.Plog.Println("handling stop message")
	done := p.shutdownCompletion()
	// Concurrent stop messages must not close storage twice or send duplicate
	// proposer notifications.
	if p.stopSignal.Swap(true) {
		return
	}
	p.sourcePublicationLane().close()
	p.sourceConfirmationLane().close()
	p.retirementAudit.close()
	networks.CloseAllConnInPool()
	if p.tcpln != nil {
		_ = p.tcpln.Close()
	}
	// All block-persistence paths are serialized by pbftLock. Waiting for this
	// lock drains a handler that entered before stop; broadcasting while holding
	// it releases condition waiters, which re-check stopSignal before use.
	p.pbftLock.Lock()
	p.conditionalVarpbftLock.Broadcast()
	p.closePbft()
	p.pbftLock.Unlock()
	p.pl.Plog.Println("handled stop message in TCPListen Routine")
	select {
	case p.pStop <- 1:
	default:
	}
	close(done)
}

func (p *PbftConsensusNode) shutdownCompletion() chan struct{} {
	p.shutdownDoneOnce.Do(func() { p.shutdownDone = make(chan struct{}) })
	return p.shutdownDone
}

// WaitForShutdown keeps the process entrypoint alive when Propose observes the
// stop flag before the stop handler finishes exporting state and closing stores.
// It is called after Propose returns, not from the stop handler or under pbftLock.
func (p *PbftConsensusNode) WaitForShutdown() {
	if p.stopSignal.Load() {
		<-p.shutdownCompletion()
	}
}

// close the pbft
func (p *PbftConsensusNode) closePbft() {
	p.chainCloseOnce.Do(func() {
		if p.CurChain != nil {
			if err := p.writeCLPAEpochFinalState(); err != nil {
				panic(err)
			}
			p.CurChain.CloseBlockChain()
		}
		p.chainClosed.Store(true)
	})
}
