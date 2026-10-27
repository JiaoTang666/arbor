// Supervisor is an abstract role in this simulator that may read txs, generate partition infos,
// and handle history data.

package supervisor

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/committee"
	"blockEmulator/supervisor/measure"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type Supervisor struct {
	// basic infos
	IPaddr       string // ip address of this Supervisor
	ChainConfig  *params.ChainConfig
	Ip_nodeTable map[uint64]map[uint64]string

	// tcp control
	listenStop bool
	tcpLn      net.Listener
	tcpLock    sync.Mutex
	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss *signal.StopSignal // to control the stop message sending

	// supervisor and committee components
	comMod committee.CommitteeModule

	// measure components
	testMeasureMods []measure.MeasureModule

	// diy, add more structures or classes here ...
}

func (d *Supervisor) NewSupervisor(ip string, pcc *params.ChainConfig, committeeMethod string, measureModNames ...string) {
	d.IPaddr = ip
	d.ChainConfig = pcc
	d.Ip_nodeTable = params.IPmap_nodeTable

	d.sl = supervisor_log.NewSupervisorLog()

	d.Ss = signal.NewStopSignal(3*int(pcc.ShardNums), int(pcc.ShardNums))

	switch committeeMethod {
	case "CLPA_Broker":
		d.comMod = committee.NewCLPACommitteeMod_Broker(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
	case "CLPA":
		d.comMod = committee.NewCLPACommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
	case "Broker":
		d.comMod = committee.NewBrokerCommitteeMod(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize)
	default:
		d.comMod = committee.NewRelayCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize)
	}

	d.testMeasureMods = make([]measure.MeasureModule, 0)
	for _, mModName := range measureModNames {
		switch mModName {
		case "TPS_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Relay())
		case "TPS_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Broker())
		case "TCL_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Relay())
		case "TCL_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Broker())
		case "CrossTxRate_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Relay())
		case "CrossTxRate_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Broker())
		case "TxNumberCount_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Relay())
		case "TxNumberCount_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Broker())
		case "Tx_Details":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxDetail())
		default:
		}
	}
}

// Supervisor received the block information from the leaders, and handle these
// message to measure the performances.
func (d *Supervisor) handleBlockInfos(content []byte) {
	bim := new(message.BlockInfoMsg)
	err := json.Unmarshal(content, bim)
	if err != nil {
		log.Panic()
	}
	d.Ss.ObserveBlockInfo(bim.SenderShardID, bim.BlockBodyLength)
	d.Ss.ObserveSecondLegProgress(
		len(bim.Relay1Txs)+len(bim.Broker1Txs),
		len(bim.Relay2Txs)+len(bim.Broker2Txs),
	)
	stopSnapshot := d.Ss.Snapshot()
	if bim.BlockBodyLength == 0 {
		if stopSnapshot.GlobalStopGap == 1 ||
			stopSnapshot.GlobalStopGap%max(1, int(d.ChainConfig.ShardNums)) == 0 ||
			stopSnapshot.PerShardReady {
			d.sl.Slog.Printf(
				"Supervisor stop-state idle update: shard=%d bodyLen=%d globalGap=%d/%d idleShards=%d/%d pendingSecondLeg=%d drainReady=%v perShardThreshold=%d degradedIdleTarget=%d terminalIdleTarget=%d terminalQuietMs=%d quietForMs=%d perShardReady=%v degradedReady=%v terminalReady=%v seenNonEmpty=%v\n",
				bim.SenderShardID,
				bim.BlockBodyLength,
				stopSnapshot.GlobalStopGap,
				stopSnapshot.GlobalStopThreshold,
				stopSnapshot.IdleShardCount,
				stopSnapshot.RequiredShards,
				stopSnapshot.PendingSecondLegTx,
				stopSnapshot.SecondLegDrainReady,
				stopSnapshot.PerShardEmptyThreshold,
				stopSnapshot.DegradedIdleTarget,
				stopSnapshot.TerminalIdleTarget,
				stopSnapshot.TerminalQuietPeriodMs,
				stopSnapshot.QuietFor.Milliseconds(),
				stopSnapshot.PerShardReady,
				stopSnapshot.DegradedReady,
				stopSnapshot.TerminalReady,
				stopSnapshot.HasSeenNonEmptyBlock,
			)
		}
	} else {
		d.sl.Slog.Printf(
			"Supervisor stop-state reset by shard=%d bodyLen=%d pendingSecondLeg=%d drainReady=%v quietForMs=%d seenNonEmpty=%v\n",
			bim.SenderShardID,
			bim.BlockBodyLength,
			stopSnapshot.PendingSecondLegTx,
			stopSnapshot.SecondLegDrainReady,
			stopSnapshot.QuietFor.Milliseconds(),
			stopSnapshot.HasSeenNonEmptyBlock,
		)
	}

	d.comMod.HandleBlockInfo(bim)

	// measure update
	for _, measureMod := range d.testMeasureMods {
		measureMod.UpdateMeasureRecord(bim)
	}
	// add codes here ...
}

// read transactions from dataFile. When the number of data is enough,
// the Supervisor will do re-partition and send partitionMSG and txs to leaders.
func (d *Supervisor) SupervisorTxHandling() {
	stopNativeControl := func() {}
	if controller, ok := d.comMod.(interface{ StartNativeControl() func() }); ok {
		stopNativeControl = controller.StartNativeControl()
		defer stopNativeControl()
	}
	d.comMod.MsgSendingControl()
	forceCloseoutPath := os.Getenv("BLOCK_EMULATOR_FORCE_CLOSEOUT_PATH")
	// TxHandling is end
	groupReady := func() bool {
		if c, ok := d.comMod.(interface{ GroupCompletionReady() bool }); ok {
			return c.GroupCompletionReady()
		}
		return true
	}
	for !d.Ss.GapEnough() || !groupReady() { // wait all txs to be handled
		if forceCloseoutPath != "" {
			if _, err := os.Stat(forceCloseoutPath); err == nil {
				d.sl.Slog.Printf("Supervisor force closeout requested by %s\n", forceCloseoutPath)
				break
			}
		}
		snapshot := d.Ss.Snapshot()
		d.sl.Slog.Printf(
			"Supervisor waiting for closeout: globalGap=%d/%d idleShards=%d/%d pendingSecondLeg=%d drainReady=%v perShardThreshold=%d degradedIdleTarget=%d terminalIdleTarget=%d terminalQuietMs=%d quietForMs=%d perShardReady=%v degradedReady=%v terminalReady=%v seenNonEmpty=%v\n",
			snapshot.GlobalStopGap,
			snapshot.GlobalStopThreshold,
			snapshot.IdleShardCount,
			snapshot.RequiredShards,
			snapshot.PendingSecondLegTx,
			snapshot.SecondLegDrainReady,
			snapshot.PerShardEmptyThreshold,
			snapshot.DegradedIdleTarget,
			snapshot.TerminalIdleTarget,
			snapshot.TerminalQuietPeriodMs,
			snapshot.QuietFor.Milliseconds(),
			snapshot.PerShardReady,
			snapshot.DegradedReady,
			snapshot.TerminalReady,
			snapshot.HasSeenNonEmptyBlock,
		)
		time.Sleep(time.Second)
	}
	// send stop message
	stopNativeControl()
	var nativeCloseoutErr error
	if controller, ok := d.comMod.(interface{ NativeCloseoutError() error }); ok {
		nativeCloseoutErr = controller.NativeCloseoutError()
		if nativeCloseoutErr != nil {
			d.sl.Slog.Printf("NATIVE_CLOSEOUT_FAIL: %v", nativeCloseoutErr)
		}
	}
	stopmsg := message.MergeMessage(message.CStop, []byte("this is a stop message~"))
	d.sl.Slog.Println("Supervisor: now sending cstop message to all nodes")
	stopResults := make(chan error, int(d.ChainConfig.ShardNums*d.ChainConfig.Nodes_perShard))
	for sid := uint64(0); sid < d.ChainConfig.ShardNums; sid++ {
		for nid := uint64(0); nid < d.ChainConfig.Nodes_perShard; nid++ {
			go func(addr string) { stopResults <- networks.TcpDialWait(stopmsg, addr) }(d.Ip_nodeTable[sid][nid])
		}
	}
	for i := 0; i < cap(stopResults); i++ {
		if err := <-stopResults; err != nil {
			d.sl.Slog.Printf("stop message failed: %v", err)
			if nativeCloseoutErr == nil {
				nativeCloseoutErr = err
			}
		}
	}

	d.sl.Slog.Println("Supervisor: now Closing")
	d.listenStop = true
	d.CloseSupervisor()
	if nativeCloseoutErr != nil {
		panic(nativeCloseoutErr)
	}
}

// handle message. only one message to be handled now
func (d *Supervisor) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	switch msgType {
	case message.CBlockInfo:
		d.handleBlockInfos(content)
		// add codes for more functionality
	default:
		d.comMod.HandleOtherMessage(msg)
		for _, mm := range d.testMeasureMods {
			mm.HandleExtraMessage(msg)
		}
	}
}

func (d *Supervisor) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		switch err {
		case nil:
			// Lightweight queue observations have their own mutex and do not
			// wait behind full block receipt processing under tcpLock.
			t, _ := message.SplitMessage(clientRequest)
			if t == message.CPlacementTelemetryV2 {
				d.comMod.HandleOtherMessage(clientRequest)
				continue
			}
			d.tcpLock.Lock()
			d.handleMessage(clientRequest)
			d.tcpLock.Unlock()
		case io.EOF:
			log.Println("client closed the connection by terminating the process")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}
}

func (d *Supervisor) TcpListen() {
	ln, err := net.Listen("tcp", d.IPaddr)
	if err != nil {
		log.Panic(err)
	}
	d.tcpLn = ln
	for {
		conn, err := d.tcpLn.Accept()
		if err != nil {
			return
		}
		go d.handleClientRequest(conn)
	}
}

// close Supervisor, and record the data in .csv file
func (d *Supervisor) CloseSupervisor() {
	d.sl.Slog.Println("Closing...")
	for _, measureMod := range d.testMeasureMods {
		d.sl.Slog.Println(measureMod.OutputMetricName())
		d.sl.Slog.Println(measureMod.OutputRecord())
		println()
	}
	networks.CloseAllConnInPool()
	d.tcpLn.Close()
}
