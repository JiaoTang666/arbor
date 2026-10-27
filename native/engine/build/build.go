package build

import (
	"blockEmulator/consensus_shard/pbft_all"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/startupgate"
	"blockEmulator/supervisor"
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"time"
)

func initConfig(nid, nnm, sid, snm uint64) *params.ChainConfig {
	// Read the contents of ipTable.json
	ipMap := readIpTable("./ipTable.json")
	params.IPmap_nodeTable = ipMap
	params.SupervisorAddr = params.IPmap_nodeTable[params.SupervisorShard][0]

	// check the correctness of params
	if len(ipMap)-1 < int(snm) {
		log.Panicf("Input ShardNumber = %d, but only %d shards in ipTable.json.\n", snm, len(ipMap)-1)
	}
	for shardID := 0; shardID < len(ipMap)-1; shardID++ {
		if len(ipMap[uint64(shardID)]) < int(nnm) {
			log.Panicf("Input NodeNumber = %d, but only %d nodes in Shard %d.\n", nnm, len(ipMap[uint64(shardID)]), shardID)
		}
	}

	params.NodesInShard = int(nnm)
	params.ShardNum = int(snm)

	// init the network layer
	networks.InitNetworkTools()

	pcc := &params.ChainConfig{
		ChainID:        sid,
		NodeID:         nid,
		ShardID:        sid,
		Nodes_perShard: uint64(params.NodesInShard),
		ShardNums:      snm,
		BlockSize:      uint64(params.MaxBlockSize_global),
		BlockInterval:  uint64(params.Block_Interval),
		InjectSpeed:    uint64(params.InjectSpeed),
	}
	return pcc
}

func BuildSupervisor(nnm, snm uint64) {
	gateDir := os.Getenv("BLOCK_EMULATOR_STARTUP_GATE_DIR")
	if os.Getenv("BLOCK_EMULATOR_INITIAL_LEDGER_PATH") != "" && gateDir == "" {
		log.Panic("initial ledger supervisor requires explicit startup gate")
	}
	methodID := params.ConsensusMethod
	var measureMod []string
	if methodID == 0 || methodID == 2 {
		measureMod = params.MeasureBrokerMod
	} else {
		measureMod = params.MeasureRelayMod
	}
	measureMod = append(measureMod, "Tx_Details")

	lsn := new(supervisor.Supervisor)
	config := initConfig(123, nnm, 123, snm)
	lsn.NewSupervisor(params.SupervisorAddr, config, params.CommitteeMethod[methodID], measureMod...)
	go lsn.TcpListen()
	if gateDir != "" {
		path := os.Getenv("BLOCK_EMULATOR_CONFIG_PATH")
		if path == "" {
			path = "paramsConfig.json"
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			log.Panic(err)
		}
		binding := startupgate.Binding{LedgerHash: os.Getenv("BLOCK_EMULATOR_INITIAL_LEDGER_SHA256"), ConfigHash: fmt.Sprintf("%x", sha256.Sum256(raw)), Shards: snm, Nodes: nnm}
		endpoints := []string{}
		for s := uint64(0); s < snm; s++ {
			for n := uint64(0); n < nnm; n++ {
				endpoints = append(endpoints, params.IPmap_nodeTable[s][n])
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		log.Printf("startup gate waiting: %s; no workload until explicit release", gateDir)
		if err := startupgate.Wait(ctx, gateDir, binding, endpoints, startupgate.Probe); err != nil {
			log.Panic(err)
		}
		log.Print("startup gate released; entering existing transaction handling")
	} else {
		time.Sleep(5000 * time.Millisecond)
	}
	lsn.SupervisorTxHandling()
}

func BuildNewPbftNode(nid, nnm, sid, snm uint64) {
	methodID := params.ConsensusMethod
	worker := pbft_all.NewPbftNode(sid, nid, initConfig(nid, nnm, sid, snm), params.CommitteeMethod[methodID])
	defer func() {
		if err := worker.CurChain.CloseOwnershipDiagnostics(); err != nil {
			log.Panicf("ownership diagnostic close failed: %v", err)
		}
	}()
	go worker.TcpListen()
	worker.Propose()
	worker.WaitForShutdown()
}
