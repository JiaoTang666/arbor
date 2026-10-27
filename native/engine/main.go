package main

import (
	"blockEmulator/build"
	"blockEmulator/params"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/spf13/pflag"
)

var (
	// network config
	shardNum int
	nodeNum  int
	shardID  int
	nodeID   int

	// supervisor or not
	isSupervisor bool

	// batch running config
	isGen                bool
	isGenerateForExeFile bool
)

func main() {
	// Read basic configs
	params.ReadConfigFile()
	if dataset := os.Getenv("BLOCK_EMULATOR_DATASET_FILE"); dataset != "" {
		params.DatasetFile = dataset
	}
	applyIntEnv := func(name string, dst *int) {
		if value := os.Getenv(name); value != "" {
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed <= 0 {
				log.Printf("ignoring invalid %s=%q", name, value)
				return
			}
			*dst = parsed
		}
	}
	applyIntEnv("BLOCK_EMULATOR_TOTAL_DATA_SIZE", &params.TotalDataSize)
	applyIntEnv("BLOCK_EMULATOR_TX_BATCH_SIZE", &params.TxBatchSize)
	applyIntEnv("BLOCK_EMULATOR_INJECT_SPEED", &params.InjectSpeed)
	applyIntEnv("BLOCK_EMULATOR_RECONFIG_GAP", &params.ReconfigTimeGap)
	log.Printf("effective runtime config: dataset=%s total=%d batch=%d inject=%d reconfig_gap=%d", params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.InjectSpeed, params.ReconfigTimeGap)

	// Generate bat files
	pflag.BoolVarP(&isGen, "gen", "g", false, "isGen is a bool value, which indicates whether to generate a batch file")
	pflag.BoolVarP(&isGenerateForExeFile, "shellForExe", "f", false, "isGenerateForExeFile is a bool value, which is effective only if 'isGen' is true; True to generate for an executable, False for 'go run'. ")

	// Start a node.
	pflag.IntVarP(&shardNum, "shardNum", "S", params.ShardNum, "shardNum is an Integer, which indicates that how many shards are deployed. ")
	pflag.IntVarP(&nodeNum, "nodeNum", "N", params.NodesInShard, "nodeNum is an Integer, which indicates how many nodes of each shard are deployed. ")
	pflag.IntVarP(&shardID, "shardID", "s", -1, "shardID is an Integer, which indicates the ID of the shard to which this node belongs. Value range: [0, shardNum). ")
	pflag.IntVarP(&nodeID, "nodeID", "n", -1, "nodeID is an Integer, which indicates the ID of this node. Value range: [0, nodeNum).")
	pflag.BoolVarP(&isSupervisor, "supervisor", "c", false, "isSupervisor is a bool value, which indicates whether this node is a supervisor.")

	pflag.Parse()

	params.ShardNum = shardNum
	params.NodesInShard = nodeNum

	if isGen {
		if isGenerateForExeFile {
			// Generate the corresponding .bat file or .sh file based on the detected operating system.
			if err := build.GenerateExeBatchByIpTable(nodeNum, shardNum); err != nil {
				fmt.Println(err.Error())
			}
		} else {
			// Generate a .bat file or .sh file for running `go run`.
			if err := build.GenerateBatchByIpTable(nodeNum, shardNum); err != nil {
				fmt.Println(err.Error())
			}
		}
		return
	}

	if isSupervisor {
		build.BuildSupervisor(uint64(nodeNum), uint64(shardNum))
	} else {
		if shardID >= shardNum || shardID < 0 {
			log.Panicf("Wrong ShardID. This ShardID is %d, but only %d shards in the current config. ", shardID, shardNum)
		}
		if nodeID >= nodeNum || nodeID < 0 {
			log.Panicf("Wrong NodeID. This NodeID is %d, but only %d nodes in the current config. ", nodeID, nodeNum)
		}
		build.BuildNewPbftNode(uint64(nodeID), uint64(nodeNum), uint64(shardID), uint64(shardNum))
	}
}
