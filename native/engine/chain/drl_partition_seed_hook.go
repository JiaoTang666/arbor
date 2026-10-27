package chain

import (
	"blockEmulator/utils"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

const externalClusterShardMapPathEnv = "BLOCK_EMULATOR_CLUSTER_SHARD_MAP_PATH"
const runtimeUnknownAdmissionEnabledSeedEnv = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_ADMISSION_ENABLED"

// SeedPartitionMapFromClusterAssignmentIfEnabled loads the split-aware initial
// stable-cluster placement directly into the in-memory PartitionMap once per
// node. This gives native snapshots a real object universe from epoch 1,
// instead of relying on Addr2Shard fallback that scatters clustered accounts.
func (bc *BlockChain) SeedPartitionMapFromClusterAssignmentIfEnabled() int {
	if bc.initialLedger != nil {
		return 0
	}
	start := time.Now()
	accountClusterMapPath := strings.TrimSpace(os.Getenv(externalAccountClusterMapPathEnv))
	clusterShardMapPath := strings.TrimSpace(os.Getenv(externalClusterShardMapPathEnv))
	if accountClusterMapPath == "" || clusterShardMapPath == "" {
		return 0
	}

	bc.partitionSeedLock.Lock()
	defer bc.partitionSeedLock.Unlock()
	if bc.partitionSeeded {
		return 0
	}

	targetShardByCluster, err := loadStableClusterShardAssignments(clusterShardMapPath)
	if err != nil || len(targetShardByCluster) == 0 {
		return 0
	}

	handle, err := os.Open(accountClusterMapPath)
	if err != nil {
		return 0
	}
	defer handle.Close()

	reader := csv.NewReader(handle)
	header, err := reader.Read()
	if err != nil {
		return 0
	}
	indexByName := make(map[string]int, len(header))
	for idx, column := range header {
		indexByName[strings.TrimSpace(column)] = idx
	}
	accountIndex, okAccount := indexByName["account"]
	clusterIndex, okCluster := indexByName["cluster_id"]
	projectedUnknownIndex, hasProjectedUnknown := indexByName["is_projected_unknown"]
	warmupSeedAllowedIndex, hasWarmupSeedAllowed := indexByName["causal_warmup_seed_allowed"]
	if !okAccount || !okCluster {
		return 0
	}
	runtimeAdmissionEnabled := parseSeedTruthValue(strings.TrimSpace(os.Getenv(runtimeUnknownAdmissionEnabledSeedEnv)))
	pendingAssignments := make(map[string]uint64)
	for {
		row, readErr := reader.Read()
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return len(pendingAssignments)
		}
		account := strings.ToLower(strings.TrimSpace(row[accountIndex]))
		if !strings.HasPrefix(account, "0x") {
			continue
		}
		accountKey := normalizePartitionMapKey(account)
		if accountKey == "" {
			continue
		}
		if runtimeAdmissionEnabled && hasProjectedUnknown && parseSeedTruthValue(row[projectedUnknownIndex]) {
			if !hasWarmupSeedAllowed || !parseSeedTruthValue(row[warmupSeedAllowedIndex]) {
				continue
			}
		}
		clusterID, parseErr := strconv.Atoi(strings.TrimSpace(row[clusterIndex]))
		if parseErr != nil {
			continue
		}
		targetShard, ok := targetShardByCluster[clusterID]
		if !ok {
			continue
		}
		if uint64(utils.Addr2Shard(accountKey)) == targetShard {
			continue
		}
		pendingAssignments[accountKey] = targetShard
	}
	if prepared := bc.SourcePreparation(); prepared != nil {
		pendingAssignments[prepared.Command.Account] = prepared.Command.SourceShard
	}
	for _, prepared := range bc.GroupSourcePreparations() {
		for _, member := range prepared.Command.Members {
			pendingAssignments[member.Account] = member.SourceShard
		}
	}
	if len(pendingAssignments) > 0 {
		bc.pmlock.Lock()
		for account, targetShard := range pendingAssignments {
			bc.PartitionMap[account] = targetShard
		}
		bc.pmlock.Unlock()
	}
	seeded := len(pendingAssignments)
	bc.partitionSeeded = true
	fmt.Printf("SeedPartitionMapFromClusterAssignmentIfEnabled shard=%d seeded=%d elapsedMs=%d\n", bc.ChainConfig.ShardID, seeded, time.Since(start).Milliseconds())
	return seeded
}

func parseSeedTruthValue(value string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	return trimmed == "1" || trimmed == "true" || trimmed == "yes" || trimmed == "y"
}

func loadStableClusterShardAssignments(path string) (map[int]uint64, error) {
	handle, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer handle.Close()

	reader := csv.NewReader(handle)
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}
	indexByName := make(map[string]int, len(header))
	for idx, column := range header {
		indexByName[strings.TrimSpace(column)] = idx
	}
	clusterIndex, okCluster := indexByName["cluster_id"]
	shardIndex, okShard := indexByName["shard_id"]
	typeIndex, okType := indexByName["cluster_type"]
	if !okCluster || !okShard || !okType {
		return nil, io.ErrUnexpectedEOF
	}

	assignments := make(map[int]uint64)
	for {
		row, readErr := reader.Read()
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return assignments, readErr
		}
		clusterType := strings.ToLower(strings.TrimSpace(row[typeIndex]))
		if clusterType != "stable" && clusterType != "virtual_stable" {
			continue
		}
		clusterID, clusterErr := strconv.Atoi(strings.TrimSpace(row[clusterIndex]))
		shardID, shardErr := strconv.Atoi(strings.TrimSpace(row[shardIndex]))
		if clusterErr != nil || shardErr != nil || clusterID < 0 || shardID < 0 {
			continue
		}
		assignments[clusterID] = uint64(shardID)
	}
	return assignments, nil
}
