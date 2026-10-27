package chain

import (
	"blockEmulator/core"
	"bufio"
	"crypto/sha1"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const externalNativeSnapshotPathEnv = "BLOCK_EMULATOR_NATIVE_SNAPSHOT_PATH"
const externalNativeSnapshotObjectModeEnv = "BLOCK_EMULATOR_NATIVE_SNAPSHOT_OBJECT_MODE"
const externalNativeSnapshotObjectCapEnv = "BLOCK_EMULATOR_NATIVE_SNAPSHOT_OBJECT_CAP"
const externalNativeProposeSnapshotObjectsEnv = "BLOCK_EMULATOR_NATIVE_PROPOSE_SNAPSHOT_OBJECTS_ENABLED"
const externalNativeCommitSnapshotObjectsEnv = "BLOCK_EMULATOR_NATIVE_COMMIT_SNAPSHOT_OBJECTS_ENABLED"

type NativeShardSnapshotRecord struct {
	ShardID           uint64  `json:"shard_id"`
	Load              float64 `json:"load"`
	QueueLength       int     `json:"queue_length"`
	RelayQueueLength  int     `json:"relay_queue_length"`
	PendingTxCount    int     `json:"pending_tx_count"`
	CommittedTxCount  int     `json:"committed_tx_count"`
	AvgLatencyMs      float64 `json:"avg_latency_ms"`
	P50LatencyMs      float64 `json:"p50_latency_ms"`
	P99LatencyMs      float64 `json:"p99_latency_ms"`
	P999LatencyMs     float64 `json:"p999_latency_ms"`
	CrossShardRatio   float64 `json:"cross_shard_ratio"`
	TxCount           int     `json:"tx_count"`
	Hot               bool    `json:"hot"`
	LocalAccountCount int     `json:"local_account_count"`
}

// NativeCompletedInteractionRecord is the transaction-level causal evidence
// emitted only when a logical interaction has reached its final commit stage.
// Relay stage one is intentionally excluded.
type NativeCompletedInteractionRecord struct {
	TxHash               string `json:"tx_hash"`
	IngressSequence      uint64 `json:"ingress_sequence"`
	RoutingEpoch         int    `json:"routing_epoch"`
	MappingVersion       uint64 `json:"mapping_version"`
	Sender               string `json:"sender"`
	Recipient            string `json:"recipient"`
	RoutedSenderShard    uint64 `json:"routed_sender_shard"`
	RoutedRecipientShard uint64 `json:"routed_recipient_shard"`
	RouteStatus          string `json:"route_status"`
	CompletionStage      string `json:"completion_stage"`
}

type NativeEpochSnapshotRecord struct {
	SnapshotScope         string                             `json:"snapshot_scope"`
	EpochID               int                                `json:"epoch_id"`
	EffectiveEpoch        int                                `json:"effective_epoch"`
	StateVersion          string                             `json:"state_version"`
	PolicyVersionHint     string                             `json:"policy_version_hint"`
	EmitterShardID        uint64                             `json:"emitter_shard_id"`
	EmitterNodeID         uint64                             `json:"emitter_node_id"`
	IsLeader              bool                               `json:"is_leader"`
	Timestamp             string                             `json:"timestamp"`
	CurrentBlockHeight    uint64                             `json:"current_block_height"`
	Shards                []NativeShardSnapshotRecord        `json:"shards"`
	CompletedInteractions []NativeCompletedInteractionRecord `json:"completed_interactions"`
	StableObjects         []map[string]interface{}           `json:"stable_objects"`
	MicroObjects          []map[string]interface{}           `json:"micro_objects"`
	TotalTxCount          int                                `json:"total_tx_count"`
	CrossShardTxCount     int                                `json:"cross_shard_tx_count"`
	AvgLatencyMs          float64                            `json:"avg_latency_ms"`
	P50LatencyMs          float64                            `json:"p50_latency_ms"`
	P99LatencyMs          float64                            `json:"p99_latency_ms"`
	P999LatencyMs         float64                            `json:"p999_latency_ms"`
	TailLatencyMs         float64                            `json:"tail_latency_ms"`
	TPS                   float64                            `json:"tps"`
	Notes                 []string                           `json:"notes"`
}

func (bc *BlockChain) ExportNativeEpochSnapshotIfEnabled(currentEpoch int, shardID uint64, nodeID uint64, isLeader bool) {
	outputPath := os.Getenv(externalNativeSnapshotPathEnv)
	if outputPath == "" || !isLeader {
		return
	}

	queueLength, relayPoolSize := bc.snapshotTxpoolSizes()
	localAccountCount, partitionMapSize := bc.snapshotPartitionCounts(shardID)
	accountClusterMapPath := os.Getenv(externalAccountClusterMapPathEnv)
	stableObjects := []map[string]interface{}{}
	microObjects := []map[string]interface{}{}
	objectSnapshotCount := 0
	// Proposal snapshots are on the critical path before a block is generated.
	// Full object scans over 10m-scale account maps can stall PBFT before the
	// first proposal is broadcast. By default, export shard execution pressure
	// quickly and let the Python runtime tracker append already-observed runtime
	// micro objects for DRL decisions. Full object snapshots remain available as
	// an explicit debugging/ablation option.
	if parseSnapshotBool(os.Getenv(externalNativeProposeSnapshotObjectsEnv), false) {
		stableObjects, microObjects, objectSnapshotCount = bc.snapshotNativeObjectsForShard(currentEpoch, shardID, localAccountCount, queueLength, accountClusterMapPath)
	}
	load := float64(queueLength + relayPoolSize)
	record := NativeEpochSnapshotRecord{
		SnapshotScope:      "shard_local_propose_boundary",
		EpochID:            currentEpoch,
		EffectiveEpoch:     currentEpoch + 1,
		StateVersion:       fmt.Sprintf("native-epoch-%d-height-%d-shard-%d", currentEpoch, bc.CurrentBlock.Header.Number, shardID),
		PolicyVersionHint:  "",
		EmitterShardID:     shardID,
		EmitterNodeID:      nodeID,
		IsLeader:           isLeader,
		Timestamp:          time.Now().UTC().Format(time.RFC3339Nano),
		CurrentBlockHeight: bc.CurrentBlock.Header.Number,
		Shards: []NativeShardSnapshotRecord{
			{
				ShardID:           shardID,
				Load:              load,
				QueueLength:       queueLength,
				RelayQueueLength:  relayPoolSize,
				PendingTxCount:    queueLength + relayPoolSize,
				CommittedTxCount:  0,
				AvgLatencyMs:      0.0,
				P50LatencyMs:      0.0,
				P99LatencyMs:      0.0,
				P999LatencyMs:     0.0,
				CrossShardRatio:   0.0,
				TxCount:           queueLength,
				Hot:               queueLength > 0 && queueLength >= 2*maxInt(1, relayPoolSize),
				LocalAccountCount: localAccountCount,
			},
		},
		StableObjects:     stableObjects,
		MicroObjects:      microObjects,
		TotalTxCount:      queueLength,
		CrossShardTxCount: relayPoolSize,
		AvgLatencyMs:      0.0,
		P50LatencyMs:      0.0,
		P99LatencyMs:      0.0,
		P999LatencyMs:     0.0,
		TailLatencyMs:     0.0,
		TPS:               0.0,
		Notes: []string{
			"leader_only_snapshot",
			"shard_local_native_boundary",
			fmt.Sprintf("partition_map_size=%d", partitionMapSize),
			fmt.Sprintf("object_snapshot_count=%d", objectSnapshotCount),
		},
	}

	appendJSONLRecord(outputPath, record)
}

func (bc *BlockChain) ExportNativeCommitSnapshotIfEnabled(
	currentEpoch int,
	shardID uint64,
	nodeID uint64,
	isLeader bool,
	block *core.Block,
	commitTime time.Time,
	innerShardTxs []*core.Transaction,
	relay1Txs []*core.Transaction,
	relay2Txs []*core.Transaction,
) {
	outputPath := os.Getenv(externalNativeSnapshotPathEnv)
	if outputPath == "" || !isLeader || block == nil {
		return
	}

	queueLength, relayPoolSize := bc.snapshotTxpoolSizes()
	localAccountCount, partitionMapSize := bc.snapshotPartitionCounts(shardID)
	accountClusterMapPath := os.Getenv(externalAccountClusterMapPathEnv)
	postEpoch := currentEpoch + 1
	stableObjects := []map[string]interface{}{}
	microObjects := []map[string]interface{}{}
	objectSnapshotCount := 0
	// Commit snapshots are used as execution evidence for latency/TPS and
	// post-update observation. Rebuilding the full object universe here can
	// block BlockInfoMsg delivery for minutes on 10m-scale maps, so keep commit
	// snapshots lightweight unless explicitly requested for debugging.
	if parseSnapshotBool(os.Getenv(externalNativeCommitSnapshotObjectsEnv), false) {
		stableObjects, microObjects, objectSnapshotCount = bc.snapshotNativeObjectsForShard(postEpoch, shardID, localAccountCount, queueLength, accountClusterMapPath)
	}
	load := float64(queueLength + relayPoolSize + len(block.Body))
	totalTxCount := len(block.Body)
	innerShardCount := len(innerShardTxs)
	relay1Count := len(relay1Txs)
	relay2Count := len(relay2Txs)
	crossShardTxCount := relay1Count + relay2Count
	avgLatencyMs, p50LatencyMs, p99LatencyMs, p999LatencyMs, tailLatencyMs := snapshotBlockLatencyStats(block.Body, commitTime)
	tps := estimateSnapshotBlockTPS(totalTxCount, avgLatencyMs, block.Body, commitTime)
	record := NativeEpochSnapshotRecord{
		SnapshotScope:      "shard_local_commit_boundary",
		EpochID:            postEpoch,
		EffectiveEpoch:     postEpoch + 1,
		StateVersion:       fmt.Sprintf("native-epoch-%d-height-%d-shard-%d", postEpoch, block.Header.Number, shardID),
		PolicyVersionHint:  "",
		EmitterShardID:     shardID,
		EmitterNodeID:      nodeID,
		IsLeader:           isLeader,
		Timestamp:          commitTime.UTC().Format(time.RFC3339Nano),
		CurrentBlockHeight: block.Header.Number,
		Shards: []NativeShardSnapshotRecord{
			{
				ShardID:           shardID,
				Load:              load,
				QueueLength:       queueLength,
				RelayQueueLength:  relayPoolSize,
				PendingTxCount:    queueLength + relayPoolSize,
				CommittedTxCount:  totalTxCount,
				AvgLatencyMs:      avgLatencyMs,
				P50LatencyMs:      p50LatencyMs,
				P99LatencyMs:      p99LatencyMs,
				P999LatencyMs:     p999LatencyMs,
				CrossShardRatio:   safeSnapshotRatio(crossShardTxCount, totalTxCount),
				TxCount:           totalTxCount,
				Hot:               totalTxCount > 0 && crossShardTxCount >= maxInt(innerShardCount, 1),
				LocalAccountCount: localAccountCount,
			},
		},
		CompletedInteractions: completedInteractionRecords(innerShardTxs, relay2Txs),
		StableObjects:         stableObjects,
		MicroObjects:          microObjects,
		TotalTxCount:          totalTxCount,
		CrossShardTxCount:     crossShardTxCount,
		AvgLatencyMs:          avgLatencyMs,
		P50LatencyMs:          p50LatencyMs,
		P99LatencyMs:          p99LatencyMs,
		P999LatencyMs:         p999LatencyMs,
		TailLatencyMs:         tailLatencyMs,
		TPS:                   tps,
		Notes: []string{
			"leader_only_snapshot",
			"shard_local_commit_boundary",
			"completed_interactions_contract=v1",
			"completed_interactions_contract=v1_final_only",
			fmt.Sprintf("partition_map_size=%d", partitionMapSize),
			fmt.Sprintf("object_snapshot_count=%d", objectSnapshotCount),
			fmt.Sprintf("block_tx_count=%d", totalTxCount),
			fmt.Sprintf("inner_shard_tx_count=%d", innerShardCount),
			fmt.Sprintf("relay1_tx_count=%d", relay1Count),
			fmt.Sprintf("relay2_tx_count=%d", relay2Count),
		},
	}

	appendJSONLRecord(outputPath, record)
}

func completedInteractionRecords(innerShardTxs, relay2Txs []*core.Transaction) []NativeCompletedInteractionRecord {
	records := make([]NativeCompletedInteractionRecord, 0, len(innerShardTxs)+len(relay2Txs))
	appendTx := func(tx *core.Transaction, stage string) {
		if tx == nil {
			return
		}
		records = append(records, NativeCompletedInteractionRecord{
			TxHash:               hex.EncodeToString(tx.TxHash),
			IngressSequence:      tx.IngressSequence,
			RoutingEpoch:         tx.RoutingEpoch,
			MappingVersion:       tx.MappingVersion,
			Sender:               string(tx.Sender),
			Recipient:            string(tx.Recipient),
			RoutedSenderShard:    tx.RoutedSenderShard,
			RoutedRecipientShard: tx.RoutedRecipientShard,
			RouteStatus:          "final",
			CompletionStage:      stage,
		})
	}
	for _, tx := range innerShardTxs {
		appendTx(tx, "direct_final")
	}
	for _, tx := range relay2Txs {
		appendTx(tx, "relay_final")
	}
	return records
}

func (bc *BlockChain) snapshotTxpoolSizes() (int, int) {
	if bc.Txpool == nil {
		return 0, 0
	}
	bc.Txpool.GetLocked()
	defer bc.Txpool.GetUnlocked()
	queueLength := bc.Txpool.PendingCountLocked()
	relayPoolSize := 0
	for _, shardPool := range bc.Txpool.RelayPool {
		relayPoolSize += len(shardPool)
	}
	return queueLength, relayPoolSize
}

func (bc *BlockChain) snapshotPartitionCounts(shardID uint64) (int, int) {
	if bc.ownership != nil {
		return bc.ownership.PartitionCounts(shardID)
	}
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	localCount := 0
	for _, mappedShard := range bc.PartitionMap {
		if mappedShard == shardID {
			localCount++
		}
	}
	return localCount, len(bc.PartitionMap)
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func safeSnapshotRatio(numerator int, denominator int) float64 {
	if denominator <= 0 {
		return 0.0
	}
	return float64(numerator) / float64(denominator)
}

func snapshotBlockLatencyStats(txs []*core.Transaction, commitTime time.Time) (float64, float64, float64, float64, float64) {
	if len(txs) == 0 {
		return 0.0, 0.0, 0.0, 0.0, 0.0
	}
	latencies := make([]float64, 0, len(txs))
	total := 0.0
	for _, tx := range txs {
		latency := float64(commitTime.Sub(tx.Time).Milliseconds())
		if latency < 0 {
			latency = 0
		}
		latencies = append(latencies, latency)
		total += latency
	}
	sort.Float64s(latencies)
	avg := total / float64(len(latencies))
	return avg,
		pickLatencyPercentile(latencies, 0.50),
		pickLatencyPercentile(latencies, 0.99),
		pickLatencyPercentile(latencies, 0.999),
		latencies[len(latencies)-1]
}

func pickLatencyPercentile(latencies []float64, quantile float64) float64 {
	if len(latencies) == 0 {
		return 0.0
	}
	if quantile <= 0 {
		return latencies[0]
	}
	if quantile >= 1 {
		return latencies[len(latencies)-1]
	}
	index := int(float64(len(latencies)-1) * quantile)
	if index < 0 {
		index = 0
	}
	if index >= len(latencies) {
		index = len(latencies) - 1
	}
	return latencies[index]
}

func estimateSnapshotBlockTPS(totalTxCount int, avgLatencyMs float64, txs []*core.Transaction, commitTime time.Time) float64 {
	if totalTxCount <= 0 {
		return 0.0
	}
	if len(txs) > 0 {
		earliest := txs[0].Time
		for _, tx := range txs[1:] {
			if tx.Time.Before(earliest) {
				earliest = tx.Time
			}
		}
		durationSeconds := commitTime.Sub(earliest).Seconds()
		if durationSeconds > 0 {
			return float64(totalTxCount) / durationSeconds
		}
	}
	if avgLatencyMs > 0 {
		return float64(totalTxCount) / (avgLatencyMs / 1000.0)
	}
	return float64(totalTxCount)
}

func (bc *BlockChain) snapshotNativeObjectsForShard(currentEpoch int, shardID uint64, localAccountCount int, queueLength int, accountClusterMapPath string) ([]map[string]interface{}, []map[string]interface{}, int) {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv(externalNativeSnapshotObjectModeEnv)))
	if mode == "" {
		mode = "budgeted"
	}
	capPerType := parseSnapshotObjectCap(os.Getenv(externalNativeSnapshotObjectCapEnv), 24)
	stableObjects := bc.snapshotStableObjectUniverseForShard(shardID, localAccountCount, queueLength, accountClusterMapPath)
	microObjects := bc.snapshotMicroObjectUniverseForShard(currentEpoch, shardID, localAccountCount, queueLength, accountClusterMapPath)
	if mode != "full" {
		recentHints := loadRecentNativeObjectHints(os.Getenv(externalMappingUpdatePathEnv), currentEpoch)
		stableObjects = topNativeObjectRows(stableObjects, capPerType, recentHints)
		microObjects = topNativeObjectRows(microObjects, capPerType, recentHints)
	}
	return stableObjects, microObjects, len(stableObjects) + len(microObjects)
}

func filterValidExternalAddresses(addresses []string) []string {
	filtered := make([]string, 0, len(addresses))
	for _, addr := range addresses {
		if isValidExternalMappingAddress(addr) {
			filtered = append(filtered, strings.ToLower(strings.TrimSpace(addr)))
		}
	}
	return filtered
}

func (bc *BlockChain) snapshotObjectCurrentShard(addresses []string) (uint64, bool) {
	if len(addresses) == 0 {
		return 0, false
	}
	observed := bc.Get_PartitionMap(addresses[0])
	for _, addr := range addresses[1:] {
		if bc.Get_PartitionMap(addr) != observed {
			return observed, false
		}
	}
	return observed, true
}

func (bc *BlockChain) snapshotStableObjectUniverseForShard(shardID uint64, localAccountCount int, queueLength int, accountClusterMapPath string) []map[string]interface{} {
	if accountClusterMapPath == "" {
		return []map[string]interface{}{}
	}
	cache, err := buildStableAccountCache(accountClusterMapPath)
	if err != nil || len(cache) == 0 {
		return []map[string]interface{}{}
	}
	accountStats, _ := buildStableAccountStats(accountClusterMapPath)
	objectIDs := make([]string, 0, len(cache))
	for objectID := range cache {
		objectIDs = append(objectIDs, objectID)
	}
	sort.Strings(objectIDs)
	rows := make([]map[string]interface{}, 0)
	for _, objectID := range objectIDs {
		addresses := filterValidExternalAddresses(cache[objectID])
		if len(addresses) == 0 {
			continue
		}
		currentShard, consistent := bc.snapshotObjectCurrentShard(addresses)
		if !consistent || currentShard != shardID {
			continue
		}
		score := stableObjectActivityScore(accountStats[objectID], addresses)
		accountSignature := buildAccountSignature(addresses)
		row := buildNativeObjectSnapshotRow(
			"stable",
			objectID,
			currentShard,
			len(addresses),
			maxInt(len(addresses), int(score)),
			score,
			len(addresses),
			localAccountCount,
			queueLength,
			objectID,
			objectID,
			accountSignature,
			"",
		)
		rows = append(rows, row)
	}
	return rows
}

func (bc *BlockChain) snapshotMicroObjectUniverseForShard(currentEpoch int, shardID uint64, localAccountCount int, queueLength int, accountClusterMapPath string) []map[string]interface{} {
	if accountClusterMapPath == "" {
		return []map[string]interface{}{}
	}
	dataRoot := filepath.Dir(accountClusterMapPath)
	featureRows, err := loadMicroFeatureRowsForEpoch(filepath.Join(dataRoot, "micro_cluster_features.csv"), currentEpoch)
	if err != nil || len(featureRows) == 0 {
		return []map[string]interface{}{}
	}
	rows := make([]map[string]interface{}, 0)
	for _, featureRow := range featureRows {
		objectID := fmt.Sprintf("micro:%d:%d", featureRow.EpochID, featureRow.MicroClusterID)
		addresses, err := loadMicroAccounts(accountClusterMapPath, objectID, currentEpoch)
		if err != nil || len(addresses) == 0 {
			continue
		}
		addresses = filterValidExternalAddresses(addresses)
		if len(addresses) == 0 {
			continue
		}
		currentShard, consistent := bc.snapshotObjectCurrentShard(addresses)
		if !consistent || currentShard != shardID {
			continue
		}
		accountCount := len(addresses)
		if featureRow.ActiveAccountCount > accountCount {
			accountCount = featureRow.ActiveAccountCount
		}
		score := float64(featureRow.ActiveAccountCount)
		parentStableObjectID := fmt.Sprintf("stable:%d", featureRow.ParentStableClusterID)
		lineageKey, lineageFamilyKey, accountSignature := buildMicroLineageIdentity(parentStableObjectID, addresses)
		row := buildNativeObjectSnapshotRow(
			"micro",
			objectID,
			currentShard,
			accountCount,
			maxInt(featureRow.ActiveAccountCount, len(addresses)),
			score,
			len(addresses),
			localAccountCount,
			queueLength,
			lineageKey,
			lineageFamilyKey,
			accountSignature,
			parentStableObjectID,
		)
		rows = append(rows, row)
	}
	return rows
}

func buildNativeObjectSnapshotRow(objectType string, objectID string, currentShard uint64, accountCount int, txCount int, activityScore float64, resolvedAccountCount int, localAccountCount int, queueLength int, lineageKey string, lineageFamilyKey string, accountSignature string, parentStableObjectID string) map[string]interface{} {
	score := activityScore
	if localAccountCount > 0 {
		score = activityScore / float64(localAccountCount)
	}
	queuePressure := 0.0
	if localAccountCount > 0 {
		queuePressure = float64(queueLength) / float64(localAccountCount)
	}
	return map[string]interface{}{
		"object_type":             objectType,
		"object_id":               objectID,
		"current_shard":           int(currentShard),
		"account_count":           accountCount,
		"tx_count":                txCount,
		"activity_score":          score,
		"latency_pressure":        queuePressure,
		"queue_pressure":          queuePressure,
		"hot":                     txCount >= 5 || score >= 0.05,
		"resolved_account_count":  resolvedAccountCount,
		"lineage_key":             lineageKey,
		"lineage_family_key":      lineageFamilyKey,
		"account_signature":       accountSignature,
		"parent_stable_object_id": parentStableObjectID,
	}
}

func buildAccountSignature(addresses []string) string {
	if len(addresses) == 0 {
		return ""
	}
	normalized := append([]string(nil), addresses...)
	sort.Strings(normalized)
	hash := sha1.Sum([]byte(strings.Join(normalized, "|")))
	return hex.EncodeToString(hash[:])[:12]
}

func buildMicroLineageIdentity(parentStableObjectID string, addresses []string) (string, string, string) {
	if parentStableObjectID == "" {
		return "", "", ""
	}
	accountSignature := buildAccountSignature(addresses)
	if accountSignature == "" {
		return parentStableObjectID, parentStableObjectID, ""
	}
	return fmt.Sprintf("%s|acctsig:%s", parentStableObjectID, accountSignature), parentStableObjectID, accountSignature
}

func stableObjectActivityScore(accountStats map[string]float64, addresses []string) float64 {
	score := float64(0)
	for _, addr := range addresses {
		score += accountStats[addr]
	}
	if score <= 0 {
		score = float64(len(addresses))
	}
	return score
}

func loadMicroFeatureRowsForEpoch(path string, epochID int) ([]*microFeatureRow, error) {
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
	required := []string{"epoch_id", "micro_cluster_id", "parent_stable_cluster_id", "active_account_count"}
	for _, name := range required {
		if _, ok := indexByName[name]; !ok {
			return nil, fmt.Errorf("micro_cluster_features missing column %s", name)
		}
	}
	rows := make([]*microFeatureRow, 0)
	for {
		row, readErr := reader.Read()
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return nil, readErr
		}
		if parseOptionalInt(row[indexByName["epoch_id"]]) != epochID {
			continue
		}
		rows = append(rows, &microFeatureRow{
			EpochID:               epochID,
			MicroClusterID:        parseOptionalInt(row[indexByName["micro_cluster_id"]]),
			ParentStableClusterID: parseOptionalInt(row[indexByName["parent_stable_cluster_id"]]),
			ActiveAccountCount:    parseOptionalInt(row[indexByName["active_account_count"]]),
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].MicroClusterID < rows[j].MicroClusterID
	})
	return rows, nil
}

func parseSnapshotObjectCap(raw string, defaultValue int) int {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(trimmed)
	if err != nil || value <= 0 {
		return defaultValue
	}
	return value
}

func parseSnapshotBool(raw string, defaultValue bool) bool {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if trimmed == "" {
		return defaultValue
	}
	return trimmed == "1" || trimmed == "true" || trimmed == "yes" || trimmed == "y"
}

type nativeObjectHint struct {
	recentCount        int
	lastEpochID        int
	lastEffectiveEpoch int
}

type externalMappingUpdateRecord struct {
	ObjectID             string `json:"object_id"`
	ObjectType           string `json:"object_type"`
	EpochID              int    `json:"epoch_id"`
	EffectiveEpoch       int    `json:"effective_epoch"`
	NativeTriggerEpochID int    `json:"native_trigger_epoch_id"`
}

func loadRecentNativeObjectHints(path string, currentEpoch int) map[string]nativeObjectHint {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return map[string]nativeObjectHint{}
	}
	handle, err := os.Open(trimmed)
	if err != nil {
		return map[string]nativeObjectHint{}
	}
	defer handle.Close()

	hints := make(map[string]nativeObjectHint)
	scanner := bufio.NewScanner(handle)
	buffer := make([]byte, 0, 64*1024)
	scanner.Buffer(buffer, 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record externalMappingUpdateRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		objectID := strings.TrimSpace(record.ObjectID)
		if objectID == "" {
			continue
		}
		epochMarker := record.NativeTriggerEpochID
		if epochMarker <= 0 {
			epochMarker = record.EpochID
		}
		if epochMarker <= 0 {
			epochMarker = record.EffectiveEpoch - 1
		}
		if epochMarker <= 0 || currentEpoch-epochMarker > 6 {
			continue
		}
		hint := hints[objectID]
		hint.recentCount++
		if epochMarker > hint.lastEpochID {
			hint.lastEpochID = epochMarker
			hint.lastEffectiveEpoch = record.EffectiveEpoch
		}
		hints[objectID] = hint
	}
	return hints
}

func topNativeObjectRows(rows []map[string]interface{}, capPerType int, recentHints map[string]nativeObjectHint) []map[string]interface{} {
	if capPerType <= 0 || len(rows) <= capPerType {
		return rows
	}
	sortedRows := append([]map[string]interface{}(nil), rows...)
	sort.Slice(sortedRows, func(i, j int) bool {
		left := nativeObjectPriority(sortedRows[i], recentHints)
		right := nativeObjectPriority(sortedRows[j], recentHints)
		if left != right {
			return left > right
		}
		return fmt.Sprintf("%v", sortedRows[i]["object_id"]) < fmt.Sprintf("%v", sortedRows[j]["object_id"])
	})
	return append([]map[string]interface{}(nil), sortedRows[:capPerType]...)
}

func nativeObjectPriority(row map[string]interface{}, recentHints map[string]nativeObjectHint) float64 {
	objectID := fmt.Sprintf("%v", row["object_id"])
	objectType := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", row["object_type"])))
	txCount := asFloat(row["tx_count"])
	resolved := asFloat(row["resolved_account_count"])
	accountCount := asFloat(row["account_count"])
	activity := asFloat(row["activity_score"])
	queuePressure := asFloat(row["queue_pressure"])
	latencyPressure := asFloat(row["latency_pressure"])
	hotBonus := 0.0
	if asBool(row["hot"]) {
		hotBonus = 40.0
	}
	recentBonus := 0.0
	if hint, ok := recentHints[objectID]; ok {
		recentBonus += float64(hint.recentCount) * 35.0
		if hint.lastEffectiveEpoch > 0 {
			recentBonus += 25.0
		}
	}
	if objectType == "micro" {
		return txCount*16.0 + resolved*7.0 + activity*20.0 + queuePressure*45.0 + latencyPressure*45.0 + hotBonus + recentBonus
	}
	return txCount*8.0 + resolved*4.0 + accountCount*2.0 + activity*10.0 + queuePressure*25.0 + latencyPressure*25.0 + hotBonus + recentBonus
}

func asFloat(value interface{}) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case int32:
		return float64(typed)
	case uint64:
		return float64(typed)
	case uint32:
		return float64(typed)
	case string:
		parsed, _ := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed
	default:
		return 0.0
	}
}

func asBool(value interface{}) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		lowered := strings.ToLower(strings.TrimSpace(typed))
		return lowered == "true" || lowered == "1" || lowered == "yes"
	case int:
		return typed != 0
	case int64:
		return typed != 0
	case float64:
		return typed != 0.0
	default:
		return false
	}
}
