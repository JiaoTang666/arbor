package chain

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const externalMappingUpdatePathEnv = "BLOCK_EMULATOR_EXTERNAL_MAPPING_UPDATE_PATH"
const externalMappingNodeApplyEnabledEnv = "BLOCK_EMULATOR_EXTERNAL_MAPPING_NODE_APPLY_ENABLED"
const runtimeUnknownMappingUpdatePathEnv = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_MAPPING_UPDATE_PATH"
const runtimeUnknownNodeApplyEnabledEnv = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_NODE_APPLY_ENABLED"
const runtimeUnknownRouterSafetyLagEnv = "BLOCK_EMULATOR_RUNTIME_UNKNOWN_ROUTER_SAFETY_LAG_EPOCHS"
const mappingVersionAuditDirEnv = "BLOCK_EMULATOR_MAPPING_VERSION_AUDIT_DIR"

type ExternalObjectMappingUpdate struct {
	ProposalID           string   `json:"proposal_id"`
	EpochID              int      `json:"epoch_id"`
	EffectiveEpoch       int      `json:"effective_epoch"`
	ObjectType           string   `json:"object_type"`
	ObjectID             string   `json:"object_id"`
	SourceShard          uint64   `json:"source_shard"`
	TargetShard          uint64   `json:"target_shard"`
	Addresses            []string `json:"addresses"`
	ResolvedAccountCount int      `json:"resolved_account_count"`
	ApplyMode            string   `json:"apply_mode"`
}

type cachedExternalObjectMappingUpdate struct {
	key    string
	update *ExternalObjectMappingUpdate
}

type externalObjectMappingUpdateCache struct {
	offset          int64
	pending         []*cachedExternalObjectMappingUpdate
	applied         map[string]bool
	appliedSequence uint64
	rollingHash     [32]byte
}

// ApplyExternalObjectMappingUpdatesIfDue is a minimal weakly coupled hook for
// externally prepared account-level remapping updates. When the environment
// variable is absent, it is a no-op and does not affect the current PBFT path.
func (bc *BlockChain) ApplyExternalObjectMappingUpdatesIfDue(currentEpoch int) int {
	appliedAccounts := 0
	if mappingNodeApplyEnabled(externalMappingNodeApplyEnabledEnv) {
		appliedAccounts += bc.applyExternalObjectMappingUpdatesFromPath(strings.TrimSpace(os.Getenv(externalMappingUpdatePathEnv)), currentEpoch)
	}
	runtimePath := strings.TrimSpace(os.Getenv(runtimeUnknownMappingUpdatePathEnv))
	// SAA decisions are materialized in the route frozen by the supervisor.
	// Nodes must not independently tail the admission JSONL: asynchronous file
	// polling can otherwise create transient per-node mappings within an epoch.
	if runtimeUnknownNodeApplyEnabled() && runtimePath != "" && runtimePath != strings.TrimSpace(os.Getenv(externalMappingUpdatePathEnv)) {
		appliedAccounts += bc.applyExternalObjectMappingUpdatesFromPath(runtimePath, currentEpoch)
	}
	return appliedAccounts
}

func mappingNodeApplyEnabled(envName string) bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(envName)))
	return raw == "1" || raw == "true" || raw == "yes" || raw == "on"
}

func runtimeUnknownNodeApplyEnabled() bool {
	return mappingNodeApplyEnabled(runtimeUnknownNodeApplyEnabledEnv)
}

func (bc *BlockChain) applyExternalObjectMappingUpdatesFromPath(updatePath string, currentEpoch int) int {
	if updatePath == "" {
		return 0
	}
	cache := bc.externalMappingUpdateCache(updatePath)
	if cache == nil {
		return 0
	}
	cache.loadNew(updatePath)
	safetyLag := parseMappingSafetyLag(os.Getenv(runtimeUnknownRouterSafetyLagEnv), 0)
	appliedAccounts := 0
	batchUpdateCount := 0
	batchAppliedAccounts := 0
	lastKey := ""
	var lastUpdate *ExternalObjectMappingUpdate
	remaining := cache.pending[:0]
	for _, entry := range cache.pending {
		if entry == nil || entry.update == nil {
			continue
		}
		update := entry.update
		if cache.applied[entry.key] {
			continue
		}
		if currentEpoch < update.EffectiveEpoch+safetyLag {
			remaining = append(remaining, entry)
			continue
		}
		appliedForUpdate := bc.ApplyExternalObjectMappingUpdate(update)
		appliedAccounts += appliedForUpdate
		batchAppliedAccounts += appliedForUpdate
		batchUpdateCount++
		lastKey = entry.key
		lastUpdate = update
		cache.applied[entry.key] = true
		cache.appliedSequence++
		cache.rollingHash = sha256.Sum256([]byte(fmt.Sprintf(
			"%x|%s|%d|%d|%d|%s",
			cache.rollingHash,
			entry.key,
			update.EffectiveEpoch,
			update.SourceShard,
			update.TargetShard,
			strings.Join(update.Addresses, ","),
		)))
	}
	cache.pending = remaining
	if batchUpdateCount > 0 && lastUpdate != nil {
		writeMappingVersionAudit(
			updatePath,
			currentEpoch,
			safetyLag,
			lastKey,
			lastUpdate,
			batchAppliedAccounts,
			batchUpdateCount,
			cache.appliedSequence,
			cache.rollingHash,
		)
	}
	return appliedAccounts
}

func (bc *BlockChain) externalMappingUpdateCache(updatePath string) *externalObjectMappingUpdateCache {
	if updatePath == "" {
		return nil
	}
	bc.mappingUpdateCacheLock.Lock()
	defer bc.mappingUpdateCacheLock.Unlock()
	if bc.mappingUpdateCaches == nil {
		bc.mappingUpdateCaches = make(map[string]*externalObjectMappingUpdateCache)
	}
	cache := bc.mappingUpdateCaches[updatePath]
	if cache == nil {
		cache = &externalObjectMappingUpdateCache{
			pending: make([]*cachedExternalObjectMappingUpdate, 0),
			applied: make(map[string]bool),
		}
		bc.mappingUpdateCaches[updatePath] = cache
	}
	return cache
}

// ApplyExternalObjectMappingUpdate writes the externally resolved account-level
// placement into the in-memory PartitionMap. This is intentionally minimal and
// does not perform PBFT/network broadcast by itself.
func (bc *BlockChain) ApplyExternalObjectMappingUpdate(update *ExternalObjectMappingUpdate) int {
	if update == nil {
		return 0
	}
	applied := 0
	for _, addr := range update.Addresses {
		if !isValidExternalMappingAddress(addr) {
			continue
		}
		if bc.Get_PartitionMap(addr) == update.TargetShard {
			continue
		}
		bc.Update_PartitionMap(addr, update.TargetShard)
		applied++
	}
	return applied
}

func isValidExternalMappingAddress(addr string) bool {
	trimmed := strings.TrimSpace(addr)
	if trimmed == "" {
		return false
	}
	lower := strings.ToLower(trimmed)
	if lower == "none" || lower == "null" || lower == "nan" {
		return false
	}
	return strings.HasPrefix(lower, "0x")
}

func parseMappingSafetyLag(raw string, defaultValue int) int {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(trimmed)
	if err != nil || value < 0 {
		return defaultValue
	}
	return value
}

func loadExternalObjectMappingUpdates(path string) ([]*ExternalObjectMappingUpdate, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	updates := make([]*ExternalObjectMappingUpdate, 0)
	scanner := bufio.NewScanner(file)
	buffer := make([]byte, 0, 64*1024)
	scanner.Buffer(buffer, 32*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		update := new(ExternalObjectMappingUpdate)
		if err := json.Unmarshal(line, update); err != nil {
			return nil, err
		}
		updates = append(updates, update)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return updates, nil
}

func (cache *externalObjectMappingUpdateCache) loadNew(path string) {
	if cache == nil || path == "" {
		return
	}
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return
	}
	if info.Size() < cache.offset {
		cache.offset = 0
		cache.pending = cache.pending[:0]
		cache.applied = make(map[string]bool)
		cache.appliedSequence = 0
		cache.rollingHash = [32]byte{}
	}
	if _, err := file.Seek(cache.offset, io.SeekStart); err != nil {
		return
	}
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			// A regular-file reader may observe a large JSONL append before the
			// writer has completed the trailing newline. Never advance the cache
			// offset for that partial record; retry it on the next commit boundary.
			if err == io.EOF && !strings.HasSuffix(line, "\n") {
				break
			}
			cache.offset += int64(len([]byte(line)))
			trimmed := strings.TrimSpace(line)
			if trimmed != "" {
				update := new(ExternalObjectMappingUpdate)
				if json.Unmarshal([]byte(trimmed), update) == nil {
					key := strings.TrimSpace(update.ProposalID)
					if key == "" {
						key = path + ":" + strconv.FormatInt(cache.offset, 10)
					}
					if !cache.applied[key] {
						cache.pending = append(cache.pending, &cachedExternalObjectMappingUpdate{
							key:    key,
							update: update,
						})
					}
				}
			}
		}
		if err != nil {
			break
		}
	}
}

func writeMappingVersionAudit(
	updatePath string,
	currentEpoch int,
	safetyLag int,
	key string,
	update *ExternalObjectMappingUpdate,
	appliedAccounts int,
	batchUpdateCount int,
	sequence uint64,
	rollingHash [32]byte,
) {
	auditDir := strings.TrimSpace(os.Getenv(mappingVersionAuditDirEnv))
	if auditDir == "" || update == nil {
		return
	}
	if err := os.MkdirAll(auditDir, 0o755); err != nil {
		return
	}
	record := map[string]interface{}{
		"recorded_at_utc": time.Now().UTC().Format(time.RFC3339Nano),
		"pid":             os.Getpid(),
		"current_epoch":   currentEpoch,
		"effective_epoch": update.EffectiveEpoch,
		"safety_lag":      safetyLag,
		"proposal_id":     key,
		"object_id":       update.ObjectID,
		"object_type":     update.ObjectType,
		"source_shard":    update.SourceShard,
		"target_shard":    update.TargetShard,
		// Mapping hooks move routing ownership only; account state is not transferred across tries.
		"state_migration_supported": false,
		"mapping_semantics":         "route_only",
		"applied_accounts":          appliedAccounts,
		"batch_update_count":        batchUpdateCount,
		"sequence":                  sequence,
		"rolling_hash":              fmt.Sprintf("%x", rollingHash),
		"update_path":               updatePath,
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return
	}
	path := filepath.Join(auditDir, fmt.Sprintf("mapping_version_pid_%d.jsonl", os.Getpid()))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.Write(append(payload, '\n'))
}
