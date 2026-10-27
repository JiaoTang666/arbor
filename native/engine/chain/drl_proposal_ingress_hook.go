package chain

import (
	"bufio"
	"bytes"
	"crypto/sha256"
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
	"sync"
	"syscall"
	"time"
)

const (
	externalProposalTxPathEnv                = "BLOCK_EMULATOR_PROPOSAL_TX_PATH"
	externalValidationRecordPathEnv          = "BLOCK_EMULATOR_ONCHAIN_VALIDATION_RECORD_PATH"
	externalGoValidationIngressResultPathEnv = "BLOCK_EMULATOR_GO_VALIDATION_RESULT_PATH"
	externalGoValidatedMappingUpdatePathEnv  = "BLOCK_EMULATOR_GO_VALIDATED_MAPPING_UPDATE_PATH"
	externalAccountClusterMapPathEnv         = "BLOCK_EMULATOR_ACCOUNT_CLUSTER_MAP_PATH"
	externalExpectedPolicyVersionEnv         = "BLOCK_EMULATOR_EXPECTED_POLICY_VERSION"
	externalExpectedStateVersionPrefixEnv    = "BLOCK_EMULATOR_EXPECTED_STATE_VERSION_PREFIX"
	externalSyncJSONLWritesEnv               = "BLOCK_EMULATOR_SYNC_JSONL_WRITES"
)

type ExternalProposalIngressRecord struct {
	TxID                       string            `json:"tx_id"`
	ProposalID                 string            `json:"proposal_id"`
	EpochID                    int               `json:"epoch_id"`
	EffectiveEpoch             int               `json:"effective_epoch"`
	ProposerSetDigest          string            `json:"proposer_set_digest"`
	AgreementStatus            string            `json:"agreement_status"`
	StateDigest                string            `json:"state_digest"`
	MaskDigest                 string            `json:"mask_digest"`
	ActionType                 string            `json:"action_type"`
	ObjectType                 string            `json:"object_type"`
	ObjectID                   string            `json:"object_id"`
	SourceShard                int               `json:"source_shard"`
	TargetShard                int               `json:"target_shard"`
	PolicyVersion              string            `json:"policy_version"`
	StateVersion               string            `json:"state_version"`
	PayloadDigest              string            `json:"payload_digest"`
	AggregatedValidationDigest string            `json:"aggregated_validation_digest"`
	CreatedAt                  string            `json:"created_at"`
	ProposerIDs                []string          `json:"proposer_ids"`
	ProposalIDs                map[string]string `json:"proposal_ids"`
	AgreedCount                int               `json:"agreed_count"`
	QuorumSize                 int               `json:"quorum_size"`
	IntentValidationStatus     string            `json:"intent_validation_status"`
	IntentValidationReason     string            `json:"intent_validation_reason"`
}

type InternalValidatedCommand struct {
	TxID             string
	ProposalID       string
	EpochID          int
	EffectiveEpoch   int
	ActionType       string
	ObjectType       string
	ObjectID         string
	SourceShard      int
	TargetShard      int
	PolicyVersion    string
	StateVersion     string
	ValidationStatus string
	ValidationReason string
}

type GoValidationIngressResult struct {
	TxID                               string `json:"tx_id"`
	ProposalID                         string `json:"proposal_id"`
	EpochID                            int    `json:"epoch_id"`
	EffectiveEpoch                     int    `json:"effective_epoch"`
	AgreementStatus                    string `json:"agreement_status"`
	ActionType                         string `json:"action_type"`
	ObjectType                         string `json:"object_type"`
	ObjectID                           string `json:"object_id"`
	SourceShard                        int    `json:"source_shard"`
	TargetShard                        int    `json:"target_shard"`
	StateDigest                        string `json:"state_digest"`
	MaskDigest                         string `json:"mask_digest"`
	PolicyVersion                      string `json:"policy_version"`
	StateVersion                       string `json:"state_version"`
	PayloadDigest                      string `json:"payload_digest"`
	ObservedPayloadDigest              string `json:"observed_payload_digest"`
	AggregatedValidationDigest         string `json:"aggregated_validation_digest"`
	ObservedAggregatedValidationDigest string `json:"observed_aggregated_validation_digest"`
	ValidationStatus                   string `json:"validation_status"`
	ValidationReason                   string `json:"validation_reason,omitempty"`
	ApplyStatus                        string `json:"apply_status"`
	ApplyReason                        string `json:"apply_reason,omitempty"`
	ObservedCurrentShard               int    `json:"observed_current_shard"`
	ResolvedAccountCount               int    `json:"resolved_account_count"`
	EmittedInternalCommand             bool   `json:"emitted_internal_command"`
	MappingUpdateWritten               bool   `json:"mapping_update_written"`
	DryRun                             bool   `json:"dry_run"`
	CurrentEpoch                       int    `json:"current_epoch"`
	CreatedAt                          string `json:"created_at"`
}

var (
	stableAccountCacheLock sync.Mutex
	stableAccountCache     = make(map[string]map[string][]string)
	stableAccountStatsLock sync.Mutex
	stableAccountStats     = make(map[string]map[string]map[string]float64)
	microAccountCacheLock  sync.Mutex
	microAccountCache      = make(map[string][]string)
)

// ApplyExternalProposalIngressIfDue is a minimal Go-side ingress hook that
// consumes proposal tx / validation-record JSONL, validates them at epoch
// boundaries, and forwards stable account-level updates into the existing
// PartitionMap hook path. It is intentionally weakly coupled and file based.
func (bc *BlockChain) ApplyExternalProposalIngressIfDue(currentEpoch int) int {
	inputPath := resolveExternalProposalIngressInputPath()
	if inputPath == "" {
		return 0
	}
	records, err := loadExternalProposalIngressRecords(inputPath)
	if err != nil {
		return 0
	}
	resultPath := resolveProposalIngressOutputPath(inputPath, externalGoValidationIngressResultPathEnv, "go_validation_ingress_results.jsonl")
	mappingUpdatePath := resolveProposalIngressOutputPath(inputPath, externalGoValidatedMappingUpdatePathEnv, "go_validated_mapping_updates.jsonl")
	accountClusterMapPath := os.Getenv(externalAccountClusterMapPathEnv)
	expectedPolicyVersion := os.Getenv(externalExpectedPolicyVersionEnv)
	expectedStateVersionPrefix := os.Getenv(externalExpectedStateVersionPrefixEnv)

	appliedAccounts := 0
	for _, record := range records {
		if record == nil {
			continue
		}
		recordKey := record.TxID
		if recordKey == "" {
			recordKey = record.ProposalID
		}
		if recordKey != "" && bc.isProposalIngressProcessed(recordKey) {
			continue
		}
		result, command := bc.validateProposalIngressRecord(
			record,
			currentEpoch,
			expectedPolicyVersion,
			expectedStateVersionPrefix,
		)
		if command != nil {
			appliedAccounts += bc.applyValidatedProposalCommand(command, &result, accountClusterMapPath, mappingUpdatePath)
		}
		appendJSONLRecord(resultPath, result)
		if result.ValidationStatus != "DEFERRED" && recordKey != "" {
			bc.markProposalIngressProcessed(recordKey)
		}
	}
	return appliedAccounts
}

func (bc *BlockChain) validateProposalIngressRecord(
	record *ExternalProposalIngressRecord,
	currentEpoch int,
	expectedPolicyVersion string,
	expectedStateVersionPrefix string,
) (GoValidationIngressResult, *InternalValidatedCommand) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	observedPayloadDigest := computeObservedPayloadDigest(record)
	observedAggregatedDigest, aggregateVerified := computeObservedAggregatedValidationDigest(record, observedPayloadDigest)

	result := GoValidationIngressResult{
		TxID:                               record.TxID,
		ProposalID:                         record.ProposalID,
		EpochID:                            record.EpochID,
		EffectiveEpoch:                     record.EffectiveEpoch,
		AgreementStatus:                    record.AgreementStatus,
		ActionType:                         record.ActionType,
		ObjectType:                         record.ObjectType,
		ObjectID:                           record.ObjectID,
		SourceShard:                        record.SourceShard,
		TargetShard:                        record.TargetShard,
		StateDigest:                        record.StateDigest,
		MaskDigest:                         record.MaskDigest,
		PolicyVersion:                      record.PolicyVersion,
		StateVersion:                       record.StateVersion,
		PayloadDigest:                      record.PayloadDigest,
		ObservedPayloadDigest:              observedPayloadDigest,
		AggregatedValidationDigest:         record.AggregatedValidationDigest,
		ObservedAggregatedValidationDigest: observedAggregatedDigest,
		ValidationStatus:                   "INVALID_RECORD",
		ApplyStatus:                        "SKIPPED",
		ObservedCurrentShard:               -1,
		CurrentEpoch:                       currentEpoch,
		CreatedAt:                          now,
	}

	if strings.ToUpper(record.AgreementStatus) != "AGREED" {
		result.ValidationStatus = "REJECTED_AGREEMENT"
		result.ValidationReason = "agreement_status_not_agreed"
		result.ApplyReason = "agreement_not_accepted"
		return result, nil
	}
	if record.TxID == "" || record.ProposalID == "" || record.EpochID < 0 || record.EffectiveEpoch < 0 || record.PayloadDigest == "" {
		result.ValidationStatus = "INVALID_RECORD"
		result.ValidationReason = "missing_required_core_fields"
		result.ApplyReason = "missing_required_core_fields"
		return result, nil
	}
	if record.ProposerSetDigest == "" || record.AggregatedValidationDigest == "" {
		result.ValidationStatus = "INVALID_RECORD"
		result.ValidationReason = "missing_validation_digest"
		result.ApplyReason = "missing_validation_digest"
		return result, nil
	}
	if observedPayloadDigest != record.PayloadDigest {
		result.ValidationStatus = "INVALID_RECORD"
		result.ValidationReason = "payload_digest_mismatch"
		result.ApplyReason = "payload_digest_mismatch"
		return result, nil
	}
	if aggregateVerified && observedAggregatedDigest != record.AggregatedValidationDigest {
		result.ValidationStatus = "INVALID_RECORD"
		result.ValidationReason = "aggregated_validation_digest_mismatch"
		result.ApplyReason = "aggregated_validation_digest_mismatch"
		return result, nil
	}
	if expectedPolicyVersion != "" && record.PolicyVersion != expectedPolicyVersion {
		result.ValidationStatus = "VERSION_MISMATCH"
		result.ValidationReason = "policy_version_mismatch"
		result.ApplyReason = "policy_version_mismatch"
		return result, nil
	}
	if expectedStateVersionPrefix != "" && !strings.HasPrefix(record.StateVersion, expectedStateVersionPrefix) {
		result.ValidationStatus = "VERSION_MISMATCH"
		result.ValidationReason = "state_version_prefix_mismatch"
		result.ApplyReason = "state_version_prefix_mismatch"
		return result, nil
	}
	if record.EffectiveEpoch > currentEpoch {
		result.ValidationStatus = "DEFERRED"
		result.ValidationReason = "effective_epoch_not_reached"
		result.ApplyStatus = "DEFERRED"
		result.ApplyReason = "effective_epoch_not_reached"
		return result, nil
	}
	if record.ActionType == "noop" || record.ObjectType == "none" {
		result.ValidationStatus = "NOOP_VALIDATED"
		result.ValidationReason = "agreed_noop_no_apply"
		result.ApplyReason = "noop_no_apply"
		return result, nil
	}
	if record.ObjectID == "" || record.SourceShard < 0 || record.TargetShard < 0 {
		result.ValidationStatus = "INVALID_RECORD"
		result.ValidationReason = "missing_object_or_shard_fields"
		result.ApplyReason = "missing_object_or_shard_fields"
		return result, nil
	}
	if record.ActionType != "migrate_stable" && record.ActionType != "migrate_micro" {
		result.ValidationStatus = "INVALID_RECORD"
		result.ValidationReason = "unsupported_action_type"
		result.ApplyReason = "unsupported_action_type"
		return result, nil
	}
	if record.ObjectType != "stable" && record.ObjectType != "micro" {
		result.ValidationStatus = "INVALID_RECORD"
		result.ValidationReason = "unsupported_object_type"
		result.ApplyReason = "unsupported_object_type"
		return result, nil
	}

	result.ValidationStatus = "VALIDATED_FOR_APPLY"
	result.EmittedInternalCommand = true
	command := &InternalValidatedCommand{
		TxID:             record.TxID,
		ProposalID:       record.ProposalID,
		EpochID:          record.EpochID,
		EffectiveEpoch:   record.EffectiveEpoch,
		ActionType:       record.ActionType,
		ObjectType:       record.ObjectType,
		ObjectID:         record.ObjectID,
		SourceShard:      record.SourceShard,
		TargetShard:      record.TargetShard,
		PolicyVersion:    record.PolicyVersion,
		StateVersion:     record.StateVersion,
		ValidationStatus: result.ValidationStatus,
	}
	return result, command
}

func (bc *BlockChain) applyValidatedProposalCommand(
	command *InternalValidatedCommand,
	result *GoValidationIngressResult,
	accountClusterMapPath string,
	mappingUpdatePath string,
) int {
	if command == nil || result == nil {
		return 0
	}

	switch command.ObjectType {
	case "micro":
		accounts, err := loadMicroAccounts(accountClusterMapPath, command.ObjectID, command.EpochID)
		if err != nil || len(accounts) == 0 {
			result.ValidationStatus = "STALE_OR_MISMATCH"
			result.ValidationReason = "micro_cluster_accounts_missing"
			result.ApplyStatus = "REJECTED"
			result.ApplyReason = "micro_cluster_accounts_missing"
			result.DryRun = true
			return 0
		}
		observedShard, consistent := bc.observeStableCurrentShard(accounts)
		result.ObservedCurrentShard = observedShard
		result.ResolvedAccountCount = len(accounts)
		if !consistent || observedShard != command.SourceShard {
			result.ValidationStatus = "STALE_OR_MISMATCH"
			result.ValidationReason = "source_shard_mismatch"
			result.ApplyStatus = "REJECTED"
			result.ApplyReason = "source_shard_mismatch"
			result.DryRun = true
			return 0
		}
		update := &ExternalObjectMappingUpdate{
			ProposalID:           command.ProposalID,
			EpochID:              command.EpochID,
			EffectiveEpoch:       command.EffectiveEpoch,
			ObjectType:           command.ObjectType,
			ObjectID:             command.ObjectID,
			SourceShard:          uint64(command.SourceShard),
			TargetShard:          uint64(command.TargetShard),
			Addresses:            accounts,
			ResolvedAccountCount: len(accounts),
			ApplyMode:            "go_validation_ingress_hook_micro",
		}
		appendJSONLRecord(mappingUpdatePath, update)
		applied := bc.ApplyExternalObjectMappingUpdate(update)
		result.ApplyStatus = "APPLIED"
		result.ApplyReason = "micro_partition_map_updated"
		result.MappingUpdateWritten = true
		result.DryRun = false
		return applied
	case "stable":
		accounts, err := loadStableAccounts(accountClusterMapPath, command.ObjectID)
		if err != nil || len(accounts) == 0 {
			result.ValidationStatus = "STALE_OR_MISMATCH"
			result.ValidationReason = "stable_cluster_accounts_missing"
			result.ApplyStatus = "REJECTED"
			result.ApplyReason = "stable_cluster_accounts_missing"
			result.DryRun = true
			return 0
		}
		observedShard, consistent := bc.observeStableCurrentShard(accounts)
		result.ObservedCurrentShard = observedShard
		result.ResolvedAccountCount = len(accounts)
		if !consistent || observedShard != command.SourceShard {
			result.ValidationStatus = "STALE_OR_MISMATCH"
			result.ValidationReason = "source_shard_mismatch"
			result.ApplyStatus = "REJECTED"
			result.ApplyReason = "source_shard_mismatch"
			result.DryRun = true
			return 0
		}
		update := &ExternalObjectMappingUpdate{
			ProposalID:           command.ProposalID,
			EpochID:              command.EpochID,
			EffectiveEpoch:       command.EffectiveEpoch,
			ObjectType:           command.ObjectType,
			ObjectID:             command.ObjectID,
			SourceShard:          uint64(command.SourceShard),
			TargetShard:          uint64(command.TargetShard),
			Addresses:            accounts,
			ResolvedAccountCount: len(accounts),
			ApplyMode:            "go_validation_ingress_hook",
		}
		appendJSONLRecord(mappingUpdatePath, update)
		applied := bc.ApplyExternalObjectMappingUpdate(update)
		result.ApplyStatus = "APPLIED"
		result.ApplyReason = "stable_partition_map_updated"
		result.MappingUpdateWritten = true
		result.DryRun = false
		return applied
	default:
		result.ValidationStatus = "INVALID_RECORD"
		result.ValidationReason = "unsupported_object_type"
		result.ApplyStatus = "REJECTED"
		result.ApplyReason = "unsupported_object_type"
		result.DryRun = true
		return 0
	}
}

func resolveExternalProposalIngressInputPath() string {
	for _, envKey := range []string{externalValidationRecordPathEnv, externalProposalTxPathEnv} {
		value := strings.TrimSpace(os.Getenv(envKey))
		if value == "" {
			continue
		}
		if _, err := os.Stat(value); err == nil {
			return value
		}
	}
	return ""
}

func resolveProposalIngressOutputPath(inputPath string, envKey string, defaultName string) string {
	if value := strings.TrimSpace(os.Getenv(envKey)); value != "" {
		return value
	}
	if inputPath == "" {
		return defaultName
	}
	return filepath.Join(filepath.Dir(inputPath), defaultName)
}

func loadExternalProposalIngressRecords(path string) ([]*ExternalProposalIngressRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	records := make([]*ExternalProposalIngressRecord, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		record := new(ExternalProposalIngressRecord)
		if err := json.Unmarshal(line, record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func appendJSONLRecord(path string, record interface{}) {
	if path == "" || record == nil {
		return
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return
	}
	line := append(payload, '\n')
	if err := os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
		return
	}
	handle, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer handle.Close()
	if err := syscall.Flock(int(handle.Fd()), syscall.LOCK_EX); err != nil {
		return
	}
	defer syscall.Flock(int(handle.Fd()), syscall.LOCK_UN)
	if _, err := handle.Write(line); err != nil {
		return
	}
	if parseSnapshotBool(os.Getenv(externalSyncJSONLWritesEnv), false) &&
		filepath.Clean(path) == filepath.Clean(os.Getenv(externalNativeSnapshotPathEnv)) {
		_ = handle.Sync()
	}
}

func canonicalJSONBytes(payload interface{}) ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(payload); err != nil {
		return nil, err
	}
	return bytes.TrimSpace(buffer.Bytes()), nil
}

func computeHexDigest(payload interface{}) string {
	raw, err := canonicalJSONBytes(payload)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func computeObservedPayloadDigest(record *ExternalProposalIngressRecord) string {
	proposerSetDigest := strings.TrimSpace(record.ProposerSetDigest)
	if proposerSetDigest == "" && len(record.ProposerIDs) > 0 {
		proposerSetDigest = buildProposerSetDigest(record.ProposerIDs)
	}
	payload := map[string]interface{}{
		"proposal_id":         record.ProposalID,
		"epoch_id":            record.EpochID,
		"effective_epoch":     record.EffectiveEpoch,
		"proposer_set_digest": proposerSetDigest,
		"agreement_status":    record.AgreementStatus,
		"state_digest":        emptyToNil(record.StateDigest),
		"mask_digest":         emptyToNil(record.MaskDigest),
		"action_type":         record.ActionType,
		"object_type":         record.ObjectType,
		"object_id":           emptyToNil(record.ObjectID),
		"source_shard":        record.SourceShard,
		"target_shard":        record.TargetShard,
		"policy_version":      record.PolicyVersion,
		"state_version":       record.StateVersion,
	}
	return computeHexDigest(payload)
}

func computeObservedAggregatedValidationDigest(record *ExternalProposalIngressRecord, payloadDigest string) (string, bool) {
	proposerSetDigest := strings.TrimSpace(record.ProposerSetDigest)
	if proposerSetDigest == "" && len(record.ProposerIDs) > 0 {
		proposerSetDigest = buildProposerSetDigest(record.ProposerIDs)
	}
	if proposerSetDigest == "" || payloadDigest == "" {
		return "", false
	}
	if record.AgreedCount == 0 && record.QuorumSize == 0 && len(record.ProposerIDs) == 0 {
		return record.AggregatedValidationDigest, false
	}
	aggregatePayload := map[string]interface{}{
		"proposal_id":         record.ProposalID,
		"agreement_status":    record.AgreementStatus,
		"proposer_set_digest": proposerSetDigest,
		"payload_digest":      payloadDigest,
		"state_digest":        emptyToNil(record.StateDigest),
		"mask_digest":         emptyToNil(record.MaskDigest),
		"policy_version":      record.PolicyVersion,
		"state_version":       record.StateVersion,
		"agreed_count":        record.AgreedCount,
		"quorum_size":         record.QuorumSize,
	}
	return computeHexDigest(aggregatePayload), true
}

func buildProposerSetDigest(proposerIDs []string) string {
	copyIDs := make([]string, 0, len(proposerIDs))
	for _, proposerID := range proposerIDs {
		if proposerID == "" {
			continue
		}
		copyIDs = append(copyIDs, proposerID)
	}
	sort.Strings(copyIDs)
	return computeHexDigest(map[string]interface{}{"proposer_ids": copyIDs})
}

func emptyToNil(value string) interface{} {
	if value == "" {
		return nil
	}
	return value
}

func loadStableAccounts(accountClusterMapPath string, objectID string) ([]string, error) {
	if accountClusterMapPath == "" || objectID == "" {
		return nil, fmt.Errorf("missing account_cluster_map path or object id")
	}
	stableAccountCacheLock.Lock()
	defer stableAccountCacheLock.Unlock()
	cache, ok := stableAccountCache[accountClusterMapPath]
	if !ok {
		loaded, err := buildStableAccountCache(accountClusterMapPath)
		if err != nil {
			return nil, err
		}
		cache = loaded
		stableAccountCache[accountClusterMapPath] = cache
	}
	accounts := append([]string(nil), cache[objectID]...)
	return accounts, nil
}

func buildStableAccountCache(path string) (map[string][]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return map[string][]string{}, nil
	}
	header := records[0]
	accountIdx := -1
	clusterIdx := -1
	for idx, column := range header {
		switch strings.TrimSpace(column) {
		case "account":
			accountIdx = idx
		case "cluster_id":
			clusterIdx = idx
		}
	}
	if accountIdx < 0 || clusterIdx < 0 {
		return nil, fmt.Errorf("account_cluster_map missing required columns")
	}
	cache := make(map[string][]string)
	for _, row := range records[1:] {
		if len(row) <= accountIdx || len(row) <= clusterIdx {
			continue
		}
		clusterID := strings.TrimSpace(row[clusterIdx])
		if clusterID == "" || clusterID == "-1" {
			continue
		}
		account := strings.TrimSpace(row[accountIdx])
		if account == "" {
			continue
		}
		objectID := "stable:" + clusterID
		cache[objectID] = append(cache[objectID], account)
	}
	for objectID, addresses := range cache {
		sort.Strings(addresses)
		cache[objectID] = uniqueStrings(addresses)
	}
	return cache, nil
}

func uniqueStrings(items []string) []string {
	if len(items) == 0 {
		return items
	}
	result := make([]string, 0, len(items))
	last := ""
	for idx, item := range items {
		if idx == 0 || item != last {
			result = append(result, item)
			last = item
		}
	}
	return result
}

type microFeatureRow struct {
	EpochID               int
	MicroClusterID        int
	ParentStableClusterID int
	ActiveAccountCount    int
}

func (bc *BlockChain) observeStableCurrentShard(addresses []string) (int, bool) {
	if len(addresses) == 0 {
		return -1, false
	}
	observed := int(bc.Get_PartitionMap(addresses[0]))
	for _, addr := range addresses[1:] {
		if int(bc.Get_PartitionMap(addr)) != observed {
			return observed, false
		}
	}
	return observed, true
}

func (bc *BlockChain) isProposalIngressProcessed(recordKey string) bool {
	bc.proposalIngressLock.Lock()
	defer bc.proposalIngressLock.Unlock()
	return bc.processedProposalIngress[recordKey]
}

func (bc *BlockChain) markProposalIngressProcessed(recordKey string) {
	bc.proposalIngressLock.Lock()
	defer bc.proposalIngressLock.Unlock()
	bc.processedProposalIngress[recordKey] = true
}

func parseOptionalInt(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return value
}

func loadMicroAccounts(accountClusterMapPath string, objectID string, commandEpoch int) ([]string, error) {
	if accountClusterMapPath == "" || objectID == "" {
		return nil, fmt.Errorf("missing account_cluster_map path or micro object id")
	}
	cacheKey := fmt.Sprintf("%s|%s|%d", accountClusterMapPath, objectID, commandEpoch)
	microAccountCacheLock.Lock()
	if cached, ok := microAccountCache[cacheKey]; ok {
		defer microAccountCacheLock.Unlock()
		return append([]string(nil), cached...), nil
	}
	microAccountCacheLock.Unlock()

	epochID, microClusterID := parseMicroObjectID(objectID)
	if epochID < 0 || microClusterID < 0 {
		return nil, fmt.Errorf("invalid micro object id: %s", objectID)
	}
	dataRoot := filepath.Dir(accountClusterMapPath)
	featureRow, err := loadMicroFeatureRow(filepath.Join(dataRoot, "micro_cluster_features.csv"), epochID, microClusterID)
	if err != nil {
		return nil, err
	}
	if featureRow == nil || featureRow.ParentStableClusterID < 0 {
		return nil, fmt.Errorf("missing parent stable cluster for %s", objectID)
	}
	stableObjectID := fmt.Sprintf("stable:%d", featureRow.ParentStableClusterID)
	stableAccounts, err := loadStableAccounts(accountClusterMapPath, stableObjectID)
	if err != nil || len(stableAccounts) == 0 {
		return nil, fmt.Errorf("stable parent accounts missing for %s", stableObjectID)
	}
	targetCount := featureRow.ActiveAccountCount
	if targetCount <= 0 {
		targetCount = 1
	}
	stableSet := make(map[string]struct{}, len(stableAccounts))
	for _, account := range stableAccounts {
		stableSet[account] = struct{}{}
	}
	accountStats, err := loadStableAccountStats(accountClusterMapPath, stableObjectID)
	if err != nil {
		accountStats = map[string]float64{}
	}
	scoredAccounts := scoreStableAccountsByTransactions(
		filepath.Join(dataRoot, "cleaned_transactions.csv"),
		commandEpoch,
		featureRow.ParentStableClusterID,
		stableSet,
	)
	ranked := append([]string(nil), stableAccounts...)
	sort.Slice(ranked, func(i, j int) bool {
		left := ranked[i]
		right := ranked[j]
		leftScore := scoredAccounts[left]
		rightScore := scoredAccounts[right]
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		leftStat := accountStats[left]
		rightStat := accountStats[right]
		if leftStat != rightStat {
			return leftStat > rightStat
		}
		return left < right
	})
	if targetCount > len(ranked) {
		targetCount = len(ranked)
	}
	selected := append([]string(nil), ranked[:targetCount]...)
	microAccountCacheLock.Lock()
	microAccountCache[cacheKey] = selected
	microAccountCacheLock.Unlock()
	return append([]string(nil), selected...), nil
}

func loadMicroFeatureRow(path string, epochID int, microClusterID int) (*microFeatureRow, error) {
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
		if parseOptionalInt(row[indexByName["micro_cluster_id"]]) != microClusterID {
			continue
		}
		return &microFeatureRow{
			EpochID:               epochID,
			MicroClusterID:        microClusterID,
			ParentStableClusterID: parseOptionalInt(row[indexByName["parent_stable_cluster_id"]]),
			ActiveAccountCount:    parseOptionalInt(row[indexByName["active_account_count"]]),
		}, nil
	}
	return nil, nil
}

func loadStableAccountStats(accountClusterMapPath string, stableObjectID string) (map[string]float64, error) {
	if accountClusterMapPath == "" || stableObjectID == "" {
		return nil, fmt.Errorf("missing account_cluster_map path or stable object id")
	}
	stableAccountStatsLock.Lock()
	defer stableAccountStatsLock.Unlock()
	cache, ok := stableAccountStats[accountClusterMapPath]
	if !ok {
		loaded, err := buildStableAccountStats(accountClusterMapPath)
		if err != nil {
			return nil, err
		}
		cache = loaded
		stableAccountStats[accountClusterMapPath] = cache
	}
	stats := cache[stableObjectID]
	if stats == nil {
		return map[string]float64{}, nil
	}
	result := make(map[string]float64, len(stats))
	for account, value := range stats {
		result[account] = value
	}
	return result, nil
}

func buildStableAccountStats(path string) (map[string]map[string]float64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return map[string]map[string]float64{}, nil
	}
	header := records[0]
	accountIdx := -1
	clusterIdx := -1
	accountInIdx := -1
	accountOutIdx := -1
	accountTotalIdx := -1
	for idx, column := range header {
		switch strings.TrimSpace(column) {
		case "account":
			accountIdx = idx
		case "cluster_id":
			clusterIdx = idx
		case "account_in_tx_count":
			accountInIdx = idx
		case "account_out_tx_count":
			accountOutIdx = idx
		case "account_total_tx_count":
			accountTotalIdx = idx
		}
	}
	if accountIdx < 0 || clusterIdx < 0 {
		return nil, fmt.Errorf("account_cluster_map missing required columns")
	}
	stats := make(map[string]map[string]float64)
	for _, row := range records[1:] {
		if len(row) <= accountIdx || len(row) <= clusterIdx {
			continue
		}
		clusterID := strings.TrimSpace(row[clusterIdx])
		if clusterID == "" || clusterID == "-1" {
			continue
		}
		account := strings.TrimSpace(row[accountIdx])
		if account == "" {
			continue
		}
		total := float64(0)
		if accountTotalIdx >= 0 && len(row) > accountTotalIdx {
			total = parseOptionalFloat(row[accountTotalIdx])
		}
		sumIO := float64(0)
		if accountInIdx >= 0 && len(row) > accountInIdx {
			sumIO += parseOptionalFloat(row[accountInIdx])
		}
		if accountOutIdx >= 0 && len(row) > accountOutIdx {
			sumIO += parseOptionalFloat(row[accountOutIdx])
		}
		score := total
		if sumIO > score {
			score = sumIO
		}
		objectID := "stable:" + clusterID
		if _, ok := stats[objectID]; !ok {
			stats[objectID] = make(map[string]float64)
		}
		stats[objectID][account] = score
	}
	return stats, nil
}

func scoreStableAccountsByTransactions(
	cleanedTransactionsPath string,
	commandEpoch int,
	parentStableClusterID int,
	stableAccounts map[string]struct{},
) map[string]float64 {
	scored := make(map[string]float64)
	handle, err := os.Open(cleanedTransactionsPath)
	if err != nil {
		return scored
	}
	defer handle.Close()

	reader := csv.NewReader(handle)
	header, err := reader.Read()
	if err != nil {
		return scored
	}
	indexByName := make(map[string]int, len(header))
	for idx, column := range header {
		indexByName[strings.TrimSpace(column)] = idx
	}
	required := []string{"epoch_id", "from", "to", "from_cluster_id", "to_cluster_id"}
	for _, name := range required {
		if _, ok := indexByName[name]; !ok {
			return scored
		}
	}
	for {
		row, readErr := reader.Read()
		if readErr != nil {
			break
		}
		if parseOptionalInt(row[indexByName["epoch_id"]]) != commandEpoch {
			continue
		}
		fromClusterID := parseOptionalInt(row[indexByName["from_cluster_id"]])
		toClusterID := parseOptionalInt(row[indexByName["to_cluster_id"]])
		if fromClusterID != parentStableClusterID && toClusterID != parentStableClusterID {
			continue
		}
		sender := strings.TrimSpace(row[indexByName["from"]])
		recipient := strings.TrimSpace(row[indexByName["to"]])
		if _, ok := stableAccounts[sender]; ok {
			scored[sender] += 2.0
		}
		if _, ok := stableAccounts[recipient]; ok {
			scored[recipient] += 1.5
		}
	}
	return scored
}

func parseMicroObjectID(objectID string) (int, int) {
	text := strings.TrimSpace(objectID)
	if !strings.HasPrefix(text, "micro:") {
		return -1, -1
	}
	parts := strings.Split(text, ":")
	if len(parts) != 3 {
		return -1, -1
	}
	return parseOptionalInt(parts[1]), parseOptionalInt(parts[2])
}

func parseOptionalFloat(raw string) float64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0
	}
	return value
}

// ResolveIngressObjectAccountsForDemo exposes the same object-to-account
// resolution path used by the Go ingress hook so the standalone demo command
// can seed source-shard mappings before replay.
func ResolveIngressObjectAccountsForDemo(accountClusterMapPath string, objectType string, objectID string, epochID int) ([]string, error) {
	switch strings.TrimSpace(objectType) {
	case "stable":
		return loadStableAccounts(accountClusterMapPath, objectID)
	case "micro":
		return loadMicroAccounts(accountClusterMapPath, objectID, epochID)
	default:
		return nil, fmt.Errorf("unsupported object type: %s", objectType)
	}
}
