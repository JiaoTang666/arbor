package main

import (
	"blockEmulator/chain"
	"blockEmulator/params"
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

type goValidationSummary struct {
	ResultCount        int `json:"result_count"`
	Applied            int `json:"applied"`
	DryRunApplied      int `json:"dry_run_applied"`
	Deferred           int `json:"deferred"`
	InvalidRecord      int `json:"invalid_record"`
	RejectedAgreement  int `json:"rejected_agreement"`
	StaleOrMismatch    int `json:"stale_or_mismatch"`
	VersionMismatch    int `json:"version_mismatch"`
	NoopValidated      int `json:"noop_validated"`
	MappingUpdateCount int `json:"mapping_update_count"`
	SeededAccounts     int `json:"seeded_accounts"`
}

func main() {
	workspaceRoot := flag.String("workspace-root", "", "Optional workspace root for relative path resolution.")
	exchangeRoot := flag.String("exchange-root", "", "Directory containing proposal_txs/onchain_validation_records and Go audit outputs.")
	proposalTxPath := flag.String("proposal-tx-path", "", "Explicit proposal_txs.jsonl path.")
	validationRecordPath := flag.String("validation-record-path", "", "Explicit onchain_validation_records.jsonl path.")
	resultPath := flag.String("result-path", "", "Explicit go_validation_ingress_results.jsonl path.")
	mappingUpdatePath := flag.String("mapping-update-path", "", "Explicit go_validated_mapping_updates.jsonl path.")
	accountClusterMapPath := flag.String("account-cluster-map-path", "", "Path to baseline account_cluster_map.csv used for stable/micro resolution.")
	expectedPolicyVersion := flag.String("expected-policy-version", "", "Optional exact policy version required by Go ingress.")
	expectedStateVersionPrefix := flag.String("expected-state-version-prefix", "", "Optional state_version prefix required by Go ingress.")
	seedStableSourceMapping := flag.Bool("seed-stable-source-mapping", false, "Seed stable/micro source-shard mappings before replay. Demo-only.")
	startEpoch := flag.Int("start-epoch", 1, "Starting epoch for replay.")
	epochs := flag.Int("epochs", 5, "How many epochs to replay.")
	flag.Parse()

	resolvedWorkspaceRoot := resolveExistingRoot(*workspaceRoot, ".")
	resolvedExchangeRoot := resolvePath(resolvedWorkspaceRoot, *exchangeRoot, filepath.Join("drl_scheduler", "artifacts", "signed_onchain_validation_demo"))
	resolvedProposalTxPath := resolvePath(resolvedWorkspaceRoot, *proposalTxPath, filepath.Join(resolvedExchangeRoot, "proposal_txs.jsonl"))
	resolvedValidationRecordPath := resolvePath(resolvedWorkspaceRoot, *validationRecordPath, filepath.Join(resolvedExchangeRoot, "onchain_validation_records.jsonl"))
	resolvedResultPath := resolvePath(resolvedWorkspaceRoot, *resultPath, filepath.Join(resolvedExchangeRoot, "go_validation_ingress_results.jsonl"))
	resolvedMappingUpdatePath := resolvePath(resolvedWorkspaceRoot, *mappingUpdatePath, filepath.Join(resolvedExchangeRoot, "go_validated_mapping_updates.jsonl"))
	resolvedAccountClusterMapPath := resolvePath(
		resolvedWorkspaceRoot,
		*accountClusterMapPath,
		filepath.Join("社区簇", "output", "account_cluster_map.csv"),
	)

	for _, path := range []string{resolvedResultPath, resolvedMappingUpdatePath} {
		if path == "" {
			continue
		}
		_ = os.Remove(path)
		_ = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	}

	artifactDemoRoot := filepath.Join(resolvedExchangeRoot, "go_validation_demo")
	_ = os.RemoveAll(artifactDemoRoot)
	_ = os.MkdirAll(artifactDemoRoot, os.ModePerm)
	params.DatabaseWrite_path = ensureTrailingSeparator(filepath.Join(artifactDemoRoot, "database"))

	if resolvedProposalTxPath != "" {
		_ = os.Setenv("BLOCK_EMULATOR_PROPOSAL_TX_PATH", resolvedProposalTxPath)
	}
	if resolvedValidationRecordPath != "" {
		_ = os.Setenv("BLOCK_EMULATOR_ONCHAIN_VALIDATION_RECORD_PATH", resolvedValidationRecordPath)
	}
	if resolvedResultPath != "" {
		_ = os.Setenv("BLOCK_EMULATOR_GO_VALIDATION_RESULT_PATH", resolvedResultPath)
	}
	if resolvedMappingUpdatePath != "" {
		_ = os.Setenv("BLOCK_EMULATOR_GO_VALIDATED_MAPPING_UPDATE_PATH", resolvedMappingUpdatePath)
	}
	if resolvedAccountClusterMapPath != "" {
		_ = os.Setenv("BLOCK_EMULATOR_ACCOUNT_CLUSTER_MAP_PATH", resolvedAccountClusterMapPath)
	}
	if *expectedPolicyVersion != "" {
		_ = os.Setenv("BLOCK_EMULATOR_EXPECTED_POLICY_VERSION", *expectedPolicyVersion)
	}
	if *expectedStateVersionPrefix != "" {
		_ = os.Setenv("BLOCK_EMULATOR_EXPECTED_STATE_VERSION_PREFIX", *expectedStateVersionPrefix)
	}

	cc := &params.ChainConfig{
		ChainID:        1,
		NodeID:         0,
		ShardID:        0,
		Nodes_perShard: 1,
		ShardNums:      8,
		BlockSize:      1,
		BlockInterval:  1,
		InjectSpeed:    1,
	}
	bc, err := chain.NewBlockChain(cc, rawdb.NewMemoryDatabase())
	if err != nil {
		panic(err)
	}
	defer bc.CloseBlockChain()

	seededAccounts := 0
	if *seedStableSourceMapping {
		seededAccounts, err = seedIngressSourceMappings(
			bc,
			resolvedValidationRecordPath,
			resolvedProposalTxPath,
			resolvedAccountClusterMapPath,
		)
		if err != nil {
			panic(err)
		}
	}

	for currentEpoch := *startEpoch; currentEpoch < *startEpoch+*epochs; currentEpoch++ {
		bc.ApplyExternalProposalIngressIfDue(currentEpoch)
	}

	summary, err := summarizeValidationResults(resolvedResultPath, resolvedMappingUpdatePath)
	if err != nil {
		panic(err)
	}
	summary.SeededAccounts = seededAccounts
	output, err := json.Marshal(summary)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(output))
}

func seedIngressSourceMappings(
	bc *chain.BlockChain,
	validationRecordPath string,
	proposalTxPath string,
	accountClusterMapPath string,
) (int, error) {
	recordPath := validationRecordPath
	if strings.TrimSpace(recordPath) == "" {
		recordPath = proposalTxPath
	}
	if strings.TrimSpace(recordPath) == "" || strings.TrimSpace(accountClusterMapPath) == "" {
		return 0, nil
	}
	records, err := loadIngressRecords(recordPath)
	if err != nil {
		return 0, err
	}
	accountCache, err := loadStableAccountCache(accountClusterMapPath)
	if err != nil {
		return 0, err
	}
	seeded := 0
	for _, record := range records {
		if record == nil || strings.ToUpper(record.AgreementStatus) != "AGREED" || record.ObjectID == "" {
			continue
		}
		if record.ActionType != "migrate_stable" && record.ActionType != "migrate_micro" {
			continue
		}
		if record.ObjectType != "stable" && record.ObjectType != "micro" {
			continue
		}
		addresses := accountCache[record.ObjectID]
		if record.ObjectType == "micro" {
			resolvedAccounts, resolveErr := chain.ResolveIngressObjectAccountsForDemo(
				accountClusterMapPath,
				record.ObjectType,
				record.ObjectID,
				record.EpochID,
			)
			if resolveErr == nil && len(resolvedAccounts) > 0 {
				addresses = resolvedAccounts
			}
		}
		for _, address := range addresses {
			bc.Update_PartitionMap(address, uint64(record.SourceShard))
			seeded++
		}
	}
	return seeded, nil
}

func resolveExistingRoot(explicitRoot string, fallback string) string {
	candidate := strings.TrimSpace(explicitRoot)
	if candidate == "" {
		root, err := os.Getwd()
		if err != nil {
			absFallback, absErr := filepath.Abs(fallback)
			if absErr != nil {
				return fallback
			}
			return absFallback
		}
		absRoot, err := filepath.Abs(root)
		if err != nil {
			return root
		}
		return absRoot
	}
	absRoot, err := filepath.Abs(candidate)
	if err != nil {
		return candidate
	}
	return absRoot
}

func resolvePath(root string, explicitValue string, fallback string) string {
	base := strings.TrimSpace(explicitValue)
	if base == "" {
		base = fallback
	}
	if filepath.IsAbs(base) {
		return filepath.Clean(base)
	}
	return filepath.Clean(filepath.Join(root, base))
}

func ensureTrailingSeparator(path string) string {
	if strings.HasSuffix(path, string(os.PathSeparator)) {
		return path
	}
	return path + string(os.PathSeparator)
}

func loadIngressRecords(path string) ([]*chain.ExternalProposalIngressRecord, error) {
	handle, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer handle.Close()

	records := make([]*chain.ExternalProposalIngressRecord, 0)
	scanner := bufio.NewScanner(handle)
	scanner.Buffer(make([]byte, 1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		record := new(chain.ExternalProposalIngressRecord)
		if err := json.Unmarshal([]byte(line), record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, scanner.Err()
}

func loadStableAccountCache(path string) (map[string][]string, error) {
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
	accountIndex := -1
	clusterIndex := -1
	for index, column := range header {
		switch strings.TrimSpace(column) {
		case "account":
			accountIndex = index
		case "cluster_id":
			clusterIndex = index
		}
	}
	if accountIndex < 0 || clusterIndex < 0 {
		return map[string][]string{}, nil
	}

	cache := make(map[string][]string)
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		account := strings.TrimSpace(row[accountIndex])
		clusterID := strings.TrimSpace(row[clusterIndex])
		if account == "" || clusterID == "" {
			continue
		}
		normalized := clusterID
		if strings.Contains(normalized, ".") {
			parts := strings.SplitN(normalized, ".", 2)
			normalized = parts[0]
		}
		objectID := "stable:" + normalized
		cache[objectID] = append(cache[objectID], account)
	}
	return cache, nil
}

func summarizeValidationResults(resultPath string, mappingUpdatePath string) (*goValidationSummary, error) {
	summary := &goValidationSummary{}
	if err := scanJSONL(resultPath, func(record map[string]interface{}) {
		summary.ResultCount++
		status, _ := record["validation_status"].(string)
		switch status {
		case "DEFERRED":
			summary.Deferred++
		case "INVALID_RECORD":
			summary.InvalidRecord++
		case "REJECTED_AGREEMENT":
			summary.RejectedAgreement++
		case "STALE_OR_MISMATCH":
			summary.StaleOrMismatch++
		case "VERSION_MISMATCH":
			summary.VersionMismatch++
		case "NOOP_VALIDATED":
			summary.NoopValidated++
		}
		applyStatus, _ := record["apply_status"].(string)
		dryRun, _ := record["dry_run"].(bool)
		if applyStatus == "APPLIED" {
			summary.Applied++
			if dryRun {
				summary.DryRunApplied++
			}
		}
	}); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err := scanJSONL(mappingUpdatePath, func(_ map[string]interface{}) {
		summary.MappingUpdateCount++
	}); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return summary, nil
}

func scanJSONL(path string, fn func(map[string]interface{})) error {
	handle, err := os.Open(path)
	if err != nil {
		return err
	}
	defer handle.Close()

	scanner := bufio.NewScanner(handle)
	scanner.Buffer(make([]byte, 1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		record := make(map[string]interface{})
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return err
		}
		fn(record)
	}
	return scanner.Err()
}
