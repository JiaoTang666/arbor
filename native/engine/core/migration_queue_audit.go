package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type MigrationQueueAudit struct {
	Boundary MigrationDrainBoundary
	Pending  int
}

// Read-only observation of published generations, not an ingress barrier.
// A writer may publish the next generation after this function returns.
func AuditMigrationQueue(directory, account string) (MigrationQueueAudit, error) {
	var result MigrationQueueAudit
	entries, err := os.ReadDir(directory)
	if err != nil {
		return result, err
	}
	account = canonicalPrepareAccount(account)
	var pending []*Transaction
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".queue.json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return result, err
		}
		var snapshot migrationQueueSnapshot
		if err = json.Unmarshal(raw, &snapshot); err != nil {
			return result, err
		}
		if snapshot.Account != account || len(snapshot.Accounts) != 0 || snapshot.Sequence != result.Boundary.Generation+1 || entry.Name() != fmt.Sprintf("%020d.queue.json", snapshot.Sequence) {
			return result, fmt.Errorf("queue audit generation or identity mismatch")
		}
		pending, err = replayMigrationGeneration(pending, snapshot, directory)
		if err != nil {
			return result, err
		}
		hash := sha256.Sum256(raw)
		result = MigrationQueueAudit{MigrationDrainBoundary{account, snapshot.Sequence, hex.EncodeToString(hash[:])}, len(pending)}
	}
	if result.Boundary.Generation == 0 {
		return result, fmt.Errorf("queue audit has no durable snapshot")
	}
	return result, nil
}
