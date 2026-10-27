package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type MigrationDrainBoundary struct {
	Account      string `json:"account"`
	Generation   uint64 `json:"generation"`
	SnapshotHash string `json:"snapshot_hash"`
}

// Records only the currently drained generation. Future arrivals remain
// fenced and journaled; this is never permission to delete forwarding state.
func (p *TxPool) MigrationDrainBoundary(account string) (MigrationDrainBoundary, error) {
	p.journalGate.Lock()
	defer p.journalGate.Unlock()
	p.lock.Lock()
	var b MigrationDrainBoundary
	j := p.migrationJournal
	if j == nil || j.account != canonicalPrepareAccount(account) || !p.heldMigrationAccounts[j.account] {
		p.lock.Unlock()
		return b, fmt.Errorf("durable fenced migration queue required")
	}
	if j.pendingRaw != nil {
		p.lock.Unlock()
		return b, fmt.Errorf("migration publication outcome uncertain; recovery required")
	}
	for _, tx := range p.migrationPendingLocked() {
		if TransactionTouchesAccount(tx, j.account) {
			p.lock.Unlock()
			return b, fmt.Errorf("migration queue not drained")
		}
	}
	p.lock.Unlock()
	raw, err := os.ReadFile(filepath.Join(j.directory, fmt.Sprintf("%020d.queue.json", j.sequence)))
	if err != nil {
		return b, err
	}
	var s migrationQueueSnapshot
	if err = json.Unmarshal(raw, &s); err != nil {
		return b, err
	}
	if err = rejectForeignConfirmationFields(s); err != nil {
		return b, err
	}
	if s.Kind == "source-credit-remove" {
		if s.Account != j.account || s.Sequence != j.sequence {
			return b, fmt.Errorf("source credit removal identity mismatch")
		}
		if err = ValidateSourceCreditRemovalReceipt(j.directory, s); err != nil {
			return b, err
		}
	}
	if s.Kind == confirmRemoveKind {
		if _, err = validateConfirmGeneration(s); err != nil {
			return b, err
		}
	}
	if s.Account == j.account && s.Sequence == j.sequence && ((s.Kind == "remove" || s.Kind == "source-credit-remove") && len(s.Transactions) > 0 || s.Kind == confirmRemoveKind) {
		if err = p.persistMigrationQueueLocked(nil); err != nil {
			return b, err
		}
		raw, err = os.ReadFile(filepath.Join(j.directory, fmt.Sprintf("%020d.queue.json", j.sequence)))
		if err != nil {
			return b, err
		}
		s = migrationQueueSnapshot{}
		if err = json.Unmarshal(raw, &s); err != nil {
			return b, err
		}
	}
	if s.Account != j.account || s.Sequence != j.sequence || len(s.Transactions) != 0 {
		return b, fmt.Errorf("durable queue is not drained")
	}
	digest := sha256.Sum256(raw)
	return MigrationDrainBoundary{j.account, j.sequence, hex.EncodeToString(digest[:])}, nil
}
