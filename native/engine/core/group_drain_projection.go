package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// The union remains the durable recovery object. A retirement certificate is
// an empty projection for ONE group, not an empty replacement of the union.
// Its hash identifies the local full generation it was derived from; it is not
// a Byzantine execution certificate. journalGate is held by the caller.
// This helper consumes the pool lock, including on errors.
func (p *TxPool) serialGroupDrainSnapshotLocked(accounts []string, timing GroupDrainTiming) ([]byte, GroupDrainTiming, error) {
	j := p.migrationJournal
	if j.account != "" || j.sequence == 0 || !canonicalJournalMembers(accounts, MinMigrationGroupMembers, MaxMigrationGroupMembers) || !journalMemberSubset(accounts, j.accounts) {
		p.lock.Unlock()
		return nil, timing, fmt.Errorf("group drain requires a canonical journal subset")
	}
	start := time.Now()
	pending := p.migrationPendingLocked()
	timing.ContactPending, timing.RecoveryDue = p.groupMigrationCountsLocked(accounts)
	if timing.RecoveryDue > 0 {
		p.lock.Unlock()
		timing.Status = "pending"
		timing.Scan = time.Since(start)
		return nil, timing, nil
	}
	// Copy the union before releasing the pool lock. Admissions owning recovery
	// obligations cannot pass journalGate, but ordinary work need not wait for IO.
	queue := append([]*Transaction(nil), pending...)
	p.lock.Unlock()
	timing.Scan = time.Since(start)
	start = time.Now()
	path := filepath.Join(j.directory, fmt.Sprintf("%020d.queue.json", j.sequence))
	raw, err := os.ReadFile(path)
	timing.Read = time.Since(start)
	if err != nil {
		return nil, timing, err
	}
	var last migrationQueueSnapshot
	if err = json.Unmarshal(raw, &last); err != nil {
		return nil, timing, err
	}
	if err = rejectForeignConfirmationFields(last); err != nil {
		return nil, timing, err
	}
	if last.Account != "" || last.Sequence != j.sequence || !sameJournalMembers(last.Accounts, j.accounts) {
		return nil, timing, fmt.Errorf("union journal identity mismatch")
	}
	switch last.Kind {
	case "", "full", "extend":
		if _, err = replayMigrationGeneration(nil, last); err != nil {
			return nil, timing, err
		}
	case "append", "prepend", "remove":
		if len(last.Transactions) == 0 {
			return nil, timing, fmt.Errorf("empty delta generation")
		}
	case "source-credit-remove":
		if err = ValidateSourceCreditRemovalReceipt(j.directory, last); err != nil {
			return nil, timing, err
		}
	case confirmRemoveKind:
		if _, err = validateConfirmGeneration(last); err != nil {
			return nil, timing, err
		}
	default:
		return nil, timing, fmt.Errorf("unknown queue generation kind")
	}
	// Publish the whole remaining union, NEVER nil merely because this group
	// drained. This also removes stale already-executed ordinary entries.
	start = time.Now()
	if err = p.persistMigrationQueueLocked(queue); err != nil {
		return nil, timing, err
	}
	timing.Publish = time.Since(start)
	start = time.Now()
	raw, err = os.ReadFile(filepath.Join(j.directory, fmt.Sprintf("%020d.queue.json", j.sequence)))
	timing.Read += time.Since(start)
	if err != nil {
		return nil, timing, err
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	timing.ContactPending, timing.RecoveryDue = p.groupMigrationCountsLocked(accounts)
	if timing.RecoveryDue > 0 {
		timing.Status = "pending"
		return nil, timing, nil
	}
	hash := sha256.Sum256(raw)
	proof := struct {
		Account         string
		Sequence        uint64
		Transactions    [][]byte
		Accounts        []string
		Kind            string
		JournalAccounts []string
		JournalSHA256   string
	}{"", j.sequence, nil, accounts, "full", j.accounts, hex.EncodeToString(hash[:])}
	encoded, err := json.Marshal(proof)
	if err != nil {
		return nil, timing, err
	}
	timing.Status = "ready"
	return encoded, timing, nil
}
