package core

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

func canonicalJournalMembers(accounts []string, minimum, maximum int) bool {
	if len(accounts) < minimum || len(accounts) > maximum {
		return false
	}
	for i, a := range accounts {
		b, err := hex.DecodeString(a)
		if err != nil || len(b) != 20 || canonicalPrepareAccount(a) != a || (i > 0 && accounts[i-1] >= a) {
			return false
		}
	}
	return true
}

func journalMemberSubset(subset, all []string) bool {
	for _, a := range subset {
		i := sort.SearchStrings(all, a)
		if i == len(all) || all[i] != a {
			return false
		}
	}
	return true
}

// A membership change is a named, monotonic full generation. Ordinary full
// snapshots and deltas cannot silently add or drop an account. Recovery is
// also bound to the exact account union supplied by committed chain state.
func validateJournalMembership(previous, next migrationQueueSnapshot, expected []string, groups *int) error {
	if len(expected) == 0 {
		if len(next.Accounts) != 0 || len(next.PreviousAccounts) != 0 || next.Kind == "extend" {
			return fmt.Errorf("group transition in singleton journal")
		}
		return nil
	}
	if !canonicalJournalMembers(next.Accounts, MinMigrationGroupMembers, len(expected)) || !journalMemberSubset(next.Accounts, expected) {
		return fmt.Errorf("invalid or uncommitted journal membership")
	}
	if previous.Sequence == 0 {
		if next.Kind != "" && next.Kind != "full" || len(next.PreviousAccounts) != 0 || len(next.Accounts) > MaxMigrationGroupMembers {
			return fmt.Errorf("invalid initial group generation")
		}
		*groups = 1
		return nil
	}
	if next.Kind == "extend" {
		added := len(next.Accounts) - len(previous.Accounts)
		if !sameJournalMembers(next.PreviousAccounts, previous.Accounts) || !journalMemberSubset(previous.Accounts, next.Accounts) || added < MinMigrationGroupMembers || added > MaxMigrationGroupMembers {
			return fmt.Errorf("invalid journal extension")
		}
		*groups = *groups + 1
	} else if len(next.PreviousAccounts) != 0 || !sameJournalMembers(previous.Accounts, next.Accounts) {
		return fmt.Errorf("journal membership changed without extension")
	}
	return nil
}

// RestoreGroupMigrationQueueJournal is intentionally distinct from initial
// enable: a >32-member union is accepted only if disk contains valid, bounded
// extensions ending at exactly that union. It cannot create a giant first group.
func (p *TxPool) RestoreGroupMigrationQueueJournal(directory string, committedUnion []string) error {
	if !canonicalJournalMembers(committedUnion, MinMigrationGroupMembers, len(committedUnion)) {
		return fmt.Errorf("invalid committed journal union")
	}
	// Recovery must never manufacture a missing first snapshot.
	if _, err := os.ReadFile(filepath.Join(directory, fmt.Sprintf("%020d.queue.json", 1))); err != nil {
		return err
	}
	return p.enableMigrationQueueJournal(directory, "", append([]string(nil), committedUnion...))
}

// ExtendGroupMigrationQueueJournal keeps prior groups' holds and recovery
// obligations. Called at serialized source preparation, before chain commit.
// A crash between journal extension and commit fails closed at startup because
// Restore requires equality with the committed union; no silent rollback.
func (p *TxPool) ExtendGroupMigrationQueueJournal(accounts []string) error {
	if !canonicalJournalMembers(accounts, MinMigrationGroupMembers, MaxMigrationGroupMembers) {
		return fmt.Errorf("invalid extension group")
	}
	p.journalGate.Lock()
	defer p.journalGate.Unlock()
	p.lock.Lock()
	defer p.lock.Unlock()
	j := p.migrationJournal
	if j == nil || j.account != "" || len(j.accounts) == 0 {
		return fmt.Errorf("journal cannot accept another group")
	}
	union := append(append([]string(nil), j.accounts...), accounts...)
	sort.Strings(union)
	if !canonicalJournalMembers(union, MinMigrationGroupMembers, len(j.accounts)+MaxMigrationGroupMembers) {
		return fmt.Errorf("remigration or overlapping group")
	}
	queue := p.pendingQueueLocked()
	s := migrationQueueSnapshot{Sequence: j.sequence + 1, Accounts: union, PreviousAccounts: append([]string(nil), j.accounts...), Kind: "extend"}
	for _, tx := range queue {
		if journalRequiresRecovery(tx, "", union) {
			s.Transactions = append(s.Transactions, tx.Encode())
		}
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if err = p.publishMigrationQueueFile(filepath.Join(j.directory, fmt.Sprintf("%020d.queue.json", s.Sequence)), raw); err != nil {
		return err
	}
	// Membership is immutable after pointer publication. Admissions either win
	// this pool lock first (and are included above), or observe the new membership.
	p.migrationJournal = &migrationQueueJournal{directory: j.directory, sequence: s.Sequence, accounts: union, groups: j.groups + 1}
	p.lanes = &migrationLanes{}
	p.appendPendingLocked(queue, false)
	for _, a := range accounts {
		p.heldMigrationAccounts[a] = true
	}
	return nil
}
