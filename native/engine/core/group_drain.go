package core

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Called from the next leader proposal after the previous block has committed.
// Drain recovery obligations, not every transaction contacting a member.
// Ordinary cross-shard debits stay in the pool and still execute through Relay.
// Late old-route work remains fenced and journaled after this snapshot; the
// caller separately checks handoff ACKs before proposing retirement.
func (p *TxPool) GroupDrainSnapshot(accounts []string) ([]byte, error) {
	raw, _, err := p.groupDrainSnapshot(accounts, false)
	return raw, err
}

// GroupDrainTiming identifies waiting separately from actual durable work.
type GroupDrainTiming struct {
	Status                          string
	ContactPending, RecoveryDue     int // -1 when no queue snapshot was observed
	Gate, Pool, Scan, Read, Publish time.Duration
}

// TryGroupDrainSnapshot is for the periodic PBFT proposal path. Busy means
// retirement is deferred, NOT that the queue is empty. Ordinary proposals may
// proceed; all migration fences and the forwarding journal remain in force.
func (p *TxPool) TryGroupDrainSnapshot(accounts []string) ([]byte, GroupDrainTiming, error) {
	return p.groupDrainSnapshot(accounts, true)
}

func (p *TxPool) groupDrainSnapshot(accounts []string, try bool) (raw []byte, timing GroupDrainTiming, err error) {
	timing.Status = "error"
	timing.ContactPending, timing.RecoveryDue = -1, -1
	start := time.Now()
	if try {
		if !p.journalGate.TryLock() {
			timing.Gate = time.Since(start)
			timing.Status = "journal_busy"
			return nil, timing, nil
		}
	} else {
		p.journalGate.Lock()
	}
	timing.Gate = time.Since(start)
	defer p.journalGate.Unlock()
	start = time.Now()
	if try {
		if !p.lock.TryLock() {
			timing.Pool = time.Since(start)
			timing.Status = "pool_busy"
			return nil, timing, nil
		}
	} else {
		p.lock.Lock()
	}
	timing.Pool = time.Since(start)
	j := p.migrationJournal
	if j != nil && j.pendingRaw != nil {
		p.lock.Unlock()
		return nil, timing, fmt.Errorf("migration publication outcome uncertain; recovery required")
	}
	if j != nil && j.groups > 1 && !sameJournalMembers(j.accounts, accounts) {
		return p.serialGroupDrainSnapshotLocked(accounts, timing)
	}
	if j == nil || !sameJournalMembers(j.accounts, accounts) || j.sequence == 0 {
		p.lock.Unlock()
		return nil, timing, fmt.Errorf("group drain requires matching journal")
	}
	start = time.Now()
	timing.ContactPending, timing.RecoveryDue = p.groupMigrationCountsLocked(accounts)
	if timing.RecoveryDue > 0 {
		p.lock.Unlock()
		timing.Scan = time.Since(start)
		timing.Status = "pending"
		return nil, timing, nil
	}
	// Recovery obligations require journalGate. New-route ordinary work can
	// enter while publishing; it is not a recovery obligation.
	p.lock.Unlock()
	timing.Scan = time.Since(start)
	start = time.Now()
	path := filepath.Join(j.directory, fmt.Sprintf("%020d.queue.json", j.sequence))
	raw, err = os.ReadFile(path)
	timing.Read = time.Since(start)
	if err != nil {
		return nil, timing, err
	}
	var s migrationQueueSnapshot
	if err = json.Unmarshal(raw, &s); err != nil {
		return nil, timing, err
	}
	if err = rejectForeignConfirmationFields(s); err != nil {
		return nil, timing, err
	}
	if s.Account != "" || !sameJournalMembers(s.Accounts, accounts) || s.Sequence != j.sequence {
		return nil, timing, fmt.Errorf("group drain snapshot mismatch")
	}
	// Do not convert corruption into a new valid-looking full generation.
	// Match the journal recovery contract before considering normalization.
	switch s.Kind {
	case "", "full", "extend":
	case "append", "prepend", "remove":
		if len(s.Transactions) == 0 {
			return nil, timing, fmt.Errorf("empty delta generation")
		}
	case "source-credit-remove":
		if err = ValidateSourceCreditRemovalReceipt(j.directory, s); err != nil {
			return nil, timing, err
		}
	case confirmRemoveKind:
		if _, err = validateConfirmGeneration(s); err != nil {
			return nil, timing, err
		}
	default:
		return nil, timing, fmt.Errorf("unknown queue generation kind")
	}
	if s.Kind != "" && s.Kind != "full" || len(s.Transactions) != 0 {
		start = time.Now()
		if err = p.persistMigrationQueueLocked(nil); err != nil {
			timing.Publish = time.Since(start)
			return nil, timing, err
		}
		timing.Publish = time.Since(start)
		start = time.Now()
		raw, err = os.ReadFile(filepath.Join(j.directory, fmt.Sprintf("%020d.queue.json", j.sequence)))
		timing.Read += time.Since(start)
		if err != nil {
			return nil, timing, err
		}
		// Kind is omitted in legacy full snapshots. Decoding into the previous
		// delta would retain its old Kind and reject a valid newly written full.
		s = migrationQueueSnapshot{}
		if err = json.Unmarshal(raw, &s); err != nil {
			return nil, timing, err
		}
		if (s.Kind != "" && s.Kind != "full") || s.Account != "" || !sameJournalMembers(s.Accounts, accounts) || s.Sequence != j.sequence || len(s.Transactions) != 0 {
			return nil, timing, fmt.Errorf("group drain snapshot mismatch")
		}
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	timing.ContactPending, timing.RecoveryDue = p.groupMigrationCountsLocked(accounts)
	if timing.RecoveryDue > 0 {
		timing.Status = "pending"
		return nil, timing, nil
	}
	timing.Status = "ready"
	return raw, timing, nil
}
