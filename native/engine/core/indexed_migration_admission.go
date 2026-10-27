package core

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// Enable before networking, journal restoration, or any queue admission. This
// mode is selected by the ledger's effect-index startup configuration, never
// toggled while a pool is live. The journal format and removal contract do not
// change; only a second identical pending obligation is refused before append.
func (p *TxPool) EnableIndexedMigrationAdmission() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.migrationJournal != nil || p.PendingCountLocked() != 0 {
		panic("indexed migration admission must be enabled on a fresh pool")
	}
	p.indexedMigrationAdmission = true
}

type pendingMigrationIdentity struct {
	ID        string
	Transport [32]byte
}

type cachedMigrationIdentity struct {
	Key      string
	Identity pendingMigrationIdentity
}

func migrationAdmissionIdentity(tx *Transaction) (string, pendingMigrationIdentity, error) {
	effect := TransactionDebit
	if tx != nil && tx.Relayed {
		effect = TransactionCredit
	}
	identity, err := BuildTransactionEffect(tx, effect)
	if err != nil {
		return "", pendingMigrationIdentity{}, err
	}
	// JSON canonicalizes map key order. Include the complete frozen transport,
	// not only the route-independent ledger effect: two routes for one pending
	// admission cannot silently be treated as an identical network retry.
	raw, err := json.Marshal(tx)
	if err != nil {
		return "", pendingMigrationIdentity{}, err
	}
	key, _ := json.Marshal([]string{identity.AdmissionID, identity.Effect})
	return string(key), pendingMigrationIdentity{identity.ID, sha256.Sum256(raw)}, nil
}

// Caller owns journalGate. Ordinary admission does not take this gate and may
// continue while the bounded relevant-lane snapshot is checked.
func (p *TxPool) pendingMigrationAdmissions() (map[string]pendingMigrationIdentity, error) {
	p.lock.Lock()
	j := p.migrationJournal
	if !p.indexedMigrationAdmission || j == nil {
		p.lock.Unlock()
		return nil, nil
	}
	if p.lanes == nil {
		p.lock.Unlock()
		return nil, fmt.Errorf("indexed migration admission requires isolated recovery lanes")
	}
	entries := make([]queuedMigrationWork, 0, len(p.lanes.related))
	for _, work := range p.lanes.related {
		if journalRequiresRecovery(work.tx, j.account, j.accounts) {
			entries = append(entries, work)
		}
	}
	p.lock.Unlock()
	seen := make(map[string]pendingMigrationIdentity, len(entries))
	// A disposable cache avoids re-encoding every old transport on each new
	// batch. Exact immutable original bytes, not pointers/admission IDs, are
	// the cache key. Rebuild from current recovery entries to bound retention;
	// every duplicate/conflict is still checked, including restored journals.
	current := make(map[string]cachedMigrationIdentity, len(entries))
	for _, work := range entries {
		if work.key == "" {
			return nil, fmt.Errorf("recovery entry missing immutable original")
		}
		cached, ok := p.migrationAdmissionCache[work.key]
		if !ok {
			// Use the queue-owned immutable transport for the first identity.
			// Gob can collapse empty slices to nil; an extra round-trip here
			// would change the existing full-JSON retry/conflict contract.
			key, identity, err := migrationAdmissionIdentity(work.tx)
			if err != nil {
				return nil, err
			}
			cached = cachedMigrationIdentity{key, identity}
		}
		if _, duplicate := seen[cached.Key]; duplicate {
			// A legacy/recovered duplicate is evidence of a violated durable
			// contract. Do not coalesce it or manufacture a removal receipt.
			return nil, fmt.Errorf("duplicate or conflicting recovered migration admission %s", work.tx.AdmissionID)
		}
		seen[cached.Key] = cached.Identity
		current[work.key] = cached
	}
	p.migrationAdmissionCache = current
	return seen, nil
}

// Run after startup journal restore, before networking. No ledger, queue or
// journal changes are made; malformed old evidence remains available to audit.
func (p *TxPool) ValidateIndexedMigrationAdmission() error {
	p.journalGate.Lock()
	defer p.journalGate.Unlock()
	_, err := p.pendingMigrationAdmissions()
	return err
}

// Caller owns journalGate. Complexity O(M_related + B), independent of the
// ordinary backlog; no persistent side index or new WAL is necessary.
func (p *TxPool) filterIndexedMigrationAdmission(txs []*Transaction) ([]*Transaction, error) {
	seen, err := p.pendingMigrationAdmissions()
	if err != nil || seen == nil {
		return txs, err // Legacy behavior is unchanged when the mode is off.
	}
	j := p.migrationJournal // Immutable membership under journalGate.
	kept := make([]*Transaction, 0, len(txs))
	for _, tx := range txs {
		if tx == nil {
			return nil, fmt.Errorf("nil indexed migration admission")
		}
		if !journalRequiresRecovery(tx, j.account, j.accounts) {
			kept = append(kept, tx)
			continue
		}
		key, identity, err := migrationAdmissionIdentity(tx)
		if err != nil {
			return nil, err
		}
		if old, pending := seen[key]; pending {
			if old != identity {
				return nil, fmt.Errorf("conflicting pending migration admission %s", tx.AdmissionID)
			}
			continue
		}
		seen[key] = identity
		kept = append(kept, tx)
	}
	return kept, nil
}
