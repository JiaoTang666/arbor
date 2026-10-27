package core

// Contact and durable recovery are different populations. Inbound ordinary
// debit may contact a moved recipient yet remain eligible for native Relay.
// Counts refer to the same immutable queue snapshot, not to later ACK jobs.
type GroupMigrationWork struct {
	Transactions                []*Transaction
	ContactPending, RecoveryDue int
}

func (p *TxPool) SnapshotGroupMigrationWork(accounts []string) GroupMigrationWork {
	p.lock.Lock()
	var originals []string
	if p.lanes != nil {
		for _, work := range p.lanes.related {
			if journalTouches(work.tx, "", accounts) {
				originals = append(originals, work.key)
			}
		}
	} else {
		// Compatibility before journal enable; live journal lanes already own
		// immutable originals and need no encode/decode under the queue lock.
		for _, tx := range p.TxQueue {
			if journalTouches(tx, "", accounts) {
				originals = append(originals, string(tx.Encode()))
			}
		}
	}
	p.lock.Unlock()
	result := GroupMigrationWork{Transactions: make([]*Transaction, 0, len(originals)), ContactPending: len(originals)}
	for _, raw := range originals {
		tx := DecodeTx([]byte(raw))
		result.Transactions = append(result.Transactions, tx)
		if journalRequiresRecovery(tx, "", accounts) {
			result.RecoveryDue++
		}
	}
	return result
}

func (p *TxPool) SnapshotGroupMigrationTransactions(accounts []string) []*Transaction {
	return p.SnapshotGroupMigrationWork(accounts).Transactions
}

// Called with the pool lock held; does not encode or perform I/O.
func (p *TxPool) groupMigrationCountsLocked(accounts []string) (contact, recovery int) {
	for _, tx := range p.migrationPendingLocked() {
		if journalTouches(tx, "", accounts) {
			contact++
		}
		if journalRequiresRecovery(tx, "", accounts) {
			recovery++
		}
	}
	return
}
