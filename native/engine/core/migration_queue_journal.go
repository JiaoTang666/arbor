package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type migrationQueueJournal struct {
	directory, account string
	sequence           uint64
	accounts           []string
	groups             int
	// An unsuccessful publication may already exist on disk. Until the exact
	// bytes are durably retried, no different event may reuse this generation.
	pendingPath string
	pendingRaw  []byte
}
type migrationQueueSnapshot struct {
	Account            string
	Sequence           uint64
	Transactions       [][]byte
	Accounts           []string                   `json:",omitempty"`
	Kind               string                     `json:",omitempty"` // empty/full: snapshot; append/prepend/remove: durable delta
	PreviousAccounts   []string                   `json:",omitempty"` // extend: exact previous immutable membership
	SourceCredits      []SourceCreditConfirmation `json:",omitempty"` // source-credit-remove only; local committed-credit evidence
	ConfirmationSchema string                     `json:",omitempty"`
	Confirmations      []queueHandoffConfirmation `json:",omitempty"`
}

// EnableMigrationQueueJournal must run before accepting migration traffic.
// Its directory belongs to exactly one local replica and migration account.
// Snapshots are retained, not overwritten; a partial write cannot replace the
// previous generation. This does not recover traffic lost before enabling it.
func (p *TxPool) EnableMigrationQueueJournal(directory, account string) error {
	return p.enableMigrationQueueJournal(directory, canonicalPrepareAccount(account), nil)
}

func (p *TxPool) EnableGroupMigrationQueueJournal(directory string, accounts []string) error {
	if len(accounts) < MinMigrationGroupMembers || len(accounts) > MaxMigrationGroupMembers {
		return fmt.Errorf("group journal requires %d..%d members", MinMigrationGroupMembers, MaxMigrationGroupMembers)
	}
	for i, a := range accounts {
		if a == "" || canonicalPrepareAccount(a) != a || (i > 0 && accounts[i-1] >= a) {
			return fmt.Errorf("group journal members must be canonical and sorted")
		}
	}
	return p.enableMigrationQueueJournal(directory, "", append([]string(nil), accounts...))
}

func journalTouches(tx *Transaction, account string, accounts []string) bool {
	if len(accounts) == 0 {
		return TransactionTouchesAccount(tx, account)
	}
	for _, a := range accounts {
		if TransactionTouchesAccount(tx, a) {
			return true
		}
	}
	return false
}

// Contact is not a recovery obligation. A frozen ordinary-sender cross-shard
// debit can execute through Relay without handoff; persisting it here would
// both stall its injection batch and replay already executed work on restart.
// Keep journalTouches for decoding legacy generations and identity validation.
func journalRequiresRecovery(tx *Transaction, account string, accounts []string) bool {
	// Non-native/broker payloads retain conservative legacy treatment.
	if tx != nil && (tx.HasBroker || tx.OriginalSender != "" || tx.FinalRecipient != "") {
		return journalTouches(tx, account, accounts)
	}
	if len(accounts) == 0 {
		return holdMigrationTransaction(tx, account)
	}
	for _, a := range accounts {
		if holdMigrationTransaction(tx, a) {
			return true
		}
	}
	return false
}

func sameJournalMembers(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (p *TxPool) enableMigrationQueueJournal(directory, account string, accounts []string) error {
	p.journalGate.Lock()
	defer p.journalGate.Unlock()
	p.lock.Lock()
	defer p.lock.Unlock()
	account = canonicalPrepareAccount(account)
	if directory == "" || (account == "" && len(accounts) == 0) {
		return fmt.Errorf("journal directory and account required")
	}
	if p.migrationJournal != nil {
		return fmt.Errorf("migration journal already enabled")
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	var last migrationQueueSnapshot
	var recovered []*Transaction
	groups := 0
	confirmed := map[string]completedBatchReceiptItem{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".queue.json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return err
		}
		var s migrationQueueSnapshot
		if err = json.Unmarshal(raw, &s); err != nil {
			return err
		}
		if s.Account != account || s.Sequence != last.Sequence+1 || entry.Name() != fmt.Sprintf("%020d.queue.json", s.Sequence) {
			return fmt.Errorf("migration journal identity or generation gap")
		}
		if err = validateJournalMembership(last, s, accounts, &groups); err != nil {
			return err
		}
		recovered, err = replayMigrationGeneration(recovered, s, directory)
		if err != nil {
			return err
		}
		for _, c := range s.Confirmations {
			if err = mergeCompletedReceipt(confirmed, c.Receipt); err != nil {
				return err
			}
		}
		last = s
	}
	if last.Sequence > 0 && !sameJournalMembers(last.Accounts, accounts) {
		return fmt.Errorf("journal membership does not match committed group union")
	}
	if last.Sequence > 0 {
		for _, tx := range p.TxQueue {
			if journalTouches(tx, account, accounts) {
				return fmt.Errorf("restore requires no existing migration entries")
			}
		}
	}
	if groups == 0 && len(accounts) > 0 {
		groups = 1
	}
	j := &migrationQueueJournal{directory: directory, account: account, sequence: last.Sequence, accounts: accounts, groups: groups}
	p.migrationJournal = j
	if last.Sequence == 0 {
		if err = p.persistMigrationQueueLocked(p.TxQueue); err != nil {
			p.migrationJournal = nil
			return err
		}
	} else {
		p.TxQueue = append(p.TxQueue, recovered...)
	}
	initial := p.TxQueue
	p.lanes = &migrationLanes{}
	p.TxQueue = nil
	p.appendPendingLocked(initial, false)
	if p.heldMigrationAccounts == nil {
		p.heldMigrationAccounts = make(map[string]bool)
	}
	if len(accounts) == 0 {
		p.heldMigrationAccounts[account] = true
		p.sourcePreparedAccount = account
	} else {
		for _, a := range accounts {
			p.heldMigrationAccounts[a] = true
		}
		p.sourcePreparedAccount = accounts[0]
	}
	return nil
}

func (p *TxPool) persistMigrationQueueLocked(queue []*Transaction) error {
	j := p.migrationJournal
	if j == nil {
		return nil
	}
	s := migrationQueueSnapshot{Account: j.account, Accounts: j.accounts, Sequence: j.sequence + 1}
	for _, tx := range queue {
		if journalRequiresRecovery(tx, j.account, j.accounts) {
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
	j.sequence = s.Sequence
	return nil
}

// Journal only newly admitted held work. The prior full generation plus the
// ordered deltas recovers the same queue; every delta is fsynced before memory
// admission. Unrelated work is neither written nor included in recovery.
func (p *TxPool) persistMigrationQueueAdditionLocked(txs []*Transaction, prepend bool) error {
	return p.persistMigrationQueueAdditionEncodedLocked(txs, prepend, nil, nil)
}

func (p *TxPool) persistMigrationQueueAdditionEncodedLocked(txs []*Transaction, prepend bool, encoded map[*Transaction][]byte, timing *journalAdmissionTiming) error {
	j := p.migrationJournal
	if j == nil {
		return nil
	}
	s := migrationQueueSnapshot{Account: j.account, Accounts: j.accounts, Sequence: j.sequence + 1, Kind: "append"}
	if prepend {
		s.Kind = "prepend"
	}
	for _, tx := range txs {
		if journalRequiresRecovery(tx, j.account, j.accounts) {
			raw, prepared := encoded[tx]
			if !prepared {
				raw = tx.Encode()
			}
			s.Transactions = append(s.Transactions, raw)
		}
	}
	if timing != nil {
		timing.RecoveryItems = len(s.Transactions)
	}
	if len(s.Transactions) == 0 {
		return nil
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	started := time.Now()
	err = p.publishMigrationQueueFile(filepath.Join(j.directory, fmt.Sprintf("%020d.queue.json", s.Sequence)), raw)
	if timing != nil {
		timing.Publish = time.Since(started)
	}
	if err != nil {
		return err
	}
	j.sequence = s.Sequence
	return nil
}

// One durable generation removes a completed batch; no full queue rewrite.
func (p *TxPool) persistMigrationRemovalLocked(records [][]byte) error {
	j := p.migrationJournal
	s := migrationQueueSnapshot{Account: j.account, Accounts: j.accounts, Sequence: j.sequence + 1, Kind: "remove", Transactions: records}
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if err = p.publishMigrationQueueFile(filepath.Join(j.directory, fmt.Sprintf("%020d.queue.json", s.Sequence)), raw); err != nil {
		return err
	}
	j.sequence = s.Sequence
	return nil
}

func (p *TxPool) publishMigrationQueueFile(path string, raw []byte) error {
	j := p.migrationJournal
	if j != nil && j.pendingRaw != nil && (j.pendingPath != path || !bytes.Equal(j.pendingRaw, raw)) {
		return fmt.Errorf("migration publication outcome uncertain; retry exact generation or recover")
	}
	var err error
	if p.migrationJournalPublish != nil {
		err = p.migrationJournalPublish(path, raw)
	} else {
		err = PublishSourcePrepareFile(path, raw)
	}
	if j != nil {
		if err != nil {
			j.pendingPath, j.pendingRaw = path, append([]byte(nil), raw...)
		} else {
			j.pendingPath, j.pendingRaw = "", nil
		}
	}
	return err
}
