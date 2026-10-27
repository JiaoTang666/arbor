package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const sourceCreditRemoveKind = "source-credit-remove"

// SourceCreditConfirmation is issued only after a local committed credit-index
// lookup. It is NOT a target ACK, a network authorization or a Byzantine proof.
// Recovery trusts the local journal/receipt filesystem, as existing queue
// recovery does; untrusted JSON must never call ConfirmSourceCreditBatch.
type SourceCreditConfirmation struct {
	Original          []byte            `json:"original"`
	Effect            TransactionEffect `json:"effect"`
	ExecutedBlock     uint64            `json:"executed_block"`
	ObservedStateRoot string            `json:"observed_state_root"`
	// Ordinary source-local transactions require both effects at the same
	// observed root. Omitted fields preserve existing Relay receipt bytes.
	DebitEffect        *TransactionEffect `json:"debit_effect,omitempty"`
	DebitExecutedBlock uint64             `json:"debit_executed_block,omitempty"`
}

func validateSourceCredit(item SourceCreditConfirmation) (*Transaction, error) {
	var tx Transaction
	if err := gob.NewDecoder(bytes.NewReader(item.Original)).Decode(&tx); err != nil {
		return nil, err
	}
	effect, err := BuildTransactionEffect(&tx, TransactionCredit)
	root, rootErr := hex.DecodeString(item.ObservedStateRoot)
	if err != nil || effect != item.Effect || !tx.RoutingFrozen || item.ExecutedBlock == 0 || rootErr != nil || len(root) != sha256.Size {
		return nil, fmt.Errorf("invalid source credit completion")
	}
	if tx.Relayed {
		if item.DebitEffect != nil || item.DebitExecutedBlock != 0 {
			return nil, fmt.Errorf("Relay completion carries an unrelated debit proof")
		}
	} else {
		debit, err := BuildTransactionEffect(&tx, TransactionDebit)
		if err != nil || tx.RoutedSenderShard != tx.RoutedRecipientShard || item.DebitEffect == nil || *item.DebitEffect != debit || item.DebitExecutedBlock == 0 {
			return nil, fmt.Errorf("ordinary source completion requires both local effects")
		}
	}
	return &tx, nil
}

func sourceCreditReceiptData(items []SourceCreditConfirmation) ([]byte, error) {
	raw, err := json.Marshal(struct {
		Schema string                     `json:"schema"`
		Items  []SourceCreditConfirmation `json:"items"`
	}{"saa_source_credit_completion_v1", items})
	if err == nil && len(raw) > artifactBatchLimit {
		return nil, fmt.Errorf("source credit completion batch too large")
	}
	return raw, err
}

func sourceCreditReceiptPath(directory string, raw []byte) string {
	h := sha256.Sum256(raw)
	return filepath.Join(directory, "source-credit-confirmed-"+hex.EncodeToString(h[:])+".json")
}

// ValidateSourceCreditRemovalReceipt binds this special removal generation to
// a previously durable source completion record in the SAME journal directory.
func ValidateSourceCreditRemovalReceipt(directory string, s migrationQueueSnapshot) error {
	_, err := sourceCreditRemovalKeys(directory, s)
	return err
}

func sourceCreditRemovalKeys(directory string, s migrationQueueSnapshot) (map[string]bool, error) {
	if directory == "" || s.Kind != sourceCreditRemoveKind || len(s.SourceCredits) == 0 || len(s.SourceCredits) != len(s.Transactions) {
		return nil, fmt.Errorf("source credit removal requires bound local receipt")
	}
	keys := map[string]bool{}
	for i, item := range s.SourceCredits {
		tx, err := validateSourceCredit(item)
		if err != nil || !journalRequiresRecovery(tx, s.Account, s.Accounts) || !bytes.Equal(item.Original, s.Transactions[i]) {
			return nil, fmt.Errorf("source credit removal proof mismatch")
		}
		key := string(tx.Encode())
		if keys[key] {
			return nil, fmt.Errorf("duplicate source credit removal identity")
		}
		keys[key] = true
	}
	raw, err := sourceCreditReceiptData(s.SourceCredits)
	if err != nil {
		return nil, err
	}
	stored, err := os.ReadFile(sourceCreditReceiptPath(directory, raw))
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(stored, append(raw, '\n')) {
		return nil, fmt.Errorf("source credit completion receipt conflict")
	}
	return keys, nil
}

func replaySourceCreditRemoval(recovered []*Transaction, s migrationQueueSnapshot, directory string) ([]*Transaction, error) {
	keys, err := sourceCreditRemovalKeys(directory, s)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	kept := make([]*Transaction, 0, len(recovered))
	for _, tx := range recovered {
		key := string(tx.Encode())
		if keys[key] {
			counts[key]++
		} else {
			kept = append(kept, tx)
		}
	}
	for key := range keys {
		if counts[key] == 0 {
			return nil, fmt.Errorf("source credit removal lacks durable occurrence")
		}
	}
	return kept, nil
}

// ConfirmSourceCreditBatch retains evidence before queue removal. Unlike normal
// handoff ACKs, one locally completed credit permits dropping all exact duplicate
// copies of that same admission; distinct AdmissionIDs remain distinct work.
// Only the related lane is scanned. Ordinary traffic never holds journalGate.
func (p *TxPool) ConfirmSourceCreditBatch(items []SourceCreditConfirmation) (int, error) {
	confirmations := map[string]SourceCreditConfirmation{}
	for _, item := range items {
		tx, err := validateSourceCredit(item)
		if err != nil {
			return 0, err
		}
		key := string(tx.Encode())
		if old, ok := confirmations[key]; ok && (old.Effect != item.Effect || old.ExecutedBlock != item.ExecutedBlock || old.ObservedStateRoot != item.ObservedStateRoot || old.DebitExecutedBlock != item.DebitExecutedBlock || (old.DebitEffect == nil) != (item.DebitEffect == nil) || old.DebitEffect != nil && *old.DebitEffect != *item.DebitEffect) {
			return 0, fmt.Errorf("conflicting source credit completions")
		}
		confirmations[key] = item
	}
	p.journalGate.Lock()
	defer p.journalGate.Unlock()
	p.lock.Lock()
	if p.migrationJournal == nil || p.lanes == nil {
		p.lock.Unlock()
		return 0, fmt.Errorf("source credit confirmation requires migration journal")
	}
	j := p.migrationJournal
	s := migrationQueueSnapshot{Account: j.account, Accounts: j.accounts, Sequence: j.sequence + 1, Kind: sourceCreditRemoveKind}
	keys := map[string]bool{}
	for _, entry := range p.lanes.related {
		if item, ok := confirmations[entry.key]; ok {
			if !p.heldForMigration(entry.tx) {
				p.lock.Unlock()
				return 0, fmt.Errorf("source credit completion matches executable work")
			}
			if !keys[entry.key] {
				keys[entry.key] = true
				original := entry.tx.Encode()
				s.Transactions = append(s.Transactions, original)
				// Encode locally so generation and receipt agree after gob recovery.
				item.Original = original
				s.SourceCredits = append(s.SourceCredits, item)
			}
		}
	}
	p.lock.Unlock()
	if len(keys) == 0 {
		return 0, nil
	}
	raw, err := sourceCreditReceiptData(s.SourceCredits)
	if err != nil {
		return 0, err
	}
	if err = PublishSourcePrepareFile(sourceCreditReceiptPath(j.directory, raw), append(raw, '\n')); err != nil {
		return 0, err
	}
	raw, err = json.Marshal(s)
	if err != nil {
		return 0, err
	}
	if err = p.publishMigrationQueueFile(filepath.Join(j.directory, fmt.Sprintf("%020d.queue.json", s.Sequence)), raw); err != nil {
		return 0, err
	}
	j.sequence = s.Sequence
	p.lock.Lock()
	defer p.lock.Unlock()
	kept, removed := p.lanes.related[:0], 0
	for _, entry := range p.lanes.related {
		if keys[entry.key] {
			removed++
		} else {
			kept = append(kept, entry)
		}
	}
	for i := len(kept); i < len(p.lanes.related); i++ {
		p.lanes.related[i] = queuedMigrationWork{}
	}
	p.lanes.related = kept
	return removed, nil
}
