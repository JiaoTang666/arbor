package core

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const confirmRemoveKind = "confirm-remove"
const confirmRemoveSchema = "saa_confirm_remove_v2"

// Retirement freshness includes completion facts, not full queue checkpoints
// produced by retirement itself. Callers explicitly provide the replica journal.
func CompletedMigrationGenerationFiles(directory string) ([]string, error) {
	info, err := os.Stat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("migration journal is not a directory")
	}
	paths, err := filepath.Glob(filepath.Join(directory, "*.queue.json"))
	if err != nil {
		return nil, err
	}
	var confirmed []string
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("nonregular migration generation %s", path)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		completed, err := classifyConfirmGeneration(path, raw)
		if err != nil {
			return nil, err
		}
		if completed {
			confirmed = append(confirmed, path)
		}
	}
	return confirmed, nil
}

// Classification deliberately avoids decoding the transaction payload of
// ordinary queue generations. Both the stateless audit and cached reader use
// this same check; full completion validation is still required below.
func classifyConfirmGeneration(path string, raw []byte) (bool, error) {
	var s struct {
		Kind               string
		Sequence           uint64
		ConfirmationSchema string
		Confirmations      json.RawMessage
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return false, err
	}
	if s.Kind != confirmRemoveKind {
		if len(s.Confirmations) != 0 || s.ConfirmationSchema != "" {
			return false, fmt.Errorf("confirmation fields in another journal kind")
		}
		return false, nil
	}
	if filepath.Base(path) != fmt.Sprintf("%020d.queue.json", s.Sequence) || s.Sequence == 0 {
		return false, fmt.Errorf("joint confirmation filename mismatch")
	}
	return true, nil
}

func rejectForeignConfirmationFields(s migrationQueueSnapshot) error {
	if s.Kind != confirmRemoveKind && (s.ConfirmationSchema != "" || len(s.Confirmations) != 0) {
		return fmt.Errorf("confirmation fields in another journal kind")
	}
	return nil
}

func mergeCompletedReceipt(result map[string]completedBatchReceiptItem, item completedBatchReceiptItem) error {
	if old, ok := result[item.ID]; ok {
		a, _ := json.Marshal(old)
		b, _ := json.Marshal(item)
		if !bytes.Equal(a, b) {
			return fmt.Errorf("conflicting completed confirmations for %s", item.ID)
		}
	}
	result[item.ID] = item
	return nil
}

func readConfirmGeneration(path string) (migrationQueueSnapshot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return migrationQueueSnapshot{}, err
	}
	return decodeConfirmGeneration(raw)
}

func decodeConfirmGeneration(raw []byte) (s migrationQueueSnapshot, err error) {
	if len(raw) > artifactBatchLimit {
		return s, fmt.Errorf("joint confirmation generation too large")
	}
	if err = json.Unmarshal(raw, &s); err == nil {
		_, err = validateConfirmGeneration(s)
	}
	return s, err
}

// One immutable queue generation is both the full completion evidence and the
// actual dequeue delta. H is retained so recovery can validate the same proof
// without depending on a separately published receipt or an outbox path.
type queueHandoffConfirmation struct {
	H               *MigrationHandoff         `json:"handoff"`
	Receipt         completedBatchReceiptItem `json:"receipt"`
	InboundOriginal []byte                    `json:"inbound_original,omitempty"`
}

func completedOriginal(item CompletedHandoffConfirmation) ([]byte, error) {
	if item.H == nil {
		return nil, fmt.Errorf("completed confirmation requires handoff")
	}
	var tx Transaction
	if err := gob.NewDecoder(bytes.NewReader(item.H.Original)).Decode(&tx); err != nil {
		return nil, err
	}
	original := tx.Encode()
	if item.InboundOriginal != nil {
		if item.InboundOriginal.Relayed || item.H.Leg != "credit" {
			return nil, fmt.Errorf("invalid inbound original")
		}
		copyTx := DecodeTx(item.InboundOriginal.Encode())
		copyTx.Relayed = true
		if !bytes.Equal(copyTx.Encode(), original) {
			return nil, fmt.Errorf("inbound original mismatch")
		}
		original = item.InboundOriginal.Encode()
	}
	return original, nil
}

// Validate complete proofs before removal, including confirmation-only events.
// The journal membership is a union of groups, not one proposal identifier.
func validateConfirmGeneration(s migrationQueueSnapshot) (map[string]bool, error) {
	if s.Kind != confirmRemoveKind || s.ConfirmationSchema != confirmRemoveSchema || s.Sequence == 0 || len(s.Confirmations) == 0 || len(s.SourceCredits) != 0 || len(s.PreviousAccounts) != 0 {
		return nil, fmt.Errorf("invalid joint confirmation generation")
	}
	if len(s.Accounts) == 0 {
		if !canonicalJournalMembers([]string{s.Account}, 1, 1) {
			return nil, fmt.Errorf("invalid joint singleton membership")
		}
	} else if s.Account != "" || !canonicalJournalMembers(s.Accounts, MinMigrationGroupMembers, len(s.Accounts)) {
		return nil, fmt.Errorf("invalid joint group union membership")
	}
	confirmed := make(map[string]bool, len(s.Confirmations))
	ids := make(map[string]bool, len(s.Confirmations))
	for _, c := range s.Confirmations {
		if c.H == nil || c.Receipt.ID != c.H.ID || ids[c.H.ID] {
			return nil, fmt.Errorf("invalid or duplicate joint handoff identity")
		}
		ids[c.H.ID] = true
		if len(s.Accounts) == 0 {
			if c.H.Account != s.Account || len(c.H.GroupAccounts) != 0 {
				return nil, fmt.Errorf("joint confirmation singleton scope mismatch")
			}
		} else if len(c.H.GroupAccounts) == 0 || !journalMemberSubset(c.H.GroupAccounts, s.Accounts) {
			return nil, fmt.Errorf("joint confirmation group scope mismatch")
		}
		if err := validateCompletedAuditReceipt(c.H, c.Receipt); err != nil {
			return nil, err
		}
		item := CompletedHandoffConfirmation{H: c.H, Ack: c.Receipt.Ack, Relay: c.Receipt.Relay}
		// A pre-credit inbound queue entry is the sole permitted difference.
		if len(c.InboundOriginal) > 0 {
			var inbound Transaction
			if err := gob.NewDecoder(bytes.NewReader(c.InboundOriginal)).Decode(&inbound); err != nil {
				return nil, err
			}
			item.InboundOriginal = &inbound
		}
		key, err := completedOriginal(item)
		if err != nil {
			return nil, err
		}
		var tx Transaction
		if err := gob.NewDecoder(bytes.NewReader(key)).Decode(&tx); err != nil {
			return nil, err
		}
		if !journalRequiresRecovery(&tx, s.Account, s.Accounts) || confirmed[string(key)] {
			return nil, fmt.Errorf("joint confirmation original or membership mismatch")
		}
		confirmed[string(key)] = true
	}
	removed := map[string]bool{}
	for _, raw := range s.Transactions {
		var tx Transaction
		if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&tx); err != nil {
			return nil, err
		}
		key := string(tx.Encode())
		if !confirmed[key] || removed[key] {
			return nil, fmt.Errorf("joint removal lacks unique complete confirmation")
		}
		removed[key] = true
	}
	return removed, nil
}

func (p *TxPool) persistCompletedGenerationLocked(items []CompletedHandoffConfirmation, removed [][]byte, existing map[string]completedBatchReceiptItem) error {
	j := p.migrationJournal
	if j == nil {
		return fmt.Errorf("joint confirmation requires migration journal")
	}
	s := migrationQueueSnapshot{Account: j.account, Accounts: j.accounts, Sequence: j.sequence + 1, Kind: confirmRemoveKind, ConfirmationSchema: confirmRemoveSchema, Transactions: removed}
	allRecorded := true
	for _, item := range items {
		r := completedBatchReceiptItem{ID: item.H.ID, Ack: item.Ack, Relay: item.Relay}
		if old, ok := existing[r.ID]; !ok {
			allRecorded = false
		} else {
			a, _ := json.Marshal(old)
			b, _ := json.Marshal(r)
			if !bytes.Equal(a, b) {
				return fmt.Errorf("conflicting joint confirmation %s", r.ID)
			}
		}
		var inbound []byte
		if item.InboundOriginal != nil {
			inbound = item.InboundOriginal.Encode()
		}
		s.Confirmations = append(s.Confirmations, queueHandoffConfirmation{item.H, r, inbound})
	}
	if _, err := validateConfirmGeneration(s); err != nil {
		return err
	}
	if len(removed) == 0 && allRecorded && j.pendingRaw == nil {
		return nil
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if len(raw) > artifactBatchLimit {
		return fmt.Errorf("joint confirmation generation too large")
	}
	if err := p.publishMigrationQueueFile(filepath.Join(j.directory, fmt.Sprintf("%020d.queue.json", s.Sequence)), raw); err != nil {
		return err
	}
	j.sequence = s.Sequence
	return nil
}
