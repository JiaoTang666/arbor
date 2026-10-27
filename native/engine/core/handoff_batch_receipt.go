package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// One immutable, fsynced record is the durable confirmation for an entire
// completed batch. The source queue is published only after this record.
// Legacy per-handoff confirmation files remain readable by the audit path.
const completedBatchReceiptSchema = "saa_completed_handoff_batch_v1"
const completedBatchReceiptPrefix = "batch-confirmed-"

type completedBatchReceiptItem struct {
	ID    string           `json:"id"`
	Ack   HandoffAck       `json:"ack"`
	Relay *HandoffRelayAck `json:"relay,omitempty"`
}

type completedBatchReceipt struct {
	Schema string                      `json:"schema"`
	Items  []completedBatchReceiptItem `json:"items"`
}

func completedBatchReceiptPath(directory string, raw []byte) string {
	hash := sha256.Sum256(raw)
	return filepath.Join(directory, completedBatchReceiptPrefix+hex.EncodeToString(hash[:])+".json")
}

func persistCompletedBatchReceipt(directory string, items []CompletedHandoffConfirmation) error {
	receipt := completedBatchReceipt{Schema: completedBatchReceiptSchema, Items: make([]completedBatchReceiptItem, 0, len(items))}
	for _, item := range items {
		receipt.Items = append(receipt.Items, completedBatchReceiptItem{ID: item.H.ID, Ack: item.Ack, Relay: item.Relay})
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if len(raw) > 8<<20 {
		return fmt.Errorf("completed batch receipt too large")
	}
	return PublishSourcePrepareFile(completedBatchReceiptPath(directory, raw), append(raw, '\n'))
}

func checkCompletedReceiptConflicts(directory string, items []CompletedHandoffConfirmation, journalDirs ...string) error {
	_, err := checkedCompletedReceiptState(directory, items, journalDirs...)
	return err
}

func checkedCompletedReceiptState(directory string, items []CompletedHandoffConfirmation, journalDirs ...string) (map[string]completedBatchReceiptItem, error) {
	existing, err := loadCompletedBatchReceipts(directory, journalDirs...)
	if err != nil {
		return nil, err
	}
	return checkSelectedCompletedReceipts(directory, items, existing)
}

func checkSelectedCompletedReceipts(directory string, items []CompletedHandoffConfirmation, existing map[string]completedBatchReceiptItem) (map[string]completedBatchReceiptItem, error) {
	for _, item := range items {
		current := completedBatchReceiptItem{ID: item.H.ID, Ack: item.Ack, Relay: item.Relay}
		if old, ok := existing[item.H.ID]; ok {
			a, _ := json.Marshal(old)
			b, _ := json.Marshal(current)
			if !bytes.Equal(a, b) {
				return nil, fmt.Errorf("conflicting completed batch receipt for %s", item.H.ID)
			}
		}
		ackRaw, _ := json.Marshal(item.Ack)
		if err := checkExistingReceiptFile(filepath.Join(directory, item.H.ID+".confirmed.json"), append(ackRaw, '\n')); err != nil {
			return nil, err
		}
		if item.Relay != nil {
			relayRaw, _ := json.Marshal(item.Relay)
			if err := checkExistingReceiptFile(filepath.Join(directory, item.H.ID+".relay-confirmed.json"), append(relayRaw, '\n')); err != nil {
				return nil, err
			}
		} else if _, err := os.Stat(filepath.Join(directory, item.H.ID+".relay-confirmed.json")); err == nil {
			return nil, fmt.Errorf("unexpected legacy relay receipt for %s", item.H.ID)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
		// Exact legacy receipts are also durable completion facts. A repeated
		// confirmation after upgrading must not manufacture an empty generation.
		_, ackErr := os.Stat(filepath.Join(directory, item.H.ID+".confirmed.json"))
		relayPresent := item.Relay == nil
		if item.Relay != nil {
			_, relayErr := os.Stat(filepath.Join(directory, item.H.ID+".relay-confirmed.json"))
			relayPresent = relayErr == nil
		}
		if ackErr == nil && relayPresent {
			existing[item.H.ID] = current
		}
	}
	return existing, nil
}

func checkExistingReceiptFile(path string, expected []byte) error {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !bytes.Equal(raw, expected) {
		return fmt.Errorf("legacy receipt conflict: %s", path)
	}
	return nil
}

// Load every durable batch before answering a drain audit. Reject modified
// filenames, malformed records and contradictory confirmations for one ID.
func loadCompletedBatchReceipts(directory string, journalDirs ...string) (map[string]completedBatchReceiptItem, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	result := make(map[string]completedBatchReceiptItem)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, completedBatchReceiptPrefix) || !strings.HasSuffix(name, ".json") {
			continue
		}
		if !entry.Type().IsRegular() {
			return nil, fmt.Errorf("nonregular completed batch receipt %s", name)
		}
		raw, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			return nil, err
		}
		items, err := decodeCompletedBatchReceipt(name, raw)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			if err := mergeCompletedReceipt(result, item); err != nil {
				return nil, err
			}
		}
	}
	seen := map[string]bool{}
	for _, journalDir := range journalDirs {
		if journalDir == "" || seen[journalDir] {
			continue
		}
		seen[journalDir] = true
		files, err := CompletedMigrationGenerationFiles(journalDir)
		if err != nil {
			return nil, err
		}
		for _, path := range files {
			s, err := readConfirmGeneration(path)
			if err != nil {
				return nil, err
			}
			for _, confirmation := range s.Confirmations {
				item := confirmation.Receipt
				if old, ok := result[item.ID]; ok {
					a, _ := json.Marshal(old)
					b, _ := json.Marshal(item)
					if !bytes.Equal(a, b) {
						return nil, fmt.Errorf("legacy/joint confirmation conflict %s", item.ID)
					}
				}
				result[item.ID] = item
			}
		}
	}
	return result, nil
}

// Both the stateless audit and the in-process index use this same validator.
func decodeCompletedBatchReceipt(name string, raw []byte) ([]completedBatchReceiptItem, error) {
	if len(raw) == 0 || len(raw) > (8<<20)+1 || raw[len(raw)-1] != '\n' ||
		filepath.Base(completedBatchReceiptPath("", bytes.TrimSuffix(raw, []byte{'\n'}))) != name {
		return nil, fmt.Errorf("completed batch receipt identity mismatch %s", name)
	}
	var batch completedBatchReceipt
	if err := json.Unmarshal(raw, &batch); err != nil {
		return nil, err
	}
	if batch.Schema != completedBatchReceiptSchema || len(batch.Items) == 0 {
		return nil, fmt.Errorf("invalid completed batch receipt %s", name)
	}
	for _, item := range batch.Items {
		if item.ID == "" || item.ID != item.Ack.ID {
			return nil, fmt.Errorf("completed batch receipt ID mismatch %s", name)
		}
	}
	return batch.Items, nil
}
