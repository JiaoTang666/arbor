package core

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// Recovery and read-only audit use the same full/delta semantics.
func replayMigrationGeneration(recovered []*Transaction, s migrationQueueSnapshot, directory ...string) ([]*Transaction, error) {
	if s.Kind == confirmRemoveKind {
		keys, err := validateConfirmGeneration(s)
		if err != nil {
			return nil, err
		}
		return removeDurableOccurrences(recovered, keys)
	}
	if err := rejectForeignConfirmationFields(s); err != nil {
		return nil, err
	}
	var batch []*Transaction
	for _, raw := range s.Transactions {
		var tx Transaction
		if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&tx); err != nil {
			return nil, err
		}
		if !journalTouches(&tx, s.Account, s.Accounts) {
			return nil, fmt.Errorf("journal contains unrelated transaction")
		}
		batch = append(batch, &tx)
	}
	switch s.Kind {
	case "", "full", "extend":
		recovered = batch
	case "append":
		if len(batch) == 0 {
			return nil, fmt.Errorf("empty append generation")
		}
		recovered = append(recovered, batch...)
	case "prepend":
		if len(batch) == 0 {
			return nil, fmt.Errorf("empty prepend generation")
		}
		recovered = append(batch, recovered...)
	case "remove":
		if len(batch) == 0 {
			return nil, fmt.Errorf("empty removal generation")
		}
		keys := map[string]bool{}
		for _, tx := range batch {
			// Compare decoded, locally re-encoded records: gob type IDs may
			// differ between the publishing and recovering processes.
			key := string(tx.Encode())
			if keys[key] {
				return nil, fmt.Errorf("duplicate removal identity")
			}
			keys[key] = true
		}
		return removeDurableOccurrences(recovered, keys)
	case sourceCreditRemoveKind:
		if len(directory) != 1 {
			return nil, fmt.Errorf("source credit removal requires journal directory")
		}
		return replaySourceCreditRemoval(recovered, s, directory[0])
	default:
		return nil, fmt.Errorf("unknown queue generation kind")
	}
	return recovered, nil
}

func removeDurableOccurrences(recovered []*Transaction, keys map[string]bool) ([]*Transaction, error) {
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
		if counts[key] != 1 {
			return nil, fmt.Errorf("removal requires exactly one durable occurrence")
		}
	}
	return kept, nil
}
