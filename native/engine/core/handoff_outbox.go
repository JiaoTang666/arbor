package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
)

func ValidateMigrationHandoff(h *MigrationHandoff) error {
	if h == nil {
		return fmt.Errorf("handoff required")
	}
	var original Transaction
	if err := gob.NewDecoder(bytes.NewReader(h.Original)).Decode(&original); err != nil {
		return err
	}
	expected, err := BuildMigrationHandoff(&original, h.ProposalID, h.Account, h.SourceShard, h.TargetShard, h.MappingVersion)
	if len(h.GroupAccounts) > 0 {
		expected, err = BuildGroupMigrationHandoff(&original, h.ProposalID, h.GroupAccounts, h.SourceShard, h.TargetShard, h.MappingVersion)
	}
	if err != nil {
		return err
	}
	// Gob type IDs depend on the process's encoder registration history.
	// Verify the ID against the ORIGINAL wire bytes, never a local re-encoding.
	// Then compare every decoded execution field against the permitted route
	// transformation. ACKs still hash the exact execution wire payload.
	fingerprint := sha256.Sum256(append([]byte(fmt.Sprintf("%s|%s|%d|%s|", h.ProposalID, h.Account, h.MappingVersion, h.Leg)), h.Original...))
	if len(h.GroupAccounts) > 0 {
		fingerprint = groupHandoffFingerprint(h)
	}
	var execution, permitted Transaction
	if err := gob.NewDecoder(bytes.NewReader(h.Execution)).Decode(&execution); err != nil {
		return err
	}
	if err := gob.NewDecoder(bytes.NewReader(expected.Execution)).Decode(&permitted); err != nil {
		return err
	}
	if hex.EncodeToString(fingerprint[:]) != h.ID || expected.Leg != h.Leg || expected.Account != h.Account || !reflect.DeepEqual(execution, permitted) {
		return fmt.Errorf("handoff content or execution placement mismatch")
	}
	return nil
}

// Persist before sending; never removes source queue entries. The directory
// is provisioned by the runner, not inferred from proposal-controlled text.
func PersistMigrationHandoff(directory string, h *MigrationHandoff) error {
	if err := ValidateMigrationHandoff(h); err != nil {
		return err
	}
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	return PublishSourcePrepareFile(filepath.Join(directory, h.ID+".handoff.json"), append(data, '\n'))
}
func LoadMigrationOutbox(directory string) ([]*MigrationHandoff, error) {
	entries, err := LoadMigrationArtifacts(directory, ".handoff.json", false)
	if err != nil {
		return nil, err
	}
	var records []*MigrationHandoff
	for _, id := range entries.IDs() {
		raw := entries[id]
		var h MigrationHandoff
		if err = json.Unmarshal(raw, &h); err != nil {
			return nil, err
		}
		if err = ValidateMigrationHandoff(&h); err != nil {
			return nil, err
		}
		records = append(records, &h)
	}
	return records, nil
}

// Snapshot under the pool lock; persistence and networking happen afterwards.
// Originals remain queued and held until an independently verified ACK.
func (p *TxPool) SnapshotMigrationTransactions(account string) []*Transaction {
	p.lock.Lock()
	defer p.lock.Unlock()
	var result []*Transaction
	for _, tx := range p.migrationPendingLocked() {
		if TransactionTouchesAccount(tx, account) {
			result = append(result, DecodeTx(tx.Encode()))
		}
	}
	return result
}
