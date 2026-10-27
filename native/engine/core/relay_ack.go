package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type HandoffRelayAck struct {
	Redirect       *RelayRedirectProof `json:",omitempty"`
	RelayPayload   []byte
	ID             string
	RelayHash      string
	RecipientShard uint64
	Block          uint64
	BlockHash      string
	StateRoot      string
	Committed      bool
}

func ValidateHandoffRelayAck(h *MigrationHandoff, a HandoffRelayAck) error {
	if err := ValidateMigrationHandoff(h); err != nil {
		return err
	}
	tx := DecodeTx(h.Execution)
	if tx.Relayed || tx.RoutedRecipientShard == h.TargetShard {
		return fmt.Errorf("handoff does not require forwarded relay")
	}
	if err := ValidateForwardedRelay(h, a.RelayPayload); err != nil {
		return err
	}
	hash := sha256.Sum256(a.RelayPayload)
	valid := func(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == 32 }
	if a.Redirect != nil {
		if err := ValidateRelayRedirectProof(h, a.RelayPayload, *a.Redirect); err != nil {
			return err
		}
		p := a.Redirect.TargetAck
		if a.ID != h.ID || a.RelayHash != hex.EncodeToString(hash[:]) || !a.Committed ||
			a.RecipientShard != p.TargetShard || a.Block != p.Block || a.BlockHash != p.BlockHash || a.StateRoot != p.StateRoot {
			return fmt.Errorf("redirect final acknowledgement mismatch")
		}
		return nil
	}
	if a.ID != h.ID || a.RelayHash != hex.EncodeToString(hash[:]) || a.RecipientShard != tx.RoutedRecipientShard || a.Block == 0 || !a.Committed || !valid(a.BlockHash) || !valid(a.StateRoot) {
		return fmt.Errorf("relay acknowledgement mismatch")
	}
	return nil
}

// Gob wire type IDs are process-dependent. Bind the received bytes and compare
// decoded fields rather than hashing a locally re-encoded transaction.
func ValidateForwardedRelay(h *MigrationHandoff, payload []byte) error {
	if err := ValidateMigrationHandoff(h); err != nil {
		return err
	}
	expected := DecodeTx(h.Execution)
	if expected.Relayed || expected.RoutedRecipientShard == h.TargetShard {
		return fmt.Errorf("handoff does not require forwarded relay")
	}
	expected.Relayed = true
	var actual Transaction
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&actual); err != nil {
		return fmt.Errorf("invalid relay payload: %w", err)
	}
	expected.Time = expected.Time.UTC()
	actual.Time = actual.Time.UTC()
	want, err := json.Marshal(expected)
	if err != nil {
		return err
	}
	got, err := json.Marshal(&actual)
	if err != nil {
		return err
	}
	if !bytes.Equal(want, got) {
		return fmt.Errorf("relay transaction fields mismatch")
	}
	return nil
}
