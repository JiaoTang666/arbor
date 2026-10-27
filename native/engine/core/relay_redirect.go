package core

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Trusted-reporter evidence, not a portable Byzantine commit certificate.
// The source receipt reserves the original credit without applying it there.
type RelayRedirectProof struct {
	ParentID        string
	RelayHash       string
	Child           MigrationHandoff
	SourceShard     uint64
	SourceBlock     uint64
	SourceBlockHash string
	SourceStateRoot string
	SourceCommitted bool
	TargetAck       HandoffAck
}

func ValidateRelayRedirectLink(parent *MigrationHandoff, relay []byte, child *MigrationHandoff) error {
	if err := ValidateForwardedRelay(parent, relay); err != nil {
		return err
	}
	if err := ValidateMigrationHandoff(child); err != nil {
		return err
	}
	tx := DecodeTx(relay)
	if child.Leg != "credit" || len(child.GroupAccounts) == 0 || child.SourceShard != tx.RoutedRecipientShard ||
		child.Account != canonicalPrepareAccount(tx.Recipient) || child.MappingVersion <= parent.MappingVersion ||
		child.ProposalID == parent.ProposalID {
		return fmt.Errorf("redirect is not a later recipient credit bound to parent relay")
	}
	if err := ValidateForwardedRelay(parent, child.Original); err != nil {
		return err
	}
	return nil
}

func ValidateRelayRedirectProof(parent *MigrationHandoff, relay []byte, p RelayRedirectProof) error {
	if err := ValidateRelayRedirectLink(parent, relay, &p.Child); err != nil {
		return err
	}
	digest := sha256.Sum256(relay)
	valid := func(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == 32 }
	if p.ParentID != parent.ID || p.RelayHash != hex.EncodeToString(digest[:]) ||
		p.SourceShard != p.Child.SourceShard || p.SourceBlock == 0 || !p.SourceCommitted ||
		!valid(p.SourceBlockHash) || !valid(p.SourceStateRoot) {
		return fmt.Errorf("redirect source receipt mismatch")
	}
	if err := ValidateHandoffAck(&p.Child, p.TargetAck); err != nil {
		return err
	}
	if p.TargetAck.RelayPending {
		return fmt.Errorf("redirect credit cannot create a debit leg")
	}
	return nil
}
