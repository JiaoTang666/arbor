package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Group identity is independent of the member used to discover a transaction.
// Each original leg has one owner: sender for debit, recipient for credit.
func BuildGroupMigrationHandoff(tx *Transaction, proposal string, accounts []string, source, target, version uint64) (*MigrationHandoff, error) {
	if len(accounts) < MinMigrationGroupMembers || len(accounts) > MaxMigrationGroupMembers || tx == nil {
		return nil, fmt.Errorf("invalid group handoff")
	}
	member := map[string]bool{}
	for i, a := range accounts {
		decoded, e := hex.DecodeString(a)
		if e != nil || len(decoded) != 20 || canonicalPrepareAccount(a) != a || (i > 0 && accounts[i-1] >= a) {
			return nil, fmt.Errorf("group members must be canonical sorted addresses")
		}
		member[a] = true
	}
	owner := canonicalPrepareAccount(tx.Sender)
	if tx.Relayed {
		owner = canonicalPrepareAccount(tx.Recipient)
	}
	if !member[owner] {
		return nil, fmt.Errorf("group inbound requires committed source debit first")
	}
	h, err := BuildMigrationHandoff(tx, proposal, owner, source, target, version)
	if err != nil {
		return nil, err
	}
	h.GroupAccounts = append([]string(nil), accounts...)
	execution := DecodeTx(h.Execution)
	if member[canonicalPrepareAccount(tx.Recipient)] {
		if tx.RoutedRecipientShard != source {
			return nil, fmt.Errorf("group recipient original route mismatch")
		}
		execution.RoutedRecipientShard = target
		execution.RecipientRequiresExistingState = true
	}
	h.Execution = execution.Encode()
	hash := groupHandoffFingerprint(h)
	h.ID = hex.EncodeToString(hash[:])
	return h, nil
}

func groupHandoffFingerprint(h *MigrationHandoff) [32]byte {
	identity, _ := json.Marshal(struct {
		Domain, Proposal        string
		Source, Target, Version uint64
		Leg                     string
		Accounts                []string
	}{"group-handoff-v1", h.ProposalID, h.SourceShard, h.TargetShard, h.MappingVersion, h.Leg, h.GroupAccounts})
	return sha256.Sum256(append(append(identity, '\n'), h.Original...))
}
