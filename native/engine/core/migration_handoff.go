package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Retain the original ingress record separately from its execution placement.
// This transport envelope alone is NOT authorization to execute or dequeue.
type MigrationHandoff struct {
	GroupAccounts  []string `json:",omitempty"`
	SourceShard    uint64
	TargetShard    uint64
	ProposalID     string
	Account        string
	MappingVersion uint64
	Original       []byte
	Execution      []byte
	Leg            string
	ID             string
}

func BuildMigrationHandoff(tx *Transaction, proposal, account string, source, target, version uint64) (*MigrationHandoff, error) {
	if tx == nil || proposal == "" || source == target || version == 0 || !tx.RoutingFrozen || tx.Value == nil || len(tx.TxHash) != 32 {
		return nil, fmt.Errorf("handoff requires identified frozen transaction and migration")
	}
	if tx.HasBroker || tx.SenderIsBroker || tx.OriginalSender != "" || tx.FinalRecipient != "" {
		return nil, fmt.Errorf("broker handoff requires separate protocol")
	}
	account = canonicalPrepareAccount(account)
	original := tx.Encode()
	copyTx := DecodeTx(original)
	leg := ""
	if tx.Relayed {
		if canonicalPrepareAccount(tx.Recipient) != account || tx.RoutedRecipientShard != source {
			return nil, fmt.Errorf("relay is not a source-owned credit leg")
		}
		copyTx.RoutedRecipientShard = target
		copyTx.RecipientRequiresExistingState = true
		leg = "credit"
	} else {
		if canonicalPrepareAccount(tx.Sender) != account || tx.RoutedSenderShard != source {
			return nil, fmt.Errorf("direct inbound transaction requires source debit before credit handoff")
		}
		copyTx.RoutedSenderShard = target
		copyTx.SenderRequiresExistingState = true
		leg = "debit"
		if canonicalPrepareAccount(tx.Recipient) == account {
			copyTx.RoutedRecipientShard = target
			copyTx.RecipientRequiresExistingState = true
		}
	}
	// MappingVersion and frozen original routes remain available in Original.
	// Execution uses the authority-approved placement without changing tx identity.
	copyTx.MappingVersion = version
	if !bytes.Equal(copyTx.TxHash, tx.TxHash) || copyTx.Relayed != tx.Relayed {
		return nil, fmt.Errorf("handoff changed transaction leg identity")
	}
	fingerprint := sha256.Sum256(append([]byte(fmt.Sprintf("%s|%s|%d|%s|", proposal, account, version, leg)), original...))
	return &MigrationHandoff{SourceShard: source, TargetShard: target, ProposalID: proposal, Account: account, MappingVersion: version, Original: original, Execution: copyTx.Encode(), Leg: leg, ID: hex.EncodeToString(fingerprint[:])}, nil
}
