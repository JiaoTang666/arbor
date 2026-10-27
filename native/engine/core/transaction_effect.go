package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"
)

const (
	TransactionDebit  = "debit"
	TransactionCredit = "credit"
)

// Legacy transactions are not evidence of an unseen effect. In particular,
// neither transaction nonce nor account nonce identifies an admission event.
var ErrTransactionEffectIdentityMissing = errors.New("transaction effect admission identity missing")

type TransactionEffect struct {
	ID              string `json:"id"`
	InstanceID      string `json:"instance_id"`
	AdmissionID     string `json:"admission_id"`
	TxHash          string `json:"tx_hash"`
	IngressSequence uint64 `json:"ingress_sequence"`
	Account         string `json:"account"`
	Effect          string `json:"effect"`
	PayloadHash     string `json:"payload_hash"`
}

func effectDigest(v interface{}) string {
	encoded, _ := json.Marshal(v) // All callers use JSON-safe concrete values.
	h := sha256.Sum256(encoded)
	return hex.EncodeToString(h[:])
}

// validateTransactionEffectInput is shared by admission and effect construction;
// both boundaries must enforce the same checks in the same order.
func validateTransactionEffectInput(tx *Transaction, effect string) (sender, recipient, account string, err error) {
	if tx == nil {
		return "", "", "", fmt.Errorf("nil transaction effect")
	}
	if tx.AdmissionID == "" || (tx.IngressSequence == 0 && !tx.IngressSequenceAssigned) || len(tx.TxHash) == 0 {
		return "", "", "", ErrTransactionEffectIdentityMissing
	}
	if !utf8.ValidString(tx.AdmissionID) || len(tx.TxHash) != sha256.Size || tx.Value == nil || tx.Value.Sign() < 0 {
		return "", "", "", fmt.Errorf("invalid transaction effect payload")
	}
	if tx.HasBroker || tx.SenderIsBroker || tx.OriginalSender != "" || tx.FinalRecipient != "" {
		return "", "", "", fmt.Errorf("broker transaction requires a separate effect protocol")
	}
	sender, recipient = canonicalPrepareAccount(tx.Sender), canonicalPrepareAccount(tx.Recipient)
	for _, account := range []string{sender, recipient} {
		address, err := hex.DecodeString(account)
		if err != nil || len(address) != 20 {
			return "", "", "", fmt.Errorf("invalid transaction effect account")
		}
	}
	account = sender
	switch effect {
	case TransactionDebit:
	case TransactionCredit:
		account = recipient
	default:
		return "", "", "", fmt.Errorf("unknown transaction effect %q", effect)
	}
	return sender, recipient, account, nil
}

// ValidateTransactionEffectInput checks an admission without constructing effect
// identities or payload hashes that the producer does not use.
func ValidateTransactionEffectInput(tx *Transaction, effect string) error {
	_, _, _, err := validateTransactionEffectInput(tx, effect)
	return err
}

// BuildTransactionEffect separates an admission's identity from its payload.
// Changing a route, relay stage or migration generation does not create a new
// debit/credit. The ledger must also bind AdmissionID to TxHash and PayloadHash;
// otherwise a conflicting hash could create a second instance for one admission.
func BuildTransactionEffect(tx *Transaction, effect string) (TransactionEffect, error) {
	sender, recipient, account, err := validateTransactionEffectInput(tx, effect)
	if err != nil {
		return TransactionEffect{}, err
	}
	hash := hex.EncodeToString(tx.TxHash)
	instance := effectDigest([]string{"transaction-instance-v1", tx.AdmissionID, hash})
	payload := struct {
		Sender, Recipient, Value, Time string
		Nonce, IngressSequence         uint64
	}{sender, recipient, tx.Value.String(), tx.Time.UTC().Format(time.RFC3339Nano), tx.Nonce, tx.IngressSequence}
	return TransactionEffect{
		ID: effectDigest([]string{"transaction-effect-v1", instance, effect}), InstanceID: instance,
		AdmissionID: tx.AdmissionID, TxHash: hash, IngressSequence: tx.IngressSequence,
		Account: account, Effect: effect, PayloadHash: effectDigest(payload),
	}, nil
}
