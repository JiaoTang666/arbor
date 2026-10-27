package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// This is the pre-extraction builder, retained as a compatibility oracle rather
// than calling either the shared validator or the production digest helper.
func legacyTransactionEffectReference(tx *Transaction, effect string) (TransactionEffect, error) {
	var result TransactionEffect
	if tx == nil {
		return result, fmt.Errorf("nil transaction effect")
	}
	if tx.AdmissionID == "" || (tx.IngressSequence == 0 && !tx.IngressSequenceAssigned) || len(tx.TxHash) == 0 {
		return result, ErrTransactionEffectIdentityMissing
	}
	if !utf8.ValidString(tx.AdmissionID) || len(tx.TxHash) != sha256.Size || tx.Value == nil || tx.Value.Sign() < 0 {
		return result, fmt.Errorf("invalid transaction effect payload")
	}
	if tx.HasBroker || tx.SenderIsBroker || tx.OriginalSender != "" || tx.FinalRecipient != "" {
		return result, fmt.Errorf("broker transaction requires a separate effect protocol")
	}
	sender, recipient := canonicalPrepareAccount(tx.Sender), canonicalPrepareAccount(tx.Recipient)
	for _, account := range []string{sender, recipient} {
		address, err := hex.DecodeString(account)
		if err != nil || len(address) != 20 {
			return result, fmt.Errorf("invalid transaction effect account")
		}
	}
	account := sender
	switch effect {
	case TransactionDebit:
	case TransactionCredit:
		account = recipient
	default:
		return result, fmt.Errorf("unknown transaction effect %q", effect)
	}
	digest := func(value interface{}) string {
		raw, _ := json.Marshal(value)
		hash := sha256.Sum256(raw)
		return hex.EncodeToString(hash[:])
	}
	hash := hex.EncodeToString(tx.TxHash)
	instance := digest([]string{"transaction-instance-v1", tx.AdmissionID, hash})
	payload := struct {
		Sender, Recipient, Value, Time string
		Nonce, IngressSequence         uint64
	}{sender, recipient, tx.Value.String(), tx.Time.UTC().Format(time.RFC3339Nano), tx.Nonce, tx.IngressSequence}
	return TransactionEffect{
		ID: digest([]string{"transaction-effect-v1", instance, effect}), InstanceID: instance,
		AdmissionID: tx.AdmissionID, TxHash: hash, IngressSequence: tx.IngressSequence,
		Account: account, Effect: effect, PayloadHash: digest(payload),
	}, nil
}

func validationEffectFixture() *Transaction {
	return &Transaction{Sender: strings.Repeat("1", 40), Recipient: strings.Repeat("2", 40),
		Nonce: 9, Value: big.NewInt(7), TxHash: bytes.Repeat([]byte{0xa5}, sha256.Size),
		Time: time.Unix(10, 0), AdmissionID: "run/9", IngressSequence: 9}
}

func cloneValidationEffectInput(tx *Transaction) *Transaction {
	if tx == nil {
		return nil
	}
	before := *tx
	before.TxHash = append([]byte(nil), tx.TxHash...)
	before.Signature = append([]byte(nil), tx.Signature...)
	before.RawTxHash = append([]byte(nil), tx.RawTxHash...)
	if tx.Value != nil {
		before.Value = new(big.Int).Set(tx.Value)
	}
	return &before
}

func TestTransactionEffectValidationMatchesLegacyErrorsAndOutput(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Transaction)
		effect  string
		nilTx   bool
		wantErr string
	}{
		{name: "debit"},
		{name: "credit", effect: TransactionCredit},
		{name: "canonical_aliases", mutate: func(tx *Transaction) {
			tx.Sender = " 0X" + strings.Repeat("A", 40) + "\t"
			tx.Recipient = "\n0x" + strings.Repeat("B", 40) + " "
		}},
		{name: "assigned_zero", mutate: func(tx *Transaction) {
			tx.AdmissionID, tx.IngressSequence, tx.IngressSequenceAssigned = "run/0", 0, true
		}},
		{name: "zero_value_and_time", mutate: func(tx *Transaction) {
			tx.Value, tx.Time = big.NewInt(0), time.Time{}
		}},
		{name: "fractional_non_utc_time", mutate: func(tx *Transaction) {
			tx.Time = time.Unix(10, 123456789).In(time.FixedZone("fixture", 8*60*60))
		}},
		{name: "execution_metadata", mutate: func(tx *Transaction) {
			tx.Relayed, tx.RoutingFrozen = true, true
			tx.MappingVersion, tx.RoutingEpoch = 17, 4
			tx.RoutedSenderShard, tx.RoutedRecipientShard = 3, 1
			tx.SenderRequiresExistingState, tx.RecipientRequiresExistingState = true, true
		}},
		{name: "nil", nilTx: true, wantErr: "nil transaction effect"},
		{name: "missing_admission", mutate: func(tx *Transaction) { tx.AdmissionID = "" }, wantErr: ErrTransactionEffectIdentityMissing.Error()},
		{name: "unassigned_zero", mutate: func(tx *Transaction) { tx.IngressSequence = 0 }, wantErr: ErrTransactionEffectIdentityMissing.Error()},
		{name: "missing_hash", mutate: func(tx *Transaction) { tx.TxHash = nil }, wantErr: ErrTransactionEffectIdentityMissing.Error()},
		{name: "short_hash", mutate: func(tx *Transaction) { tx.TxHash = tx.TxHash[:31] }, wantErr: "invalid transaction effect payload"},
		{name: "long_hash", mutate: func(tx *Transaction) { tx.TxHash = append(tx.TxHash, 0) }, wantErr: "invalid transaction effect payload"},
		{name: "invalid_utf8_admission", mutate: func(tx *Transaction) { tx.AdmissionID = string([]byte{0xff}) }, wantErr: "invalid transaction effect payload"},
		{name: "nil_value", mutate: func(tx *Transaction) { tx.Value = nil }, wantErr: "invalid transaction effect payload"},
		{name: "negative_value", mutate: func(tx *Transaction) { tx.Value = big.NewInt(-1) }, wantErr: "invalid transaction effect payload"},
		{name: "has_broker", mutate: func(tx *Transaction) { tx.HasBroker = true }, wantErr: "broker transaction requires a separate effect protocol"},
		{name: "sender_is_broker", mutate: func(tx *Transaction) { tx.SenderIsBroker = true }, wantErr: "broker transaction requires a separate effect protocol"},
		{name: "original_sender", mutate: func(tx *Transaction) { tx.OriginalSender = tx.Sender }, wantErr: "broker transaction requires a separate effect protocol"},
		{name: "final_recipient", mutate: func(tx *Transaction) { tx.FinalRecipient = tx.Recipient }, wantErr: "broker transaction requires a separate effect protocol"},
		{name: "invalid_sender", mutate: func(tx *Transaction) { tx.Sender = strings.Repeat("g", 40) }, wantErr: "invalid transaction effect account"},
		{name: "short_recipient", mutate: func(tx *Transaction) { tx.Recipient = strings.Repeat("2", 38) }, wantErr: "invalid transaction effect account"},
		{name: "unknown_effect", effect: "unsupported", wantErr: `unknown transaction effect "unsupported"`},
		{name: "identity_before_payload_broker_account_effect", effect: "unsupported", mutate: func(tx *Transaction) {
			tx.AdmissionID, tx.Value, tx.HasBroker, tx.Sender = "", nil, true, "invalid"
		}, wantErr: ErrTransactionEffectIdentityMissing.Error()},
		{name: "payload_before_broker_account_effect", effect: "unsupported", mutate: func(tx *Transaction) {
			tx.Value, tx.HasBroker, tx.Sender = nil, true, "invalid"
		}, wantErr: "invalid transaction effect payload"},
		{name: "broker_before_account_effect", effect: "unsupported", mutate: func(tx *Transaction) {
			tx.HasBroker, tx.Sender = true, "invalid"
		}, wantErr: "broker transaction requires a separate effect protocol"},
		{name: "account_before_effect", effect: "unsupported", mutate: func(tx *Transaction) { tx.Sender = "invalid" }, wantErr: "invalid transaction effect account"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			tx := validationEffectFixture()
			if test.nilTx {
				tx = nil
			} else if test.mutate != nil {
				test.mutate(tx)
			}
			effect := test.effect
			if effect == "" {
				effect = TransactionDebit
			}
			before := cloneValidationEffectInput(tx)
			want, legacyErr := legacyTransactionEffectReference(tx, effect)
			validationErr := ValidateTransactionEffectInput(tx, effect)
			got, buildErr := BuildTransactionEffect(tx, effect)
			for name, err := range map[string]error{"legacy": legacyErr, "validation": validationErr, "build": buildErr} {
				message := ""
				if err != nil {
					message = err.Error()
				}
				if message != test.wantErr || reflect.TypeOf(err) != reflect.TypeOf(legacyErr) ||
					errors.Is(err, ErrTransactionEffectIdentityMissing) != errors.Is(legacyErr, ErrTransactionEffectIdentityMissing) {
					t.Fatalf("%s changed error type, identity or priority: got %T %v; legacy %T %v; want %q", name, err, err, legacyErr, legacyErr, test.wantErr)
				}
			}
			if got != want {
				t.Fatalf("effect differs from legacy: got %+v; want %+v", got, want)
			}
			if !reflect.DeepEqual(tx, before) {
				t.Fatal("validation or construction mutated the input transaction")
			}
		})
	}
}

func TestTransactionEffectValidationKeepsLegacyHashVectors(t *testing.T) {
	tx := validationEffectFixture()
	want := TransactionEffect{
		InstanceID:  "05fdf35d0df6144ac5f7a760032359c022ad6a89170d102552a3394f2f1080c1",
		AdmissionID: "run/9", TxHash: strings.Repeat("a5", 32), IngressSequence: 9,
		PayloadHash: "27ad83408b1e658a73e8c75aafc2d8d50b730f80f4f65d2b90c710ae10bef570",
	}
	for _, effect := range []string{TransactionDebit, TransactionCredit} {
		want.Effect, want.Account = effect, tx.Sender
		want.ID = "68d471b8f54180e056f4ad976d4e5dda8e63c0374eb543801feaf19ec73c1965"
		if effect == TransactionCredit {
			want.Account = tx.Recipient
			want.ID = "50f09d24ed99515e3bd014b14553e06015283c3adea15a954ba811eb4763693f"
		}
		got, err := BuildTransactionEffect(tx, effect)
		if err != nil || got != want {
			t.Fatalf("%s changed the legacy hash vector: got %+v (%v); want %+v", effect, got, err, want)
		}
	}
}
