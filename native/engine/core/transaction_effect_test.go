package core

import (
	"bytes"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"
)

func effectFixture() *Transaction {
	tx := NewTransaction(strings.Repeat("1", 40), strings.Repeat("2", 40), big.NewInt(7), 9, time.Unix(10, 0))
	tx.AdmissionID, tx.IngressSequence = "run/9", 9
	return tx
}

func TestTransactionEffectIgnoresExecutionPlacement(t *testing.T) {
	tx := effectFixture()
	before := tx.Encode()
	credit, err := BuildTransactionEffect(tx, TransactionCredit)
	if err != nil {
		t.Fatal(err)
	}
	other := DecodeTx(before)
	other.Relayed, other.RoutingFrozen = true, true
	other.MappingVersion, other.RoutingEpoch = 19, 4
	other.RoutedSenderShard, other.RoutedRecipientShard = 3, 1
	other.SenderRequiresExistingState, other.RecipientRequiresExistingState = true, true
	other.Sender, other.Recipient = "0x"+strings.ToUpper(other.Sender), "0x"+other.Recipient
	after, err := BuildTransactionEffect(other, TransactionCredit)
	if err != nil || credit != after {
		t.Fatalf("placement changed effect: %v", err)
	}
	debit, err := BuildTransactionEffect(tx, TransactionDebit)
	if err != nil || debit.ID == credit.ID || debit.InstanceID != credit.InstanceID {
		t.Fatal("debit and credit identity not separated", err)
	}
	if !bytes.Equal(tx.Encode(), before) {
		t.Fatal("effect builder mutated transaction")
	}
}

func TestTransactionEffectBindsPayloadSeparately(t *testing.T) {
	tx := effectFixture()
	original, _ := BuildTransactionEffect(tx, TransactionCredit)
	for _, mutate := range []func(*Transaction){
		func(tx *Transaction) { tx.Value = big.NewInt(8) },
		func(tx *Transaction) { tx.Sender = strings.Repeat("3", 40) },
		func(tx *Transaction) { tx.Recipient = strings.Repeat("3", 40) },
		func(tx *Transaction) { tx.Nonce++ },
		func(tx *Transaction) { tx.IngressSequence++ },
		func(tx *Transaction) { tx.Time = tx.Time.Add(time.Second) },
	} {
		other := DecodeTx(tx.Encode())
		mutate(other)
		effect, err := BuildTransactionEffect(other, TransactionCredit)
		if err != nil || effect.ID != original.ID || effect.PayloadHash == original.PayloadHash {
			t.Fatal("payload mutation escaped stable identity", err)
		}
	}
	other := DecodeTx(tx.Encode())
	other.AdmissionID = "run/10"
	effect, _ := BuildTransactionEffect(other, TransactionCredit)
	if effect.ID == original.ID {
		t.Fatal("different admissions collapsed")
	}
}

func TestTransactionEffectRejectsLegacyAndUnsupportedIdentity(t *testing.T) {
	tx := effectFixture()
	tx.AdmissionID = ""
	if _, err := BuildTransactionEffect(tx, TransactionCredit); !errors.Is(err, ErrTransactionEffectIdentityMissing) {
		t.Fatal("legacy transaction inferred as identified", err)
	}
	tx = effectFixture()
	tx.HasBroker = true
	if _, err := BuildTransactionEffect(tx, TransactionCredit); err == nil {
		t.Fatal("broker accepted without effect protocol")
	}
}

func TestTransactionEffectZeroIngressRequiresExplicitAssignment(t *testing.T) {
	tx := effectFixture()
	tx.AdmissionID, tx.IngressSequence = "run/0", 0
	if _, err := BuildTransactionEffect(tx, TransactionDebit); !errors.Is(err, ErrTransactionEffectIdentityMissing) {
		t.Fatal("default legacy zero inferred as assigned", err)
	}
	tx.IngressSequenceAssigned = true
	original, err := BuildTransactionEffect(tx, TransactionCredit)
	if err != nil {
		t.Fatal("explicit first ingress rejected", err)
	}
	other := DecodeTx(tx.Encode())
	other.Relayed, other.MappingVersion = true, 8
	if !other.IngressSequenceAssigned || other.IngressSequence != 0 {
		t.Fatal("transport lost explicit zero ingress")
	}
	if effect, err := BuildTransactionEffect(other, TransactionCredit); err != nil || effect != original {
		t.Fatal("relay changed first admission effect", err)
	}
	other.AdmissionID = ""
	if _, err := BuildTransactionEffect(other, TransactionCredit); !errors.Is(err, ErrTransactionEffectIdentityMissing) {
		t.Fatal("assignment flag replaced admission identity", err)
	}
}
