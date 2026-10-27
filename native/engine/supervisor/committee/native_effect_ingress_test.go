package committee

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"bytes"
	"math/big"
	"strings"
	"testing"
	"time"
)

func nativeIngressTx(sequence uint64) *core.Transaction {
	tx := core.NewTransaction(strings.Repeat("1", 40), strings.Repeat("2", 40), big.NewInt(7), sequence, time.Unix(1, 0))
	tx.IngressSequence = sequence
	return tx
}

func TestNativeEffectIngressPreservesZeroBasedBatchAndRetransmission(t *testing.T) {
	t.Setenv(chain.NativeEffectIndexEnv, "true")
	t.Setenv("SAA_NATIVE_INPUT_SHA256", strings.Repeat("a", 64))
	t.Setenv("SAA_FOREST_INPUT_SHA256", strings.Repeat("a", 64))
	m := &RelayCommitteeModule{nativeBatch: true}
	first, second := nativeIngressTx(0), nativeIngressTx(1)
	hash := append([]byte(nil), first.TxHash...)
	if err := m.bindNativeEffectAdmissions([]*core.Transaction{first, second}); err != nil {
		t.Fatal(err)
	}
	for i, tx := range []*core.Transaction{first, second} {
		if tx.IngressSequence != uint64(m.nativeCount+i) || !tx.IngressSequenceAssigned || tx.AdmissionID == "" {
			t.Fatal("source batch was shifted or unidentified", i)
		}
		if _, err := core.BuildTransactionEffect(tx, core.TransactionDebit); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(first.TxHash, hash) || first.AdmissionID == second.AdmissionID {
		t.Fatal("original hash or occurrence identity changed")
	}
	retry := core.DecodeTx(first.Encode())
	before := retry.Encode()
	if err := m.bindNativeEffectAdmissions([]*core.Transaction{retry}); err != nil || !bytes.Equal(before, retry.Encode()) {
		t.Fatal("retransmission received a new identity", err)
	}
	m.nativeCount += 2
	next := nativeIngressTx(2)
	if err := m.bindNativeEffectAdmissions([]*core.Transaction{next}); err != nil || next.IngressSequence != uint64(m.nativeCount) {
		t.Fatal("next batch is not contiguous and zero-based", err)
	}
}

func TestNativeEffectIngressRejectsConflictingBindingsBeforeAssigningBatch(t *testing.T) {
	t.Setenv(chain.NativeEffectIndexEnv, "true")
	t.Setenv("SAA_NATIVE_INPUT_SHA256", strings.Repeat("a", 64))
	t.Setenv("SAA_FOREST_INPUT_SHA256", "")
	m := &RelayCommitteeModule{nativeBatch: true}
	first, conflict := nativeIngressTx(0), nativeIngressTx(1)
	conflict.AdmissionID = "another-input/1"
	if err := m.bindNativeEffectAdmissions([]*core.Transaction{first, conflict}); err == nil || first.AdmissionID != "" || first.IngressSequenceAssigned || m.nativeInputHash != "" {
		t.Fatal("invalid batch assigned a prefix", err)
	}
	if err := m.bindNativeEffectAdmissions([]*core.Transaction{first}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SAA_NATIVE_INPUT_SHA256", strings.Repeat("b", 64))
	if err := m.bindNativeEffectAdmissions([]*core.Transaction{nativeIngressTx(1)}); err == nil {
		t.Fatal("producer switched input identity")
	}
	t.Setenv("SAA_FOREST_INPUT_SHA256", strings.Repeat("a", 64))
	if _, err := nativeEffectInputHash(); err == nil {
		t.Fatal("generic and forest bindings disagree but were accepted")
	}
	t.Setenv("SAA_NATIVE_INPUT_SHA256", "invalid")
	t.Setenv("SAA_FOREST_INPUT_SHA256", "")
	if _, err := nativeEffectInputHash(); err == nil {
		t.Fatal("unbound input accepted")
	}
}

func TestNativeEffectIngressOffKeepsLegacyIdentity(t *testing.T) {
	t.Setenv(chain.NativeEffectIndexEnv, "false")
	t.Setenv("SAA_NATIVE_INPUT_SHA256", "")
	t.Setenv("SAA_FOREST_INPUT_SHA256", "")
	tx := nativeIngressTx(0)
	before := tx.Encode()
	m := &RelayCommitteeModule{nativeBatch: true}
	if err := m.bindNativeEffectAdmissions([]*core.Transaction{tx}); err != nil || !bytes.Equal(before, tx.Encode()) || m.nativeInputHash != "" {
		t.Fatal("disabled index changed original batch behavior", err)
	}
}
