package committee

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"bytes"
	"math/big"
	"strings"
	"testing"
)

func TestNativeEffectValidationRejectsWholeBatchBeforeAssignment(t *testing.T) {
	t.Setenv(chain.NativeEffectIndexEnv, "true")
	t.Setenv("SAA_NATIVE_INPUT_SHA256", strings.Repeat("a", 64))
	t.Setenv("SAA_FOREST_INPUT_SHA256", "")
	for _, test := range []struct {
		name   string
		mutate func(*core.Transaction)
	}{
		{"short_hash", func(tx *core.Transaction) { tx.TxHash = tx.TxHash[:31] }},
		{"negative_value", func(tx *core.Transaction) { tx.Value = big.NewInt(-1) }},
		{"broker", func(tx *core.Transaction) { tx.HasBroker = true }},
		{"invalid_account", func(tx *core.Transaction) { tx.Recipient = "invalid" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := &RelayCommitteeModule{nativeBatch: true}
			first, invalid := nativeIngressTx(0), nativeIngressTx(1)
			test.mutate(invalid)
			beforeFirst, beforeInvalid := first.Encode(), invalid.Encode()
			if err := m.bindNativeEffectAdmissions([]*core.Transaction{first, invalid}); err == nil {
				t.Fatal("invalid effect input admitted")
			}
			if !bytes.Equal(first.Encode(), beforeFirst) || !bytes.Equal(invalid.Encode(), beforeInvalid) || m.nativeInputHash != "" {
				t.Fatal("invalid batch mutated a transaction or published its input identity")
			}
			if err := m.bindNativeEffectAdmissions([]*core.Transaction{first}); err != nil {
				t.Fatal("failed batch prevented the original valid occurrence from being admitted", err)
			}
		})
	}
}
