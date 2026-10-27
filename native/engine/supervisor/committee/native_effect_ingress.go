package committee

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func nativeEffectInputHash() (string, error) {
	input := strings.ToLower(strings.TrimSpace(os.Getenv("SAA_NATIVE_INPUT_SHA256")))
	forest := strings.ToLower(strings.TrimSpace(os.Getenv("SAA_FOREST_INPUT_SHA256")))
	if input != "" && forest != "" && input != forest {
		return "", fmt.Errorf("native and forest input identities disagree")
	}
	if input == "" {
		input = forest
	}
	raw, err := hex.DecodeString(input)
	if err != nil || len(raw) != 32 {
		return "", fmt.Errorf("indexed native batch requires a bound input SHA256")
	}
	return input, nil
}

// A fresh indexed ledger consumes each input once. Input hash plus accepted
// zero-based sequence identifies an occurrence, not its route or dispatch time.
// Retransmission reuses the original transaction; re-reading/re-timestamping a
// row is not a supported restart and cannot bypass ledger payload binding.
// Validate the whole batch before assigning identities or emitting acceptance.
func (m *RelayCommitteeModule) bindNativeEffectAdmissions(txs []*core.Transaction) error {
	if !m.nativeBatch {
		return nil
	}
	enabled, err := chain.NativeEffectIndexConfiguration()
	if err != nil || !enabled {
		return err
	}
	input, err := nativeEffectInputHash()
	if err != nil {
		return err
	}
	if m.nativeInputHash != "" && m.nativeInputHash != input {
		return fmt.Errorf("native input identity changed during admission")
	}
	for _, tx := range txs {
		if tx == nil || tx.Relayed {
			return fmt.Errorf("native batch admission requires an original first leg")
		}
		id := "native-input/" + input + "/" + strconv.FormatUint(tx.IngressSequence, 10)
		if tx.AdmissionID != "" && tx.AdmissionID != id {
			return fmt.Errorf("native ingress admission identity conflicts")
		}
		candidate := *tx
		candidate.AdmissionID, candidate.IngressSequenceAssigned = id, true
		if err := core.ValidateTransactionEffectInput(&candidate, core.TransactionDebit); err != nil {
			return err
		}
	}
	m.nativeInputHash = input
	for _, tx := range txs {
		tx.AdmissionID = "native-input/" + input + "/" + strconv.FormatUint(tx.IngressSequence, 10)
		tx.IngressSequenceAssigned = true
	}
	return nil
}
