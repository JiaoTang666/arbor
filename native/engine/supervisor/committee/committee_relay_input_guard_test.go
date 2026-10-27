package committee

import (
	"strings"
	"testing"
)

func nativeInputRow(sender, recipient, value string) []string {
	return []string{
		"1", "2", "0xhash", sender, recipient, "None", "0", "0", value,
		"21000", "0", "21000", "0x", "None", "None", "None", "None", "None",
	}
}

func TestData2TxRejectsUnsafeInputWithoutPanicking(t *testing.T) {
	sender := "0x1111111111111111111111111111111111111111"
	recipient := "0x2222222222222222222222222222222222222222"
	tests := []struct {
		name string
		row  []string
	}{
		{name: "short row", row: []string{"short"}},
		{name: "invalid integer", row: nativeInputRow(sender, recipient, "invalid")},
		{name: "negative value", row: nativeInputRow(sender, recipient, "-1")},
		{name: "malformed sender", row: nativeInputRow("0x1234", recipient, "1")},
		{name: "same account case insensitive", row: nativeInputRow(sender, strings.ToUpper(sender), "1")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, ok := data2tx(test.row, 0); ok {
				t.Fatal("unsafe row was accepted")
			}
		})
	}
}

func TestData2TxAcceptsZeroValue(t *testing.T) {
	row := nativeInputRow(
		"0x1111111111111111111111111111111111111111",
		"0x2222222222222222222222222222222222222222",
		"0",
	)
	tx, ok := data2tx(row, 8)
	if !ok {
		t.Fatal("zero-value EOA transaction was rejected")
	}
	if tx.Nonce != 8 || tx.Value.Sign() != 0 {
		t.Fatalf("unexpected transaction: nonce=%d value=%s", tx.Nonce, tx.Value)
	}
}

func TestData2TxAcceptsPositiveEOATransfer(t *testing.T) {
	row := nativeInputRow(
		"0x1111111111111111111111111111111111111111",
		"0x2222222222222222222222222222222222222222",
		"1",
	)
	tx, ok := data2tx(row, 7)
	if !ok {
		t.Fatal("valid row was rejected")
	}
	if tx.Nonce != 7 || tx.Value.Sign() != 1 {
		t.Fatalf("unexpected transaction: nonce=%d value=%s", tx.Nonce, tx.Value)
	}
}
