package core

import (
	"bytes"
	"math/big"
	"testing"
	"time"
)

func TestMigrationHandoffPreservesOriginalAndLeg(t *testing.T) {
	a := "0000000000000000000000000000000000000010"
	b := "0000000000000000000000000000000000000011"
	for _, relay := range []bool{false, true} {
		tx := NewTransaction(a, b, big.NewInt(7), 1, time.Unix(1, 0))
		tx.RoutingFrozen = true
		tx.RoutedSenderShard = 0
		tx.RoutedRecipientShard = 1
		if relay {
			tx.Sender = b
			tx.Recipient = a
			tx.Relayed = true
			tx.RoutedSenderShard = 1
			tx.RoutedRecipientShard = 0
		}
		original := tx.Encode()
		h, err := BuildMigrationHandoff(tx, "p", a, 0, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(original, tx.Encode()) || !bytes.Equal(original, h.Original) {
			t.Fatal("original mutated")
		}
		execution := DecodeTx(h.Execution)
		if execution.Relayed != relay || !bytes.Equal(execution.TxHash, tx.TxHash) {
			t.Fatal("leg changed")
		}
		if relay && execution.RoutedRecipientShard != 1 || !relay && execution.RoutedSenderShard != 1 {
			t.Fatal("execution owner not moved")
		}
		again, err := BuildMigrationHandoff(tx, "p", a, 0, 1, 1)
		if err != nil || again.ID != h.ID {
			t.Fatal("handoff identity unstable")
		}
	}
	inbound := NewTransaction(b, a, big.NewInt(7), 2, time.Unix(1, 0))
	inbound.RoutingFrozen = true
	if _, err := BuildMigrationHandoff(inbound, "p", a, 0, 1, 1); err == nil {
		t.Fatal("unexecuted inbound debit was silently moved")
	}
}
