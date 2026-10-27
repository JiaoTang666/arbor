package core

import (
	"bytes"
	"math/big"
	"testing"
	"time"
)

func TestQueueObservationPreservesQueueAndLegResponsibility(t *testing.T) {
	a, b := "0000000000000000000000000000000000000010", "0000000000000000000000000000000000000020"
	p := NewTxPool()
	tx := NewTransaction(a, b, big.NewInt(7), 2, time.Unix(1, 0))
	tx.IngressSequence = 11
	tx.RoutingFrozen = true
	tx.RoutedSenderShard = 0
	tx.RoutedRecipientShard = 1
	credit := DecodeTx(tx.Encode())
	credit.Relayed = true
	credit.IngressSequence = 12
	p.AddTxs2Pool([]*Transaction{tx, credit})
	p.AddRelayTx(tx, 1)
	p.HoldSourceAccount(a)
	beforeA, beforeB := tx.Encode(), credit.Encode()
	rows, relays := p.SnapshotPendingQueueObservation()
	if len(rows) != 2 || relays != 1 || rows[0].QueueIndex != 0 || rows[1].QueueIndex != 1 || rows[0].IngressSequence != 11 || rows[1].IngressSequence != 12 {
		t.Fatal("snapshot occurrence/order mismatch")
	}
	if !rows[0].Held || rows[1].Held || rows[0].Relayed || !rows[1].Relayed {
		t.Fatal("relay credit responsibility misclassified")
	}
	if p.GetTxQueueLen() != 2 || !bytes.Equal(beforeA, tx.Encode()) || !bytes.Equal(beforeB, credit.Encode()) {
		t.Fatal("read-only snapshot mutated input")
	}
	rows[0].Sender = b
	if tx.Sender != a {
		t.Fatal("snapshot aliases mutable queue")
	}
}

func TestQueueObservationHeldMatchesActualPacking(t *testing.T) {
	a, b := "0000000000000000000000000000000000000010", "0000000000000000000000000000000000000020"
	p := NewTxPool()
	inbound := NewTransaction(b, a, big.NewInt(7), 2, time.Unix(1, 0))
	inbound.RoutingFrozen = true
	inbound.RoutedSenderShard, inbound.RoutedRecipientShard = 0, 1
	credit := DecodeTx(inbound.Encode())
	credit.Relayed = true
	local := DecodeTx(inbound.Encode())
	local.RoutedRecipientShard = 0
	p.AddTxs2Pool([]*Transaction{inbound, credit, local})
	p.HoldSourceAccounts([]string{a})
	rows, _ := p.SnapshotPendingQueueObservation()
	if len(rows) != 3 || rows[0].Held || !rows[1].Held || !rows[2].Held {
		t.Fatal("observer confused contact with an actual migration hold")
	}
	if got := p.PackTxs(100); len(got) != 1 || got[0] != inbound {
		t.Fatal("observed hold disagrees with actual eligible execution")
	}
}
