package core

import (
	"math/big"
	"testing"
	"time"
)

func TestCLPAEpochQueueCopiesAndCommitsOnlyMatchingLeg(t *testing.T) {
	p := NewTxPool()
	first := NewTransaction("sender", "recipient", big.NewInt(1), 0, time.Unix(1, 0))
	first.AdmissionID = "same"
	second := DecodeTx(first.Encode())
	second.Relayed = true
	if err := p.ReplaceCLPAEpochPending([]*Transaction{first, second}); err != nil {
		t.Fatal(err)
	}
	first.Value.SetInt64(77)
	snapshot, err := p.SnapshotCLPAEpochPending()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot[0].Value.Int64() != 1 {
		t.Fatal("queue aliased install payload")
	}
	snapshot[0].Value.SetInt64(88)
	p.RemoveCLPAEpochCommitted([]*Transaction{first})
	left, err := p.SnapshotCLPAEpochPending()
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 1 || !left[0].Relayed || left[0].Value.Int64() != 1 {
		t.Fatal("commit lost or changed pending relay second leg")
	}
}

func TestCLPAEpochQueueRejectsUnsentRelayAndSerialPool(t *testing.T) {
	p := NewTxPool()
	tx := NewTransaction("sender", "recipient", big.NewInt(1), 0, time.Unix(1, 0))
	p.AddRelayTx(tx, 1)
	if _, err := p.SnapshotCLPAEpochPending(); err == nil {
		t.Fatal("snapshot ignored unsent relay pool")
	}
	p.ClearRelayPool()
	p.HoldSourceAccount("sender")
	if err := p.ReplaceCLPAEpochPending(nil); err == nil {
		t.Fatal("epoch overwrote serial held queue")
	}
}

func TestCLPAEpochDeltaPreservesRetainedObjectsAndRejectsChangedSource(t *testing.T) {
	p := NewTxPool()
	keep := NewTransaction("sender", "recipient", big.NewInt(1), 0, time.Unix(1, 0))
	keep.AdmissionID = "retained"
	exit := DecodeTx(keep.Encode())
	exit.AdmissionID = "exiting"
	arrive := DecodeTx(keep.Encode())
	arrive.AdmissionID = "incoming"
	p.AddTxs2Pool([]*Transaction{keep, exit})
	source, err := p.SnapshotCLPAEpochPendingReferences()
	if err != nil || source[0] != keep || source[1] != exit {
		t.Fatalf("source snapshot copied retained payload: %v", err)
	}
	if err := p.InstallCLPAEpochPendingDelta(source, []*Transaction{exit, keep}, nil); err == nil {
		t.Fatal("retained queue was reordered")
	}
	if err := p.InstallCLPAEpochPendingDelta(source, []*Transaction{keep}, []*Transaction{arrive}); err != nil {
		t.Fatal(err)
	}
	if p.TxQueue[0] != keep || p.TxQueue[1] == arrive || p.TxQueue[1].AdmissionID != "incoming" {
		t.Fatal("delta install did not preserve retained object and clone moved payload")
	}
	arrive.Value.SetInt64(9)
	if p.TxQueue[1].Value.Int64() != 1 {
		t.Fatal("incoming pool payload aliased the consensus request")
	}
	before := append([]*Transaction(nil), p.TxQueue...)
	if err := p.InstallCLPAEpochPendingDelta(source, []*Transaction{keep}, nil); err == nil {
		t.Fatal("stale source view overwrote newly installed work")
	}
	if len(p.TxQueue) != len(before) || p.TxQueue[0] != before[0] || p.TxQueue[1] != before[1] {
		t.Fatal("rejected stale delta mutated the queue")
	}
}
