package committee

import (
	"blockEmulator/core"
	"blockEmulator/firstplacement"
	"blockEmulator/initialledger"
	"fmt"
	"testing"
	"time"
)

func queueAddress(n int) string { return fmt.Sprintf("%040x", n) }
func queueTx(a, b int) *core.Transaction {
	return &core.Transaction{Sender: queueAddress(a), Recipient: queueAddress(b)}
}

func TestStreamQueueDisjointBypassAndTransitiveOrder(t *testing.T) {
	q := []*core.Transaction{queueTx(1, 2), queueTx(2, 3), queueTx(3, 4), queueTx(5, 6)}
	if i := streamReadyIndex(q, func(tx *core.Transaction) bool { return tx != q[0] }); i != 3 {
		t.Fatalf("want independent index 3, got %d", i)
	}
	if i := streamReadyIndex(q[:3], func(tx *core.Transaction) bool { return tx != q[0] }); i != -1 {
		t.Fatalf("shared endpoint overtook: %d", i)
	}
	if i := streamReadyIndex(q, func(tx *core.Transaction) bool { return true }); i != 0 {
		t.Fatal(i)
	}
}

func TestStreamQueueDependencyRetainsUncertainAndRelayFirstLeg(t *testing.T) {
	f := firstplacement.NewFence("queue-test")
	x, e := f.Admit(queueAddress(1), queueAddress(2), time.Now())
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Route(x.ID, 0, 1, time.Now()); e != nil {
		t.Fatal(e)
	}
	q := []*core.Transaction{queueTx(2, 3), queueTx(3, 4), queueTx(5, 6)}
	ready := func(tx *core.Transaction) bool { return f.Idle([]string{tx.Sender, tx.Recipient}) }
	f.MarkSend(x.ID, "uncertain")
	if i := streamReadyIndex(q, ready); i != 2 {
		t.Fatal(i)
	}
	if f.Outstanding() != 1 {
		t.Fatal("uncertain must consume capacity")
	}
	if e = f.Report(x.ID, x.Sender, x.Recipient, 0, 1, 0, "relay1", time.Now()); e != nil {
		t.Fatal(e)
	}
	if i := streamReadyIndex(q, ready); i != 2 {
		t.Fatal("source leg released dependency", i)
	}
	if e = f.Report(x.ID, x.Sender, x.Recipient, 0, 1, 1, "relay2", time.Now()); e != nil {
		t.Fatal(e)
	}
	if i := streamReadyIndex(q, ready); i != 0 {
		t.Fatal(i)
	}
	if f.Outstanding() != 0 {
		t.Fatal("completed work retained capacity")
	}
}

func TestStreamQueueLeaseAndSelfClaim(t *testing.T) {
	f := firstplacement.NewFence("lease-test")
	if _, e := f.Begin("reserved", []string{queueAddress(1)}, time.Now()); e != nil {
		t.Fatal(e)
	}
	if f.Idle([]string{queueAddress(1)}) {
		t.Fatal("reservation is not idle")
	}
	if !f.Idle([]string{queueAddress(2)}) {
		t.Fatal("unrelated account blocked")
	}
	m := &RelayCommitteeModule{initialLedger: &initialledger.Ledger{Routes: map[string]uint64{}}, admission: &RuntimeUnknownAdmissionManager{shardNum: 2}}
	claims := m.streamClaims(queueTx(16, 16))
	if len(claims) != 1 {
		t.Fatal("same account must not reserve twice", claims)
	}
}

func TestStreamQueueBoundIncludesHeldAndInFlight(t *testing.T) {
	f := firstplacement.NewFence("capacity")
	for i := 0; i < streamOutstandingLimit; i++ {
		if _, e := f.Admit(queueAddress(1), queueAddress(2), time.Now()); e != nil {
			t.Fatal(e)
		}
	}
	if f.Outstanding() != streamOutstandingLimit {
		t.Fatal(f.Outstanding())
	}
	if f.HeldCount() != streamOutstandingLimit-1 {
		t.Fatal(f.HeldCount())
	}
	// The queue adapter uses pending + Outstanding, so HOLD cannot evade capacity.
}
