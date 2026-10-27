package committee

import (
	"blockEmulator/core"
	"blockEmulator/initialledger"
	"blockEmulator/message"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestLocalBatchRealWireAndNativePoolOrder(t *testing.T) {
	t.Setenv("SAA_ORDERED_LOCAL_BATCH_V1", "true")
	m, received, ctx := dispatchFixture(t)
	f := m.firstAdmission
	a, b := queueAddress(1), queueAddress(2)
	m.initialLedger = &initialledger.Ledger{Routes: map[string]uint64{a: 0, b: 0}}
	head := queueTx(1, 2)
	f.submitOrdered([]*core.Transaction{head})
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	tail := []*core.Transaction{}
	for i := 0; i < 3; i++ {
		x := queueTx(1, 2)
		x.IngressSequence = uint64(i + 1)
		tail = append(tail, x)
		f.submitOrdered([]*core.Transaction{x})
	}
	select {
	case <-received:
		t.Fatal("sent before head terminal")
	default:
	}
	f.report(&message.BlockInfoMsg{InnerShardTxs: []*core.Transaction{head}, SenderShardID: 0, Epoch: 1, CommitTime: time.Now()})
	var packet message.InjectTxs
	select {
	case packet = <-received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if len(packet.Txs) != 3 {
		t.Fatal("not one batch", len(packet.Txs))
	}
	pool := &core.TxPool{}
	pool.AddTxs2Pool(packet.Txs)
	packed := append(pool.PackTxs(2), pool.PackTxs(2)...)
	for i, tx := range packed {
		if tx.AdmissionID != tail[i].AdmissionID || tx.RoutedSenderShard != 0 || tx.RoutedRecipientShard != 0 || !tx.SenderRequiresExistingState || !tx.RecipientRequiresExistingState {
			t.Fatal("wire/native pool order or binding changed")
		}
	}
	f.report(&message.BlockInfoMsg{InnerShardTxs: packet.Txs, SenderShardID: 0, Epoch: 2, CommitTime: time.Now()})
	if f.ledger.Outstanding() != 0 {
		t.Fatal("not complete")
	}
	f.report(&message.BlockInfoMsg{InnerShardTxs: packet.Txs, SenderShardID: 0, Epoch: 2, CommitTime: time.Now()})
	select {
	case <-received:
		t.Fatal("duplicate release")
	default:
	}
}

func TestLocalBatchEligibilityAndCutoff(t *testing.T) {
	m, _, _ := dispatchFixture(t)
	f := m.firstAdmission
	if f.localBatchEnabled {
		t.Fatal("enabled by default")
	}
	a, b := queueAddress(1), queueAddress(2)
	x := queueTx(1, 2)
	m.initialLedger = &initialledger.Ledger{Routes: map[string]uint64{a: 0}}
	if _, ok := f.localBatchShard(x); ok {
		t.Fatal("unknown account eligible")
	}
	m.initialLedger.Routes[b] = 1
	if _, ok := f.localBatchShard(x); ok {
		t.Fatal("cross shard eligible")
	}
	m.initialLedger.Routes[b] = 0
	if _, ok := f.localBatchShard(x); !ok {
		t.Fatal("known local excluded")
	}
	m.admission.dynamicAdmissionEnabled = true
	if _, ok := f.localBatchShard(x); ok {
		t.Fatal("dynamic mode eligible")
	}
	m.admission.dynamicAdmissionEnabled = false
	x.FirstPlacement = &core.FirstPlacementBinding{}
	if _, ok := f.localBatchShard(x); ok {
		t.Fatal("creator eligible")
	}
	f.localBatchEnabled = true
	origin := time.Now()
	m.finitePlan = &finitePlan{now: time.Now, origin: origin, cutoff: origin.Add(-time.Nanosecond)}
	f.scheduleLocalBatchesLocked()
	if f.ledger.Outstanding() != 0 {
		t.Fatal("cutoff mutated ledger")
	}
}

func TestArrivalIntegerPlanAll12000(t *testing.T) {
	origin := time.Unix(1000, 0)
	now := origin
	s := &arrivalSchedule{now: func() time.Time { return now }, sleep: func(d time.Duration) { now = now.Add(d) }}
	for i := 0; i < 12000; i++ {
		n, p, _ := s.next(1000)
		if n != int64(i) || p.Sub(origin) != time.Duration(i)*time.Millisecond {
			t.Fatalf("ordinal %d: %v", i, p.Sub(origin))
		}
	}
}

func TestLocalBatchUncertainWriteRetainsEveryMember(t *testing.T) {
	t.Setenv("SAA_ORDERED_LOCAL_BATCH_V1", "true")
	m, _, _ := dispatchFixture(t)
	f := m.firstAdmission
	a, b := queueAddress(1), queueAddress(2)
	m.initialLedger = &initialledger.Ledger{Routes: map[string]uint64{a: 0, b: 0}}
	head := queueTx(1, 2)
	if !f.admit(head) {
		t.Fatal("head held")
	}
	f.bindFixedRoute(head, 1)
	f.routed(head)
	first, second := queueTx(1, 2), queueTx(1, 2)
	f.submitOrdered([]*core.Transaction{first, second})
	var writes atomic.Int32
	f.writeOrdered = func(context.Context, string, []byte) error {
		writes.Add(1)
		return fmt.Errorf("controlled partial/uncertain batch write")
	}
	f.report(&message.BlockInfoMsg{InnerShardTxs: []*core.Transaction{head}, SenderShardID: 0, Epoch: 1, CommitTime: time.Now()})
	deadline := time.Now().Add(time.Second)
	for {
		x, _ := f.ledger.Flight(second.AdmissionID)
		if x.State == "SEND_UNCERTAIN" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("missing uncertain state")
		}
		time.Sleep(time.Millisecond)
	}
	for _, tx := range []*core.Transaction{first, second} {
		x, _ := f.ledger.Flight(tx.AdmissionID)
		if x.State != "SEND_UNCERTAIN" || x.Terminal {
			t.Fatal(x)
		}
	}
	next := queueTx(1, 2)
	if f.admit(next) {
		t.Fatal("uncertain batch allowed overtaking")
	}
	f.flowMu.Lock()
	f.scheduleReadyLocked()
	f.flowMu.Unlock()
	if writes.Load() != 1 || f.ledger.Outstanding() != 3 {
		t.Fatal("retried or released uncertain batch")
	}
}
