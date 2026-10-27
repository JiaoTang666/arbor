package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestOrderedSlowTransportDoesNotLockIngress(t *testing.T) {
	m, _, _ := dispatchFixture(t)
	f := m.firstAdmission
	a, b, c, d := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40), strings.Repeat("d", 40)
	first := core.NewTransaction(a, b, big.NewInt(1), 0, time.Now())
	if !f.admit(first) {
		t.Fatal("first held")
	}
	f.bindFixedRoute(first, 1)
	f.routed(first)
	next := core.NewTransaction(a, b, big.NewInt(1), 1, time.Now())
	if f.admit(next) {
		t.Fatal("successor bypassed")
	}
	started, finish, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	f.writeOrdered = func(ctx context.Context, _ string, _ []byte) error {
		close(started)
		select {
		case <-finish:
		case <-ctx.Done():
		}
		close(done)
		return fmt.Errorf("controlled uncertain write")
	}
	f.report(&message.BlockInfoMsg{Relay1Txs: []*core.Transaction{first}, SenderShardID: first.RoutedSenderShard, CommitTime: time.Now()})
	f.report(&message.BlockInfoMsg{Relay2Txs: []*core.Transaction{first}, SenderShardID: first.RoutedRecipientShard, CommitTime: time.Now()})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker not started")
	}
	accepted := make(chan bool, 1)
	go func() { accepted <- f.admit(core.NewTransaction(c, d, big.NewInt(1), 2, time.Now())) }()
	select {
	case ok := <-accepted:
		if !ok {
			t.Fatal("unrelated ingress held")
		}
	case <-time.After(time.Second):
		t.Fatal("network held ingress lock")
	}
	same := core.NewTransaction(b, a, big.NewInt(1), 3, time.Now())
	if f.admit(same) {
		t.Fatal("same-account bypassed slow predecessor")
	}
	close(finish)
	<-done
	deadline := time.Now().Add(time.Second)
	for {
		flight, _ := f.ledger.Flight(next.AdmissionID)
		if flight.State == "SEND_UNCERTAIN" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("uncertain status missing")
		}
		time.Sleep(time.Millisecond)
	}
	if f.ledger.HeldCount() != 1 {
		t.Fatal("uncertain predecessor released successor")
	}
}
