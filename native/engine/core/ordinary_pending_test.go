package core

import "testing"

func TestOrdinaryEligibilityPreservesHold(t *testing.T) {
	p := NewTxPool()
	p.HoldSourceAccount("held")
	p.AddTx2Pool(&Transaction{Sender: "held", Recipient: "other"})
	if p.HasOrdinaryPending() {
		t.Fatal("held transaction declared ordinary")
	}
	p.AddTx2Pool(&Transaction{Sender: "free", Recipient: "other"})
	if !p.HasOrdinaryPending() {
		t.Fatal("ordinary transaction starved")
	}
	if p.GetTxQueueLen() != 2 {
		t.Fatal("eligibility consumed queue")
	}
}

func TestPostActivationRouteReleasedWithoutReleasingOldRoute(t *testing.T) {
	p := NewTxPool()
	account := "974caa59e49682cda0ad2bbe82983419a2ecc400"
	p.HoldSourceAccount(account)
	old := &Transaction{Sender: "ordinary", Recipient: account, RoutingFrozen: true, RoutedSenderShard: 6, RoutedRecipientShard: 6}
	newRoute := &Transaction{Sender: "ordinary", Recipient: account, RoutingFrozen: true, RoutedSenderShard: 6, RoutedRecipientShard: 2}
	memberSender := &Transaction{Sender: account, Recipient: "ordinary", RoutingFrozen: true, RoutedSenderShard: 6, RoutedRecipientShard: 2}
	relayCredit := &Transaction{Sender: "ordinary", Recipient: account, RoutingFrozen: true, Relayed: true, RoutedSenderShard: 6, RoutedRecipientShard: 2}
	p.AddTx2Pool(old)
	p.AddTx2Pool(newRoute)
	p.AddTx2Pool(memberSender)
	p.AddTx2Pool(relayCredit)
	if !p.HasOrdinaryPending() {
		t.Fatal("new target route withheld")
	}
	packed := p.PackTxs(4)
	if len(packed) != 1 || packed[0] != newRoute {
		t.Fatal("routing generation mixed in ordinary pack")
	}
	if p.GetTxQueueLen() != 3 {
		t.Fatal("old work disappeared")
	}
	if p.HasOrdinaryPending() {
		t.Fatal("old held route became ordinary")
	}
}
