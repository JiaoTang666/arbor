package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"testing"
)

func TestFairHandoffNoLaneStarvation(t *testing.T) {
	counts := map[message.RequestType]int{}
	lanes := [3]func() (*message.Request, error){}
	for i, kind := range []message.RequestType{inboundDebitBatchRequest, handoffRelayBatchRequest, migrationHandoffBatchRequest} {
		k := kind
		lanes[i] = func() (*message.Request, error) { return &message.Request{RequestType: k}, nil }
	}
	for seq := uint64(0); seq < 40; seq++ {
		r, e := chooseFairHandoff(seq, true, lanes)
		if e != nil {
			t.Fatal(e)
		}
		if r == nil {
			counts["ordinary"]++
		} else {
			counts[r.RequestType]++
		}
	}
	if len(counts) != 4 {
		t.Fatal(counts)
	}
	for _, n := range counts {
		if n != 10 {
			t.Fatal(counts)
		}
	}
}
func TestFairHandoffSkipsEmptyLanes(t *testing.T) {
	none := func() (*message.Request, error) { return nil, nil }
	ready := func() (*message.Request, error) { return &message.Request{RequestType: handoffRelayBatchRequest}, nil }
	for seq := uint64(0); seq < 4; seq++ {
		r, e := chooseFairHandoff(seq, false, [3]func() (*message.Request, error){none, ready, none})
		if e != nil || r == nil || r.RequestType != handoffRelayBatchRequest {
			t.Fatal("idle lane wasted proposal")
		}
	}
}

func TestGroupInboundUsesFrozenSourceLocalRoute(t *testing.T) {
	member := "974caa59e49682cda0ad2bbe82983419a2ecc400"
	members := map[string]bool{member: true}
	tx := &core.Transaction{Sender: "ordinary", Recipient: member, RoutingFrozen: true, RoutedSenderShard: 6, RoutedRecipientShard: 6}
	if !groupInboundOnSource(tx, members, 6) {
		t.Fatal("old source-local route lost")
	}
	tx.RoutedRecipientShard = 2
	if groupInboundOnSource(tx, members, 6) {
		t.Fatal("new target route misclassified as old inbound")
	}
	tx.RoutedRecipientShard = 6
	tx.Relayed = true
	if groupInboundOnSource(tx, members, 6) {
		t.Fatal("relay credit misclassified as debit")
	}
}
