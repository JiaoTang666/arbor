package core

import (
	"strings"
	"testing"
	"unsafe"
)

func TestSourceHoldPreservesLateRelayAndUnrelatedFIFO(t *testing.T) {
	for _, bytesMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "count", true: "bytes"}[bytesMode], func(t *testing.T) {
			p := NewTxPool()
			a, b, c := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
			early := &Transaction{Sender: a, Recipient: b}
			first := &Transaction{Sender: b, Recipient: c}
			late := &Transaction{Sender: c, Recipient: "0x" + a, Relayed: true}
			second := &Transaction{Sender: c, Recipient: b}
			p.AddTxs2Pool([]*Transaction{early, first})
			p.HoldSourceAccount(a)
			p.AddTxs2Pool([]*Transaction{late, second})
			var selected []*Transaction
			if bytesMode {
				selected = p.PackTxsWithBytes(2 * int(unsafe.Sizeof(Transaction{})))
			} else {
				selected = p.PackTxs(2)
			}
			if len(selected) != 2 || selected[0] != first || selected[1] != second {
				t.Fatal("unrelated FIFO blocked")
			}
			if len(p.TxQueue) != 2 || p.TxQueue[0] != early || p.TxQueue[1] != late {
				t.Fatal("held transaction lost or reordered")
			}
			if got := p.PackTxs(100); len(got) != 0 {
				t.Fatal("held transaction executed")
			}
			if p.GetPendingWorkloadCount() != 2 {
				t.Fatal("held work hidden from accounting")
			}
		})
	}
}

// Keep the old group scan as an oracle: the optimization must change cost,
// not which endpoint owns a pending transaction's execution obligation.
func legacyHeldForMigration(p *TxPool, tx *Transaction) bool {
	if holdMigrationTransaction(tx, p.sourcePreparedAccount) {
		return true
	}
	for account := range p.heldMigrationAccounts {
		if holdMigrationTransaction(tx, account) {
			return true
		}
	}
	return false
}

func TestHeldForMigrationMatchesLegacyPredicate(t *testing.T) {
	a, b, c, d := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40), strings.Repeat("d", 40)
	endpoints := [][4]string{
		{}, {a, b, "", ""}, {a, a, "", ""},
		{" 0X" + strings.ToUpper(a) + " ", "0x" + b, "", ""},
		{a, b, c, d}, {b, c, a, ""}, {b, c, "", a},
		{a, b, b, a}, {a, b, " ", ""}, {a, b, "", "0x"},
	}
	pools := []TxPool{
		{}, {sourcePreparedAccount: a}, {sourcePreparedAccount: "0X" + strings.ToUpper(a)},
		{heldMigrationAccounts: map[string]bool{a: true}},
		{heldMigrationAccounts: map[string]bool{b: false}},
		{heldMigrationAccounts: map[string]bool{c: true, d: true}},
		{sourcePreparedAccount: a, heldMigrationAccounts: map[string]bool{b: true}},
		{heldMigrationAccounts: map[string]bool{a: true, b: true, c: true, d: true}},
	}
	for pi := range pools {
		p := &pools[pi]
		if p.heldForMigration(nil) != legacyHeldForMigration(p, nil) {
			t.Fatalf("nil transaction differs for pool %d", pi)
		}
		for ei, e := range endpoints {
			for flags := 0; flags < 16; flags++ {
				for _, shards := range [][2]uint64{{0, 0}, {0, 1}, {1, 0}} {
					tx := &Transaction{
						Sender: e[0], Recipient: e[1], OriginalSender: e[2], FinalRecipient: e[3],
						Relayed: flags&1 != 0, HasBroker: flags&2 != 0,
						RoutingFrozen: flags&4 != 0, SenderIsBroker: flags&8 != 0,
						RoutedSenderShard: shards[0], RoutedRecipientShard: shards[1],
					}
					if got, want := p.heldForMigration(tx), legacyHeldForMigration(p, tx); got != want {
						t.Fatalf("pool=%d endpoints=%d flags=%d shards=%v: got %v, want %v", pi, ei, flags, shards, got, want)
					}
				}
			}
		}
	}
}

func TestHeldForMigrationEndpointContracts(t *testing.T) {
	a, b, c := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	p := NewTxPool()
	p.HoldSourceAccounts([]string{a})
	tests := []struct {
		name string
		tx   Transaction
		want bool
	}{
		{"raw_relay_sender_is_not_written", Transaction{Sender: a, Recipient: b, Relayed: true}, false},
		{"raw_relay_recipient_is_written", Transaction{Sender: b, Recipient: a, Relayed: true}, true},
		{"new_route_inbound_debit_can_run", Transaction{Sender: b, Recipient: a, RoutingFrozen: true, RoutedSenderShard: 0, RoutedRecipientShard: 1}, false},
		{"old_local_inbound_stays_held", Transaction{Sender: b, Recipient: a, RoutingFrozen: true}, true},
		{"migrating_sender_stays_held", Transaction{Sender: a, Recipient: b, RoutingFrozen: true, RoutedSenderShard: 0, RoutedRecipientShard: 1}, true},
		{"self_transfer_stays_held", Transaction{Sender: a, Recipient: a, RoutingFrozen: true, RoutedSenderShard: 0, RoutedRecipientShard: 1}, true},
		{"broker_original_sender_stays_held", Transaction{Sender: b, Recipient: c, HasBroker: true, OriginalSender: a}, true},
		{"broker_final_recipient_stays_held", Transaction{Sender: b, Recipient: c, HasBroker: true, FinalRecipient: a}, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := p.heldForMigration(&test.tx); got != test.want {
				t.Fatalf("got %v, want %v", got, test.want)
			}
		})
	}
}

func TestSourceGroupHoldAndReleasePreserveFIFO(t *testing.T) {
	for _, bytesMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "count", true: "bytes"}[bytesMode], func(t *testing.T) {
			p := NewTxPool()
			a, b, c, d := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40), strings.Repeat("d", 40)
			heldSender := &Transaction{Sender: a, Recipient: c}
			first := &Transaction{Sender: c, Recipient: d}
			p.AddTxs2Pool([]*Transaction{heldSender, first})
			p.HoldSourceAccounts([]string{"0X" + strings.ToUpper(a), " ", "0x" + b, a, b})
			if len(p.heldMigrationAccounts) != 2 || !p.heldMigrationAccounts[a] || !p.heldMigrationAccounts[b] {
				t.Fatal("group fence did not publish canonical members")
			}
			heldRelay := &Transaction{Sender: c, Recipient: b, Relayed: true}
			second := &Transaction{Sender: d, Recipient: c}
			relayWithHeldSender := &Transaction{Sender: a, Recipient: d, Relayed: true}
			inboundDebit := &Transaction{Sender: c, Recipient: a, RoutingFrozen: true, RoutedSenderShard: 0, RoutedRecipientShard: 1}
			p.AddTxs2Pool([]*Transaction{heldRelay, second, relayWithHeldSender, inboundDebit})
			var selected []*Transaction
			if bytesMode {
				selected = p.PackTxsWithBytes(4 * int(unsafe.Sizeof(Transaction{})))
			} else {
				selected = p.PackTxs(4)
			}
			want := []*Transaction{first, second, relayWithHeldSender, inboundDebit}
			if len(selected) != len(want) {
				t.Fatalf("selected %d transactions, want %d", len(selected), len(want))
			}
			for i := range want {
				if selected[i] != want[i] {
					t.Fatalf("eligible FIFO differs at %d", i)
				}
			}
			if len(p.TxQueue) != 2 || p.TxQueue[0] != heldSender || p.TxQueue[1] != heldRelay {
				t.Fatal("group-held entries lost or reordered")
			}
			p.HoldSourceAccounts([]string{"", " 0x "})
			if got := p.PackTxs(100); len(got) != 0 {
				t.Fatal("empty group unexpectedly released existing fence")
			}
			p.ReleaseMigrationAccount("0x" + a)
			if got := p.PackTxs(100); len(got) != 1 || got[0] != heldSender {
				t.Fatal("single-account compatibility released the wrong group member")
			}
			p.ReleaseMigrationAccount(b)
			if got := p.PackTxs(100); len(got) != 1 || got[0] != heldRelay {
				t.Fatal("last group member was not released")
			}
		})
	}
}
