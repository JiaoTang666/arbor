package core

import "testing"

func TestReplaceRelayPoolPreservesDestinationAndOrder(t *testing.T) {
	pool := NewTxPool()
	a := &Transaction{Nonce: 1}
	b := &Transaction{Nonce: 2}
	c := &Transaction{Nonce: 3}
	groups := map[uint64][]*Transaction{1: {a, c}, 3: {b}}
	pool.ReplaceRelayPool(groups)
	if len(pool.RelayPool) != 2 || len(pool.RelayPool[1]) != 2 || pool.RelayPool[1][0] != a || pool.RelayPool[1][1] != c || pool.RelayPool[3][0] != b {
		t.Fatal("relay grouping changed")
	}
	pool.ReplaceRelayPool(nil)
	if len(pool.RelayPool) != 0 {
		t.Fatal("next block retained previous relays")
	}
}
