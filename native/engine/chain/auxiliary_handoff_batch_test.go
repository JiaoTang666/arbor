package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"bytes"
	"math/big"
	"testing"
	"time"
)

func TestInboundBatchOneRootCumulativeAndReplay(t *testing.T) {
	source, target, c := activatedLazyFixture(t)
	sender := "0000000000000000000000000000000000000020"
	makeItem := func(n uint64, value int64) InboundDebitItem {
		tx := core.NewTransaction(sender, c.Account, big.NewInt(value), n, time.Unix(1, 0))
		tx.RoutingFrozen = true
		return InboundDebitItem{tx, 1}
	}
	bad := []InboundDebitItem{makeItem(1, 60), makeItem(2, 60)}
	head := append([]byte(nil), source.CurrentBlock.Hash...)
	if source.ValidateInboundDebitBatch(bad) == nil || source.CommitInboundDebitBatch(bad) == nil {
		t.Fatal("cumulative overdraft accepted")
	}
	if !bytes.Equal(head, source.CurrentBlock.Hash) {
		t.Fatal("failed batch published state")
	}
	items := []InboundDebitItem{makeItem(1, 7), makeItem(2, 9)}
	height := source.CurrentBlock.Header.Number
	if e := source.ValidateInboundDebitBatch(items); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(head, source.CurrentBlock.Hash) {
		t.Fatal("validation mutated state")
	}
	if e := source.CommitInboundDebitBatch(items); e != nil {
		t.Fatal(e)
	}
	if source.CurrentBlock.Header.Number != height+1 {
		t.Fatal("one batch created multiple blocks")
	}
	s, _ := source.ExportAccountState(sender)
	if s.State.Balance.Int64() != 84 || s.State.Nonce != 2 {
		t.Fatal("wrong cumulative debit")
	}
	for _, item := range items {
		r, e := source.FindInboundDebit(item.Transaction, 1)
		if e != nil || r == nil || r.Block != height+1 {
			t.Fatal("missing shared-root marker")
		}
		if _, e = target.CommitMigrationHandoff(r.Credit); e != nil {
			t.Fatal(e)
		}
	}
	head = append([]byte(nil), source.CurrentBlock.Hash...)
	if e := source.CommitInboundDebitBatch(items); e != nil || !bytes.Equal(head, source.CurrentBlock.Hash) {
		t.Fatal("batch replay changed state")
	}
	frozen, _ := source.ExportAccountState(c.Account)
	if frozen.State.Balance.Int64() != 100 {
		t.Fatal("frozen source changed")
	}
	if source.ValidateInboundDebitBatch([]InboundDebitItem{items[0], items[0]}) == nil {
		t.Fatal("duplicate accepted")
	}
}

func TestRelayBatchLazyRecipientSharedRootAndReplay(t *testing.T) {
	source, target, c := activatedLazyFixture(t)
	recipient := "f93b779a96e54868d9cbe6f64351015c97b67b02"
	var items []HandoffExecution
	for i := 0; i < 3; i++ {
		tx := core.NewTransaction(c.Account, recipient, big.NewInt(7), uint64(i+1), time.Unix(1, 0))
		tx.RoutingFrozen = true
		h, e := core.BuildMigrationHandoff(tx, c.ProposalID, c.Account, 0, 1, 1)
		if e != nil {
			t.Fatal(e)
		}
		r, e := target.CommitMigrationHandoff(*h)
		if e != nil {
			t.Fatal(e)
		}
		items = append(items, *r)
	}
	height := source.CurrentBlock.Header.Number
	head := append([]byte(nil), source.CurrentBlock.Hash...)
	if e := source.ValidateHandoffRelayBatch(items); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(head, source.CurrentBlock.Hash) {
		t.Fatal("validation changed root")
	}
	if source.ValidateHandoffRelayBatch([]HandoffExecution{items[0], items[0]}) == nil {
		t.Fatal("duplicate relay accepted")
	}
	if e := source.CommitHandoffRelayBatch(items); e != nil {
		t.Fatal(e)
	}
	if source.CurrentBlock.Header.Number != height+1 {
		t.Fatal("multiple roots")
	}
	s, e := source.ExportAccountState(recipient)
	if e != nil {
		t.Fatal(e)
	}
	want := new(big.Int).Add(new(big.Int).Set(params.Init_Balance), big.NewInt(21))
	if s.State.Balance.Cmp(want) != 0 {
		t.Fatal("new account initialized more than once")
	}
	for _, item := range items {
		r, e := source.CommitHandoffRelay(item)
		if e != nil || r.Block != height+1 {
			t.Fatal("shared root receipt lost")
		}
	}
	head = append([]byte(nil), source.CurrentBlock.Hash...)
	if e := source.CommitHandoffRelayBatch(items); e != nil || !bytes.Equal(head, source.CurrentBlock.Hash) {
		t.Fatal("replay changed root")
	}
}

func TestTargetAtomicBatchLazyCounterpart(t *testing.T) {
	_, target, c := activatedLazyFixture(t)
	recipient := "f93b779a96e54868d9cbe6f64351015c97b67b02"
	items := []core.MigrationHandoff{}
	for i := 0; i < 2; i++ {
		tx := core.NewTransaction(c.Account, recipient, big.NewInt(7), uint64(i+1), time.Unix(1, 0))
		tx.RoutingFrozen = true
		tx.RoutedRecipientShard = 1
		h, e := core.BuildMigrationHandoff(tx, c.ProposalID, c.Account, 0, 1, 1)
		if e != nil {
			t.Fatal(e)
		}
		items = append(items, *h)
	}
	height := target.CurrentBlock.Header.Number
	if e := target.CommitMigrationHandoffBatch(items); e != nil {
		t.Fatal(e)
	}
	if target.CurrentBlock.Header.Number != height+1 {
		t.Fatal("multiple blocks")
	}
	s, e := target.ExportAccountState(recipient)
	if e != nil {
		t.Fatal(e)
	}
	if s.State.Balance.Cmp(new(big.Int).Add(new(big.Int).Set(params.Init_Balance), big.NewInt(14))) != 0 {
		t.Fatal("wrong atomic balance")
	}
}
