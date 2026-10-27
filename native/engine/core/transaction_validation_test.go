package core

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func validationPanic(t *testing.T, action func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("invalid operation accepted")
		}
	}()
	action()
}

func TestOwnershipValidationRejectsWholeBatchBeforeTimeOrQueue(t *testing.T) {
	p := NewTxPool()
	if err := p.ConfigureTransactionValidation(func(tx *Transaction) error {
		if tx.Recipient == "bad" {
			return fmt.Errorf("invalid authorization")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	good, bad := &Transaction{Recipient: "good"}, &Transaction{Recipient: "bad"}
	if err := p.AddAuthorizedTxs([]*Transaction{good, bad}); err == nil {
		t.Fatal("untrusted invalid batch accepted")
	}
	if err := p.AddAuthorizedTxs([]*Transaction{nil}); err == nil {
		t.Fatal("untrusted nil accepted")
	}
	validationPanic(t, func() { p.AddTxs2Pool([]*Transaction{good, bad}) })
	if p.GetTxQueueLen() != 0 || !good.Time.IsZero() || !bad.Time.IsZero() {
		t.Fatal("partial admission side effects")
	}
	p.AddTx2Pool(good) // Also proves the rejection released its lock.
	if err := p.ConfigureTransactionValidation(func(*Transaction) error { return nil }); err == nil {
		t.Fatal("startup validator was replaced")
	}
	validationPanic(t, func() { p.AddTxs2Pool_Head([]*Transaction{bad}) })
	if p.GetTxQueueLen() != 1 || p.PackTxs(1)[0] != good {
		t.Fatal("head rejection changed FIFO")
	}
}

func TestOwnershipValidationChecksOnlySelectedPrefix(t *testing.T) {
	p := NewTxPool()
	if err := p.ConfigureTransactionValidation(func(tx *Transaction) error {
		if tx.Recipient == "bad" {
			return fmt.Errorf("invalid authorization")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	good, bad := &Transaction{Recipient: "good"}, &Transaction{Recipient: "bad"}
	p.TxQueue = []*Transaction{good, bad} // Simulate a legacy in-process bypass.
	if got := p.PackTxs(1); len(got) != 1 || got[0] != good {
		t.Fatal("unrelated tail blocked legal FIFO prefix")
	}
	validationPanic(t, func() { p.PackTxs(1) })
	validationPanic(t, func() { p.PackTxsWithBytes(1 << 20) })
	if p.GetTxQueueLen() != 1 || p.TxQueue[0] != bad {
		t.Fatal("rejected selection consumed work")
	}
}

func TestOwnershipValidationConcurrentAdmission(t *testing.T) {
	p := NewTxPool()
	var checks int64
	if err := p.ConfigureTransactionValidation(func(*Transaction) error { atomic.AddInt64(&checks, 1); return nil }); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); p.AddTx2Pool(&Transaction{Time: time.Unix(1, 0)}) }()
	}
	wg.Wait()
	if p.GetTxQueueLen() != 20 || atomic.LoadInt64(&checks) != 20 {
		t.Fatal("concurrent admission lost work or authorization")
	}
}
