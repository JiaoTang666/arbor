package core

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkOrdinaryQueueBatch(b *testing.B) {
	for _, q := range []int{1000, 5000, 15000} {
		b.Run(fmt.Sprint(q), func(b *testing.B) {
			tx := &Transaction{Time: time.Unix(1, 0)}
			batch := make([]*Transaction, 1000)
			for i := range batch {
				batch[i] = tx
			}
			p := NewTxPool()
			b.ReportAllocs()
			b.ResetTimer()
			for k := 0; k < b.N; k++ {
				b.StopTimer()
				p.TxQueue = make([]*Transaction, q)
				for i := range p.TxQueue {
					p.TxQueue[i] = tx
				}
				b.StartTimer()
				p.AddTxs2Pool(batch)
			}
			b.StopTimer()
			if len(p.TxQueue) != q+1000 {
				b.Fatal("count")
			}
		})
	}
}

func TestOrdinaryQueueFastPathPreservesOrder(t *testing.T) {
	p := NewTxPool()
	a := &Transaction{Nonce: 1, Time: time.Unix(1, 0)}
	b := &Transaction{Nonce: 2}
	c := &Transaction{Nonce: 3}
	p.AddTx2Pool(a)
	p.AddTxs2Pool([]*Transaction{b, c})
	p.AddTxs2Pool(nil)
	if p.TxQueue[0] != a || p.TxQueue[1] != b || p.TxQueue[2] != c || b.Time.IsZero() || c.Time.IsZero() || !a.Time.Equal(time.Unix(1, 0)) {
		t.Fatal("admission order/time changed")
	}
	first := p.PackTxs(2)
	p.AddTx2Pool(&Transaction{Nonce: 4})
	if first[0] != a || first[1] != b {
		t.Fatal("packed slice overwritten")
	}
	rest := p.PackTxs(3)
	if len(rest) != 2 || rest[0] != c || rest[1].Nonce != 4 {
		t.Fatal("dequeue order")
	}
}
