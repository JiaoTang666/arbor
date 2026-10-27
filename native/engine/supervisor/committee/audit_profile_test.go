package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"bufio"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"
)

// Isolates production audit CPU/allocations from disk and consensus. This is
// not a native performance experiment and cannot establish policy benefit.
func BenchmarkProductionLifecycleAudit(b *testing.B) {
	f, e := os.OpenFile(os.DevNull, os.O_WRONLY, 0600)
	if e != nil {
		b.Fatal(e)
	}
	defer f.Close()
	m := &RelayCommitteeModule{offeredAuditPath: "profile", nativeAudit: &batchAudit{f: f, w: bufio.NewWriterSize(f, 1024*1024)}}
	batches := make(chan []*core.Transaction, 4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for txs := range batches {
			m.auditTransport(txs, "first_socket_write", time.Now(), nil)
			m.auditBlock(&message.BlockInfoMsg{Epoch: 1, CommitTime: time.Now(), InnerShardTxs: txs})
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	batch := make([]*core.Transaction, 0, 1000)
	for i := 0; i < b.N; i++ {
		tx := &core.Transaction{Sender: fmt.Sprintf("%040x", i), Recipient: fmt.Sprintf("%040x", i+1), Value: big.NewInt(100), TxHash: []byte(fmt.Sprintf("%064x", i)), IngressSequence: uint64(i), RoutingFrozen: true, RoutingEpoch: 1}
		m.auditAccepted(tx)
		m.auditRoute(tx)
		batch = append(batch, tx)
		if len(batch) == 1000 {
			batches <- batch
			batch = make([]*core.Transaction, 0, 1000)
		}
	}
	if len(batch) > 0 {
		batches <- batch
	}
	close(batches)
	<-done
	m.nativeAudit.flush()
	b.StopTimer()
	if len(m.lifecycle) != b.N {
		b.Fatal("lost lifecycle")
	}
	for _, x := range m.lifecycle {
		if !x.Done || !x.Written {
			b.Fatal("incomplete lifecycle")
		}
	}
}
