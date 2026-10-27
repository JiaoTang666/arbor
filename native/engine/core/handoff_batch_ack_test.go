package core

import (
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHandoffBatchAckRecoveryAndFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "partial_confirm_failure"}[fail], func(t *testing.T) {
			a := "0000000000000000000000000000000000000010"
			p := NewTxPool()
			queue := t.TempDir()
			out := t.TempDir()
			hs := []HandoffConfirmation{}
			for n := uint64(1); n <= 2; n++ {
				tx := NewTransaction(a, a, big.NewInt(1), n, time.Unix(1, 0))
				tx.RoutingFrozen = true
				p.AddTx2Pool(tx)
				h, e := BuildMigrationHandoff(tx, "p", a, 0, 1, 1)
				if e != nil {
					t.Fatal(e)
				}
				if e = PersistMigrationHandoff(out, h); e != nil {
					t.Fatal(e)
				}
				digest := sha256.Sum256(h.Execution)
				hs = append(hs, HandoffConfirmation{h, HandoffAck{ID: h.ID, ProposalID: "p", TargetShard: 1, ExecutionHash: hex.EncodeToString(digest[:]), Block: n, BlockHash: strings.Repeat("1", 64), StateRoot: strings.Repeat("2", 64), Executed: true}})
			}
			if e := p.EnableMigrationQueueJournal(queue, a); e != nil {
				t.Fatal(e)
			}
			if fail {
				os.WriteFile(filepath.Join(out, hs[1].H.ID+".confirmed.json"), []byte("conflict"), 0600)
				if p.ConfirmMigrationHandoffBatch(out, hs) == nil || len(p.pendingQueueLocked()) != 2 || p.migrationJournal.sequence != 1 {
					t.Fatal("partial confirmation changed queue")
				}
				fresh := NewTxPool()
				if e := fresh.EnableMigrationQueueJournal(queue, a); e != nil || len(fresh.pendingQueueLocked()) != 2 {
					t.Fatal("failed batch lost recoverable entries", e)
				}
				return
			}
			if e := p.ConfirmMigrationHandoffBatch(out, hs); e != nil {
				t.Fatal(e)
			}
			if len(p.pendingQueueLocked()) != 0 || p.migrationJournal.sequence != 2 {
				t.Fatal("not one queue publication")
			}
			if e := p.ConfirmMigrationHandoffBatch(out, hs); e != nil || p.migrationJournal.sequence != 2 {
				t.Fatal("ack replay not idempotent", e)
			}
			fresh := NewTxPool()
			if e := fresh.EnableMigrationQueueJournal(queue, a); e != nil || len(fresh.pendingQueueLocked()) != 0 {
				t.Fatal("durable dequeue recovery failed", e)
			}
		})
	}
}
