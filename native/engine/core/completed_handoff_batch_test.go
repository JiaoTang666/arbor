package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func completedFixture(t *testing.T, n int) (*TxPool, string, string, []CompletedHandoffConfirmation) {
	t.Helper()
	a, b, c := strings.Repeat("0", 39)+"a", strings.Repeat("0", 39)+"b", strings.Repeat("0", 39)+"c"
	p := NewTxPool()
	dir := t.TempDir()
	journal := t.TempDir()
	var items []CompletedHandoffConfirmation
	for i := 0; i < n; i++ {
		tx := NewTransaction(a, b, big.NewInt(1), uint64(i), time.Unix(1, 0))
		tx.RoutingFrozen = true
		tx.IngressSequence = uint64(i)
		var inbound *Transaction
		switch i % 3 {
		case 1:
			tx.Recipient = c
			tx.RoutedRecipientShard = 2
		case 2:
			tx.Sender = c
			tx.Recipient = a
			inbound = tx
		}
		p.AddTx2Pool(tx)
		wire := DecodeTx(tx.Encode())
		if inbound != nil {
			wire.Relayed = true
		}
		h, e := BuildGroupMigrationHandoff(wire, "p", []string{a, b}, 0, 1, 1)
		if e != nil {
			t.Fatal(e)
		}
		if e = PersistMigrationHandoff(dir, h); e != nil {
			t.Fatal(e)
		}
		hash := sha256.Sum256(h.Execution)
		ack := HandoffAck{ID: h.ID, ProposalID: "p", TargetShard: 1, ExecutionHash: hex.EncodeToString(hash[:]), Block: 1, BlockHash: strings.Repeat("1", 64), StateRoot: strings.Repeat("2", 64), Executed: true, RelayPending: i%3 == 1}
		item := CompletedHandoffConfirmation{H: h, Ack: ack, InboundOriginal: inbound}
		if ack.RelayPending {
			relay := DecodeTx(h.Execution)
			relay.Relayed = true
			raw := relay.Encode()
			rh := sha256.Sum256(raw)
			item.Relay = &HandoffRelayAck{RelayPayload: raw, ID: h.ID, RelayHash: hex.EncodeToString(rh[:]), RecipientShard: 2, Block: 1, BlockHash: strings.Repeat("3", 64), StateRoot: strings.Repeat("4", 64), Committed: true}
		}
		items = append(items, item)
	}
	// Unrelated backlog must remain ordered and not be re-encoded for every ACK.
	for i := 0; i < 2000; i++ {
		p.AddTx2Pool(NewTransaction(c, c, big.NewInt(1), uint64(10000+i), time.Unix(1, 0)))
	}
	if e := p.EnableGroupMigrationQueueJournal(journal, []string{a, b}); e != nil {
		t.Fatal(e)
	}
	return p, dir, journal, items
}

func TestCompletedBatchMixedParityRecovery(t *testing.T) {
	p, dir, journal, items := completedFixture(t, 60)
	legacy, ld, _, li := completedFixture(t, 60)
	start := time.Now()
	for _, x := range li {
		var e error
		if x.InboundOriginal != nil {
			e = legacy.ConfirmInboundHandoff(ld, x.H, x.Ack, x.InboundOriginal)
		} else if x.Relay != nil {
			e = legacy.ConfirmMigrationHandoff(ld, x.H, x.Ack, *x.Relay)
		} else {
			e = legacy.ConfirmMigrationHandoff(ld, x.H, x.Ack)
		}
		if e != nil {
			t.Fatal(e)
		}
	}
	oldTime := time.Since(start)
	start = time.Now()
	if e := p.ConfirmCompletedHandoffBatch(dir, items); e != nil {
		t.Fatal(e)
	}
	t.Logf("same 2060-entry queue, 60 mixed ACKs: per-item=%s batch=%s; queue publications=%d vs %d", oldTime, time.Since(start), legacy.migrationJournal.sequence-1, p.migrationJournal.sequence-1)
	if p.migrationJournal.sequence != 2 || len(p.pendingQueueLocked()) != 2000 {
		t.Fatal("not one durable dequeue")
	}
	for i, tx := range p.pendingQueueLocked() {
		if string(tx.Encode()) != string(legacy.pendingQueueLocked()[i].Encode()) {
			t.Fatal("queue/order changed")
		}
	}
	batched, err := loadCompletedBatchReceipts(dir, journal)
	if err != nil || len(batched) != len(items) {
		t.Fatal("durable batch receipt missing", err)
	}
	for _, x := range items {
		got := batched[x.H.ID]
		a, _ := os.ReadFile(filepath.Join(ld, x.H.ID+".confirmed.json"))
		b, _ := json.Marshal(got.Ack)
		if string(bytes.TrimSpace(a)) != string(b) {
			t.Fatal("batch ACK differs from legacy")
		}
		if x.Relay != nil {
			a, _ = os.ReadFile(filepath.Join(ld, x.H.ID+".relay-confirmed.json"))
			b, _ = json.Marshal(got.Relay)
			if string(bytes.TrimSpace(a)) != string(b) {
				t.Fatal("batch relay ACK differs from legacy")
			}
		}
	}
	if e := p.ConfirmCompletedHandoffBatch(dir, items); e != nil || p.migrationJournal.sequence != 2 {
		t.Fatal("replay not idempotent", e)
	}
	fresh := NewTxPool()
	if e := fresh.EnableGroupMigrationQueueJournal(journal, p.migrationJournal.accounts); e != nil || len(fresh.pendingQueueLocked()) != 0 {
		t.Fatal("recovery resurrects completed migration work", e)
	}
	members := map[string]string{}
	for _, x := range items {
		members[x.H.Account] = x.H.ProposalID
	}
	audits, e := AuditMigrationHandoffsForMembers(dir, members, journal)
	if e != nil {
		t.Fatal("batch receipt audit failed", e)
	}
	confirmed := 0
	for _, a := range audits {
		if len(a.Pending) != 0 || a.Persisted != a.Confirmed {
			t.Fatal("batch receipt did not close outbox", a)
		}
		confirmed += a.Confirmed
	}
	if confirmed != len(items) {
		t.Fatal("audit omitted batched confirmations")
	}
}

func TestCompletedBatchReceiptCorruptionStopsAudit(t *testing.T) {
	p, dir, journal, items := completedFixture(t, 3)
	if err := p.ConfirmCompletedHandoffBatch(dir, items); err != nil {
		t.Fatal(err)
	}
	files, err := CompletedMigrationGenerationFiles(journal)
	if err != nil || len(files) != 1 {
		t.Fatal("one durable batch receipt required", err, files)
	}
	if err := os.WriteFile(files[0], []byte("corrupt\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := AuditMigrationHandoffs(dir, items[0].H.ProposalID, items[0].H.Account, journal); err == nil {
		t.Fatal("corrupt batch receipt passed retirement audit")
	}
}

func TestCompletedBatchRejectsIncompleteOrConflictingEvidence(t *testing.T) {
	for _, kind := range []string{"missing_relay", "wrong_inbound", "duplicate", "confirmation_conflict", "queue_publish_conflict"} {
		t.Run(kind, func(t *testing.T) {
			p, dir, journal, items := completedFixture(t, 3)
			switch kind {
			case "missing_relay":
				items[1].Relay = nil
			case "wrong_inbound":
				copyTx := DecodeTx(items[2].InboundOriginal.Encode())
				copyTx.Nonce++
				items[2].InboundOriginal = copyTx
			case "duplicate":
				items = append(items, items[0])
			case "confirmation_conflict":
				os.WriteFile(filepath.Join(dir, items[1].H.ID+".confirmed.json"), []byte("conflict"), 0600)
			case "queue_publish_conflict":
				os.WriteFile(filepath.Join(journal, "00000000000000000002.queue.json"), []byte("conflict"), 0600)
			}
			if e := p.ConfirmCompletedHandoffBatch(dir, items); e == nil {
				t.Fatal("bad evidence accepted")
			}
			if len(p.pendingQueueLocked()) != 2003 || p.migrationJournal.sequence != 1 {
				t.Fatal("failed batch changed queue")
			}
			if kind != "queue_publish_conflict" {
				fresh := NewTxPool()
				if e := fresh.EnableGroupMigrationQueueJournal(journal, p.migrationJournal.accounts); e != nil || len(fresh.pendingQueueLocked()) != 3 {
					t.Fatal("failed batch lost recoverable work", e)
				}
			}
		})
	}
}
