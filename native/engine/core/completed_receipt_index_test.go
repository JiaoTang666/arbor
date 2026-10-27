package core

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func checkedReceiptIndex(t *testing.T, p *TxPool, outbox string, items []CompletedHandoffConfirmation) map[string]completedBatchReceiptItem {
	t.Helper()
	p.journalGate.Lock()
	defer p.journalGate.Unlock()
	selected, err := p.checkedCompletedReceiptStateLocked(outbox, items)
	if err != nil {
		t.Fatal(err)
	}
	return selected
}

func TestCompletedReceiptIndexReadsOnlyNewGenerations(t *testing.T) {
	p, outbox, journal, items := completedFixture(t, 3)
	for i := range items {
		if err := p.ConfirmCompletedHandoffBatch(outbox, items[i:i+1]); err != nil {
			t.Fatal(err)
		}
		stats := p.completedReceipts.stats
		wantValidated := 1
		if i == 0 {
			wantValidated = 0 // One old full snapshot, no confirmation yet.
		}
		if stats.Reads != 1 || stats.Validated != wantValidated {
			t.Fatal("batch re-read old generation bodies", i, stats)
		}
	}
	if len(checkedReceiptIndex(t, p, outbox, items[1:2])) != 1 || len(p.completedReceipts.receipts) != 3 {
		t.Fatal("selected result copied or omitted history")
	}
	if stats := p.completedReceipts.stats; stats.Reads != 1 || stats.Validated != 1 {
		t.Fatal("last new confirmation not validated once", stats)
	}
	checkedReceiptIndex(t, p, outbox, items[:1])
	if stats := p.completedReceipts.stats; stats.Reads != 0 || stats.BytesRead != 0 || stats.Validated != 0 {
		t.Fatal("warm lookup decoded old bodies", stats)
	}
	// A new ordinary generation is classified once, not mistaken for a proof.
	tx := DecodeTx(items[0].H.Original)
	tx.Nonce += 100
	p.AddTx2Pool(tx)
	checkedReceiptIndex(t, p, outbox, items[:1])
	if stats := p.completedReceipts.stats; stats.Reads != 1 || stats.Validated != 0 {
		t.Fatal("new ordinary delta rescanned confirmation history", stats)
	}
	// Restart has no trusted process cache and rebuilds from all durable facts.
	fresh := NewTxPool()
	if err := fresh.EnableGroupMigrationQueueJournal(journal, p.migrationJournal.accounts); err != nil {
		t.Fatal(err)
	}
	if len(checkedReceiptIndex(t, fresh, outbox, items)) != 3 || fresh.completedReceipts.stats.Validated != 3 {
		t.Fatal("cold recovery missed durable confirmations")
	}
	sequence := p.migrationJournal.sequence
	if err := p.ConfirmCompletedHandoffBatch(outbox, items); err != nil || p.migrationJournal.sequence != sequence {
		t.Fatal("indexed duplicate manufactured another generation", err)
	}
}

func TestCompletedReceiptIndexFailsClosedOnChanges(t *testing.T) {
	for _, mode := range []string{"replace_same_metadata", "delete", "root_replace", "new_conflict", "scope_change"} {
		t.Run(mode, func(t *testing.T) {
			p, outbox, journal, items := completedFixture(t, 3)
			if err := p.ConfirmCompletedHandoffBatch(outbox, items[:1]); err != nil {
				t.Fatal(err)
			}
			checkedReceiptIndex(t, p, outbox, items[:1])
			path := filepath.Join(journal, "00000000000000000002.queue.json")
			switch mode {
			case "replace_same_metadata":
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(path, path+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
					t.Fatal(err)
				}
			case "delete":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "root_replace":
				if err := os.Rename(journal, journal+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(journal, 0700); err != nil {
					t.Fatal(err)
				}
			case "new_conflict":
				conflicting := items[0]
				conflicting.Ack.Block++
				if err := persistCompletedBatchReceipt(outbox, []CompletedHandoffConfirmation{conflicting}); err != nil {
					t.Fatal(err)
				}
			case "scope_change":
				journal = t.TempDir()
			}
			// Even a request for a different ID must detect global contradictions.
			if _, err := p.completedReceipts.check(outbox, journal, items[1:2]); err == nil {
				t.Fatal("stale confirmation index survived", mode)
			}
		})
	}
}

func writeReceiptIndexGeneration(t *testing.T, journal string, members []string, sequence uint64, item CompletedHandoffConfirmation) string {
	t.Helper()
	var inbound []byte
	if item.InboundOriginal != nil {
		inbound = item.InboundOriginal.Encode()
	}
	s := migrationQueueSnapshot{Accounts: members, Sequence: sequence, Kind: confirmRemoveKind, ConfirmationSchema: confirmRemoveSchema,
		Confirmations: []queueHandoffConfirmation{{H: item.H, Receipt: completedBatchReceiptItem{ID: item.H.ID, Ack: item.Ack, Relay: item.Relay}, InboundOriginal: inbound}}}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(journal, fmt.Sprintf("%020d.queue.json", sequence))
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCompletedReceiptIndexFailedScanAndBudgetFallback(t *testing.T) {
	p, outbox, journal, items := completedFixture(t, 3)
	checkedReceiptIndex(t, p, outbox, items)
	x := p.completedReceipts
	used := x.used
	path := writeReceiptIndexGeneration(t, journal, p.migrationJournal.accounts, 2, items[0])
	bad := filepath.Join(journal, "00000000000000000003.queue.json")
	if err := os.WriteFile(bad, []byte(`{"Kind":"full","Confirmations":[{}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := x.check(outbox, journal, items); err == nil || len(x.receipts) != 0 || x.used != used {
		t.Fatal("partial failed scan remembered a completion", err)
	}
	if _, cached := x.files[path]; cached {
		t.Fatal("failed scan cached a partially validated file set")
	}
	if err := os.Remove(bad); err != nil {
		t.Fatal(err)
	}
	if len(checkedReceiptIndex(t, p, outbox, items)) != 1 {
		t.Fatal("clean retry lost durable confirmation")
	}
	// With no cache budget, the existing full loader still verifies every fact.
	uncached := &completedReceiptIndex{}
	selected, err := uncached.check(outbox, journal, items)
	if err != nil || len(selected) != 1 || !uncached.fullReload || uncached.used != 0 {
		t.Fatal("budget fallback changed the durable answer", err)
	}
	if err := os.WriteFile(bad, []byte(`{"Kind":"full","ConfirmationSchema":"foreign"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := uncached.check(outbox, journal, items); err == nil {
		t.Fatal("budget fallback ignored corrupt historical metadata")
	}
	if err := os.Remove(bad); err != nil {
		t.Fatal(err)
	}
	// A different pool/journal cannot borrow proof from this source journal.
	other := &completedReceiptIndex{limit: completedReceiptIndexLimit}
	selected, err = other.check(outbox, t.TempDir(), items)
	if err != nil || len(selected) != 0 {
		t.Fatal("confirmation leaked between journal scopes", err)
	}
	conflicting := items[0]
	conflicting.Ack.Block++
	if err := persistCompletedBatchReceipt(outbox, []CompletedHandoffConfirmation{conflicting}); err != nil {
		t.Fatal(err)
	}
	if _, err := uncached.check(outbox, journal, items); err == nil {
		t.Fatal("uncached fallback dropped a conflict")
	}
}
