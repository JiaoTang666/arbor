package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Opt-in forensic replay on private copies. Never writes failed-run evidence.
func TestStoppedRunConfirmationReplay(t *testing.T) {
	run := os.Getenv("SAA_STOPPED_REPLAY_DIR")
	if run == "" {
		t.Skip("requires explicitly selected stopped evidence")
	}
	copyFiles := func(source, destination string, accept func(string) bool) {
		t.Helper()
		entries, err := os.ReadDir(source)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if !entry.Type().IsRegular() || !accept(entry.Name()) {
				continue
			}
			raw, err := os.ReadFile(filepath.Join(source, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(destination, entry.Name()), raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	journal, outbox := t.TempDir(), t.TempDir()
	copyFiles(filepath.Join(run, "SAA_NATIVE_SOURCE_QUEUE_ROOT", "s3-n0"), journal, func(s string) bool { return strings.HasSuffix(s, ".queue.json") })
	copyFiles(filepath.Join(run, "SAA_NATIVE_HANDOFF_OUTBOX_DIR"), outbox, func(s string) bool { return !strings.HasPrefix(s, ".") })
	raw, err := os.ReadFile(filepath.Join(journal, "00000000000000000001.queue.json"))
	if err != nil {
		t.Fatal(err)
	}
	var first migrationQueueSnapshot
	if err := json.Unmarshal(raw, &first); err != nil {
		t.Fatal(err)
	}
	p := NewTxPool()
	p.EnableIndexedMigrationAdmission()
	start := time.Now()
	if err := p.EnableGroupMigrationQueueJournal(journal, first.Accounts); err != nil {
		t.Fatal(err)
	}
	t.Logf("journal restore: %s, pending=%d", time.Since(start), p.GetTxQueueLen())
	records, err := LoadMigrationArtifacts(outbox, ".handoff.json", false)
	if err != nil {
		t.Fatal(err)
	}
	ackDir := filepath.Join(run, "SAA_NATIVE_HANDOFF_ACK_DIR")
	acks, err := LoadMigrationArtifacts(ackDir, ".ack.json", false)
	if err != nil {
		t.Fatal(err)
	}
	relays, err := LoadMigrationArtifacts(ackDir, ".relay-ack.json", false)
	if err != nil {
		t.Fatal(err)
	}
	pending := map[string]bool{}
	for _, tx := range p.pendingQueueLocked() {
		pending[string(tx.Encode())] = true
	}
	var items []CompletedHandoffConfirmation
	for _, id := range records.IDs() {
		var h MigrationHandoff
		if err := json.Unmarshal(records[id], &h); err != nil {
			t.Fatal(err)
		}
		if !pending[string(DecodeTx(h.Original).Encode())] {
			continue
		}
		a, ok := acks[id]
		if !ok {
			continue
		}
		item := CompletedHandoffConfirmation{H: &h}
		if err := json.Unmarshal(a, &item.Ack); err != nil {
			t.Fatal(err)
		}
		if item.Ack.RelayPending {
			r, ok := relays[id]
			if !ok {
				continue
			}
			item.Relay = &HandoffRelayAck{}
			if err := json.Unmarshal(r, item.Relay); err != nil {
				t.Fatal(err)
			}
		}
		items = append(items, item)
		if len(items) == 60 {
			break
		}
	}
	if len(items) != 60 {
		t.Fatalf("expected 60 completed obligations, got %d", len(items))
	}
	before := p.GetTxQueueLen()
	timing, err := p.ConfirmCompletedHandoffBatchTimed(outbox, items)
	t.Logf("real 60-record replay: %+v", timing)
	if err != nil {
		t.Fatal(err)
	}
	if p.GetTxQueueLen() != before-60 {
		t.Fatal("dequeue mismatch")
	}
	fresh := NewTxPool()
	fresh.EnableIndexedMigrationAdmission()
	if err := fresh.EnableGroupMigrationQueueJournal(journal, first.Accounts); err != nil {
		t.Fatal(err)
	}
	if fresh.GetTxQueueLen() != before-60 {
		t.Fatal("durable removal did not survive another restore")
	}
	if err := fresh.ConfirmCompletedHandoffBatch(outbox, items); err != nil {
		t.Fatal(err)
	}
	if fresh.GetTxQueueLen() != before-60 {
		t.Fatal("duplicate receipt changed recovered queue")
	}
}
