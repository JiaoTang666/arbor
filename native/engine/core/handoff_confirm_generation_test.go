package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestJointConfirmationOnePublicationAndIndependentRecovery(t *testing.T) {
	p, outbox, journal, items := completedFixture(t, 3)
	publications := 0
	p.migrationJournalPublish = func(path string, raw []byte) error {
		publications++
		return PublishSourcePrepareFile(path, raw)
	}
	if err := p.ConfirmCompletedHandoffBatch(outbox, items); err != nil {
		t.Fatal(err)
	}
	if publications != 1 || p.migrationJournal.sequence != 2 || p.GetTxQueueLen() != 2000 {
		t.Fatal("not one joint publication", publications)
	}
	legacy, err := filepath.Glob(filepath.Join(outbox, "*confirmed*.json"))
	if err != nil || len(legacy) != 0 {
		t.Fatal("new writer dual-wrote old receipts", legacy, err)
	}
	fresh := NewTxPool()
	if err := fresh.EnableGroupMigrationQueueJournal(journal, p.migrationJournal.accounts); err != nil || fresh.GetTxQueueLen() != 0 {
		t.Fatal("joint proof did not recover dequeue", err)
	}
	if err := fresh.ConfirmCompletedHandoffBatch(outbox, items); err != nil || fresh.migrationJournal.sequence != 2 {
		t.Fatal("repeated proof published empty delta", err)
	}
	if _, timing, err := fresh.TryGroupDrainSnapshot(p.migrationJournal.accounts); err != nil || timing.Status != "ready" {
		t.Fatal("joint generation not accepted by drain", timing, err)
	}
	// A normalized full snapshot does not replace the earlier confirmation fact.
	proofs, err := loadCompletedBatchReceipts(outbox, journal)
	if err != nil || len(proofs) != len(items) {
		t.Fatal("drain normalization lost joint confirmations", err)
	}
	if len(items[0].H.Original) == 0 {
		t.Fatal("missing original fixture")
	}
	s, err := readConfirmGeneration(filepath.Join(journal, "00000000000000000002.queue.json"))
	if err != nil || len(s.Confirmations[0].InboundOriginal) != 0 || len(s.Confirmations[2].InboundOriginal) == 0 {
		t.Fatal("ordinary original duplicated or inbound identity omitted", err)
	}
}

func TestJointConfirmationUncertainPublicationPinsExactEvent(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_publication", true: "after_publication"}[published], func(t *testing.T) {
			p, outbox, journal, items := completedFixture(t, 3)
			var attempted []byte
			p.migrationJournalPublish = func(path string, raw []byte) error {
				attempted = append([]byte(nil), raw...)
				if published {
					if err := PublishSourcePrepareFile(path, raw); err != nil {
						return err
					}
				}
				return errors.New("injected uncertain publication result")
			}
			if err := p.ConfirmCompletedHandoffBatch(outbox, items); err == nil || p.GetTxQueueLen() != 2003 || p.migrationJournal.sequence != 1 || p.migrationJournal.pendingRaw == nil {
				t.Fatal("failed publication changed queue or lost uncertain generation", err)
			}
			p.migrationJournalPublish = nil
			if err := p.ConfirmCompletedHandoffBatch(outbox, items[:1]); err == nil {
				t.Fatal("different confirmation reused uncertain generation")
			}
			if !bytes.Equal(attempted, p.migrationJournal.pendingRaw) {
				t.Fatal("different retry replaced uncertain event")
			}
			if _, _, err := p.TryGroupDrainSnapshot(p.migrationJournal.accounts); err == nil {
				t.Fatal("uncertain durability certified drain")
			}
			fresh := NewTxPool()
			want := 3
			if published {
				want = 0
			}
			if err := fresh.EnableGroupMigrationQueueJournal(journal, p.migrationJournal.accounts); err != nil || fresh.GetTxQueueLen() != want {
				t.Fatal("crash recovery did not use complete published prefix", err, fresh.GetTxQueueLen())
			}
			if err := p.ConfirmCompletedHandoffBatch(outbox, items); err != nil || p.GetTxQueueLen() != 2000 || p.migrationJournal.sequence != 2 || p.migrationJournal.pendingRaw != nil {
				t.Fatal("exact durable retry did not finish", err)
			}
		})
	}
}

func TestJointConfirmationOnlyEventAndLegacyNoop(t *testing.T) {
	_, outbox, _, items := completedFixture(t, 3)
	p, journal := NewTxPool(), t.TempDir()
	accounts := items[0].H.GroupAccounts
	if err := p.EnableGroupMigrationQueueJournal(journal, accounts); err != nil {
		t.Fatal(err)
	}
	if err := p.ConfirmCompletedHandoffBatch(outbox, items); err != nil || p.migrationJournal.sequence != 2 {
		t.Fatal("new complete proof without current queue entry rejected", err)
	}
	fresh := NewTxPool()
	if err := fresh.EnableGroupMigrationQueueJournal(journal, accounts); err != nil || fresh.GetTxQueueLen() != 0 {
		t.Fatal("confirmation-only event did not recover", err)
	}
	legacyJournal, legacy := t.TempDir(), NewTxPool()
	if err := legacy.EnableGroupMigrationQueueJournal(legacyJournal, accounts); err != nil {
		t.Fatal(err)
	}
	if err := persistCompletedBatchReceipt(outbox, items); err != nil {
		t.Fatal(err)
	}
	if err := legacy.ConfirmCompletedHandoffBatch(outbox, items); err != nil || legacy.migrationJournal.sequence != 1 {
		t.Fatal("upgrade rewrote already recorded legacy batch", err)
	}
	other := NewTxPool()
	if err := other.EnableGroupMigrationQueueJournal(t.TempDir(), []string{"000000000000000000000000000000000000000c", "000000000000000000000000000000000000000d"}); err != nil {
		t.Fatal(err)
	}
	if err := other.ConfirmCompletedHandoffBatch(outbox, items); err == nil || other.migrationJournal.sequence != 1 {
		t.Fatal("legacy no-op bypassed local journal scope")
	}
}

func TestJointConfirmationCorruptionFailsRecoveryAndDrain(t *testing.T) {
	for _, corrupt := range []string{"relay", "unbound_remove", "foreign_kind", "group_scope", "mixed_scope", "duplicate_union", "duplicate_confirmation"} {
		t.Run(corrupt, func(t *testing.T) {
			p, outbox, journal, items := completedFixture(t, 3)
			if err := p.ConfirmCompletedHandoffBatch(outbox, items); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(journal, "00000000000000000002.queue.json")
			s, err := readConfirmGeneration(path)
			if err != nil {
				t.Fatal(err)
			}
			switch corrupt {
			case "relay":
				s.Confirmations[1].Receipt.Relay = nil
			case "unbound_remove":
				x := DecodeTx(s.Transactions[0])
				x.Nonce++
				s.Transactions[0] = x.Encode()
			case "foreign_kind":
				s.Kind = "full"
				s.Transactions = nil
			case "group_scope":
				s.Accounts = []string{"000000000000000000000000000000000000000c", "000000000000000000000000000000000000000d"}
			case "mixed_scope":
				s.Account = s.Accounts[0]
			case "duplicate_union":
				s.Accounts = append(s.Accounts, s.Accounts[len(s.Accounts)-1])
			case "duplicate_confirmation":
				s.Confirmations = append(s.Confirmations, s.Confirmations[0])
			}
			raw, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if err := NewTxPool().EnableGroupMigrationQueueJournal(journal, p.migrationJournal.accounts); err == nil {
				t.Fatal("corrupt joint event passed recovery")
			}
			if _, _, err := p.TryGroupDrainSnapshot(p.migrationJournal.accounts); err == nil {
				t.Fatal("corrupt joint event was normalized into a valid drain")
			}
			if _, err := AuditMigrationHandoffs(outbox, items[0].H.ProposalID, items[0].H.Account, journal); err == nil {
				t.Fatal("corrupt joint event passed retirement audit")
			}
		})
	}
}
