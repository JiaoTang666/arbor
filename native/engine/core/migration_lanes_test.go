package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

func TestIsolatedAckIOAllowsOrdinaryAdmissionAndPacking(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			p, dir, journal, items := completedFixture(t, 3)
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			p.migrationJournalPublish = func(path string, raw []byte) error {
				var s migrationQueueSnapshot
				if err := json.Unmarshal(raw, &s); err != nil {
					return err
				}
				if s.Kind != confirmRemoveKind || len(s.Transactions) != 3 || len(s.Confirmations) != 3 {
					return errors.New("ACK rewrote a full queue")
				}
				close(started)
				<-release
				if fail {
					return errors.New("injected removal failure")
				}
				return PublishSourcePrepareFile(path, raw)
			}
			go func() { done <- p.ConfirmCompletedHandoffBatch(dir, items) }()
			awaitIsolation(t, started, "ACK never reached removal publication")
			ordinary := isolationTx("000000000000000000000000000000000000000c", 99999)
			progress := make(chan struct{})
			go func() {
				p.AddTx2Pool(ordinary)
				packed := p.PackTxs(3000)
				if len(packed) != 2001 || packed[len(packed)-1] != ordinary {
					t.Error("ordinary work blocked/lost or held work packed")
				}
				close(progress)
			}()
			awaitIsolation(t, progress, "ACK disk I/O blocked ordinary work")
			if p.GetTxQueueLen() != 3 {
				t.Fatal("ACK became visible before durability")
			}
			close(release)
			select {
			case err := <-done:
				if (err != nil) != fail {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("ACK stuck")
			}
			want := 0
			if fail {
				want = 3
			}
			if p.GetTxQueueLen() != want {
				t.Fatal("wrong post-ACK count")
			}
			fresh := NewTxPool()
			if err := fresh.EnableGroupMigrationQueueJournal(journal, p.migrationJournal.accounts); err != nil || fresh.GetTxQueueLen() != want {
				t.Fatal("wrong removal recovery", err)
			}
			p.migrationJournalPublish = nil
			if err := p.ConfirmCompletedHandoffBatch(dir, items); err != nil {
				t.Fatal("retry/idempotence", err)
			}
		})
	}
}

func TestLaneFIFOHeadReleaseAndExecutableRelatedWork(t *testing.T) {
	a := "000000000000000000000000000000000000000a"
	b := "000000000000000000000000000000000000000b"
	p := NewTxPool()
	if err := p.EnableMigrationQueueJournal(t.TempDir(), a); err != nil {
		t.Fatal(err)
	}
	held, ordinary, creditSource := isolationTx(a, 1), isolationTx(b, 2), isolationTx(b, 3)
	creditSource.Recipient = a
	creditSource.RoutingFrozen = true
	creditSource.RoutedSenderShard = 0
	creditSource.RoutedRecipientShard = 1
	p.AddTxs2Pool([]*Transaction{held, ordinary, creditSource})
	head := isolationTx(b, 0)
	p.AddTxs2Pool_Head([]*Transaction{head})
	rows, _ := p.SnapshotPendingQueueObservation()
	if len(rows) != 4 || p.PendingCountLocked() != 4 || len(p.lanes.related) != 2 || len(p.lanes.ordinary) != 2 {
		t.Fatal("lane or observer count")
	}
	got := p.PackTxsWithBytes(2 * int(unsafe.Sizeof(Transaction{})))
	if !reflect.DeepEqual(got, []*Transaction{head, ordinary}) {
		t.Fatal("FIFO/byte limit changed")
	}
	if got = p.PackTxs(10); !reflect.DeepEqual(got, []*Transaction{creditSource}) {
		t.Fatal("new-route source debit incorrectly held")
	}
	p.ReleaseMigrationAccount(a)
	if got = p.PackTxs(10); !reflect.DeepEqual(got, []*Transaction{held}) {
		t.Fatal("release lost work")
	}
}

func TestRemovalJournalRejectsMissingAndDuplicateIdentities(t *testing.T) {
	for _, kind := range []string{"missing", "duplicate", "empty"} {
		t.Run(kind, func(t *testing.T) {
			p, _, journal, _ := completedFixture(t, 1)
			tx := p.migrationPendingLocked()[0]
			records := [][]byte{tx.Encode()}
			if kind == "missing" {
				copyTx := DecodeTx(tx.Encode())
				copyTx.Nonce += 1000
				records = [][]byte{copyTx.Encode()}
			}
			if kind == "duplicate" {
				records = append(records, records[0])
			}
			if kind == "empty" {
				records = nil
			}
			s := migrationQueueSnapshot{Accounts: p.migrationJournal.accounts, Sequence: 2, Kind: "remove", Transactions: records}
			raw, _ := json.Marshal(s)
			if err := os.WriteFile(filepath.Join(journal, "00000000000000000002.queue.json"), raw, 0600); err != nil {
				t.Fatal(err)
			}
			fresh := NewTxPool()
			if err := fresh.EnableGroupMigrationQueueJournal(journal, p.migrationJournal.accounts); err == nil {
				t.Fatal("invalid remove generation accepted")
			}
			if fresh.GetTxQueueLen() != 0 || fresh.migrationJournal != nil || fresh.lanes != nil {
				t.Fatal("failed recovery published partial state")
			}
		})
	}
}

func TestRemovalAuditCountsReplayNotLastDelta(t *testing.T) {
	a := "000000000000000000000000000000000000000a"
	p, dir := NewTxPool(), t.TempDir()
	if err := p.EnableMigrationQueueJournal(dir, a); err != nil {
		t.Fatal(err)
	}
	x, y := isolationTx(a, 1), isolationTx(a, 2)
	p.AddTxs2Pool([]*Transaction{x, y})
	p.journalGate.Lock()
	err := p.persistMigrationRemovalLocked([][]byte{x.Encode()})
	p.journalGate.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	audit, err := AuditMigrationQueue(dir, a)
	if err != nil || audit.Pending != 1 || audit.Boundary.Generation != 3 {
		t.Fatal("audit did not replay removal", audit, err)
	}
	fresh := NewTxPool()
	if err = fresh.EnableMigrationQueueJournal(dir, a); err != nil || fresh.GetTxQueueLen() != 1 || fresh.migrationPendingLocked()[0].Nonce != 2 {
		t.Fatal("audit/recovery disagree", err)
	}
}

func TestDrainPublicationDoesNotLockOrdinaryLane(t *testing.T) {
	a, b, c := "000000000000000000000000000000000000000a", "000000000000000000000000000000000000000b", "000000000000000000000000000000000000000c"
	p := NewTxPool()
	if err := p.EnableGroupMigrationQueueJournal(t.TempDir(), []string{a, b}); err != nil {
		t.Fatal(err)
	}
	tx := isolationTx(c, 1)
	tx.Recipient = a
	tx.RoutingFrozen = true
	tx.RoutedRecipientShard = 1
	p.AddTx2Pool(tx)
	if len(p.PackTxs(1)) != 1 {
		t.Fatal("ordinary source debit blocked")
	}
	// Replay the legacy writer's broad journal: new writers intentionally do
	// not persist this ordinary debit. Drain must still normalize old files.
	seedLegacyOrdinaryDelta(t, p, tx)
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	p.migrationJournalPublish = func(path string, raw []byte) error {
		close(started)
		<-release
		return PublishSourcePrepareFile(path, raw)
	}
	go func() { _, err := p.GroupDrainSnapshot([]string{a, b}); done <- err }()
	awaitIsolation(t, started, "drain never reached publish")
	progress := make(chan struct{})
	go func() {
		p.AddTx2Pool(isolationTx(c, 2))
		if len(p.PackTxs(10)) != 1 {
			t.Error("ordinary work missing")
		}
		close(progress)
	}()
	awaitIsolation(t, progress, "drain I/O blocked ordinary lane")
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain stuck")
	}
}
