package core

import (
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"
)

func isolationTx(account string, nonce uint64) *Transaction {
	return NewTransaction(account, account, big.NewInt(1), nonce, time.Unix(1, 0))
}

func awaitIsolation(t *testing.T, ch <-chan struct{}, reason string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(reason)
	}
}

func TestUnrelatedAdmissionBypassesBlockedMixedJournal(t *testing.T) {
	for _, single := range []bool{true, false} {
		t.Run(map[bool]string{true: "single", false: "batch"}[single], func(t *testing.T) {
			account := strings.Repeat("0", 39) + "a"
			other := strings.Repeat("0", 39) + "b"
			p, dir := NewTxPool(), t.TempDir()
			if err := p.EnableMigrationQueueJournal(dir, account); err != nil {
				t.Fatal(err)
			}
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
				awaitIsolation(t, done, "mixed admission did not finish")
			}()
			p.migrationJournalPublish = func(path string, raw []byte) error {
				close(started)
				<-release
				return PublishSourcePrepareFile(path, raw)
			}
			held, companion := isolationTx(account, 1), isolationTx(other, 2)
			go func() { p.AddTxs2Pool([]*Transaction{held, companion}); close(done) }()
			awaitIsolation(t, started, "mixed admission did not reach fsync")
			ordinary := isolationTx(other, 3)
			fastDone := make(chan struct{})
			go func() {
				if single {
					p.AddTx2Pool(ordinary)
				} else {
					p.AddTxs2Pool([]*Transaction{ordinary})
				}
				close(fastDone)
			}()
			awaitIsolation(t, fastDone, "unrelated admission blocked by migration journal")
			packed := p.PackTxs(100)
			if len(packed) != 1 || packed[0] != ordinary {
				t.Fatal("mixed batch became visible before durability")
			}
			close(release)
			awaitIsolation(t, done, "mixed admission stuck")
			packed = p.PackTxs(100)
			if len(packed) != 1 || packed[0] != companion || p.GetTxQueueLen() != 1 {
				t.Fatal("mixed batch split/lost or migration hold bypassed")
			}
			fresh := NewTxPool()
			if err := fresh.EnableMigrationQueueJournal(dir, account); err != nil || fresh.GetTxQueueLen() != 1 {
				t.Fatal("migration recovery lost work", err)
			}
		})
	}
}

func TestAdmissionOnBothSidesOfJournalEnable(t *testing.T) {
	account := strings.Repeat("0", 39) + "a"
	p, dir := NewTxPool(), t.TempDir()
	p.AddTxs2Pool([]*Transaction{isolationTx(account, 1)})
	started, release, enabled, admitted := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	calls := 0
	p.migrationJournalPublish = func(path string, raw []byte) error {
		calls++
		if calls == 1 {
			close(started)
			<-release
		}
		return PublishSourcePrepareFile(path, raw)
	}
	go func() {
		if err := p.EnableMigrationQueueJournal(dir, account); err != nil {
			t.Error(err)
		}
		close(enabled)
	}()
	awaitIsolation(t, started, "enable did not capture pre-existing queue")
	go func() { p.AddTx2Pool(isolationTx(account, 2)); close(admitted) }()
	select {
	case <-admitted:
		t.Fatal("admission crossed uncommitted journal enable")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	awaitIsolation(t, enabled, "enable stuck")
	awaitIsolation(t, admitted, "admission stuck")
	if calls != 2 || len(p.PackTxs(100)) != 0 {
		t.Fatal("post-enable admission not journaled or hold absent")
	}
	fresh := NewTxPool()
	if err := fresh.EnableMigrationQueueJournal(dir, account); err != nil || fresh.GetTxQueueLen() != 2 {
		t.Fatal("enable race lost durable occurrence", err)
	}
}

func TestFailedMixedAdmissionDoesNotRemoveUnrelatedWork(t *testing.T) {
	account := strings.Repeat("0", 39) + "a"
	other := strings.Repeat("0", 39) + "b"
	p, dir := NewTxPool(), t.TempDir()
	if err := p.EnableMigrationQueueJournal(dir, account); err != nil {
		t.Fatal(err)
	}
	ordinary := isolationTx(other, 1)
	p.AddTx2Pool(ordinary)
	p.migrationJournalPublish = func(string, []byte) error { return errors.New("injected disk failure") }
	func() {
		defer func() {
			if recover() == nil {
				t.Error("durability error silently accepted")
			}
		}()
		p.AddTxs2Pool([]*Transaction{isolationTx(account, 2), isolationTx(other, 3)})
	}()
	packed := p.PackTxs(100)
	if len(packed) != 1 || packed[0] != ordinary || p.GetTxQueueLen() != 0 {
		t.Fatal("failed mixed batch became visible or removed ordinary work")
	}
	fresh := NewTxPool()
	if err := fresh.EnableMigrationQueueJournal(dir, account); err != nil || fresh.GetTxQueueLen() != 0 {
		t.Fatal("failed publication changed durable generation", err)
	}
}
