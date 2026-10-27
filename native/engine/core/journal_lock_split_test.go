package core

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestJournalFsyncDoesNotHoldRelayPoolLockAndDrainWaits(t *testing.T) {
	account := strings.Repeat("0", 39) + "a"
	p := NewTxPool()
	dir := t.TempDir()
	if err := p.EnableMigrationQueueJournal(dir, account); err != nil {
		t.Fatal(err)
	}
	started, release, admitted := make(chan struct{}), make(chan struct{}), make(chan struct{})
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
	tx := NewTransaction(account, account, big.NewInt(1), 1, time.Unix(1, 0))
	go func() {
		p.AddTx2Pool(tx)
		close(admitted)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("journal publish was not reached")
	}
	poolReady := make(chan int, 1)
	go func() {
		p.ReplaceRelayPool(map[uint64][]*Transaction{1: {tx}})
		p.ClearRelayPool()
		poolReady <- p.GetTxQueueLen()
	}()
	select {
	case length := <-poolReady:
		if length != 0 {
			t.Fatal("undurable admission became visible")
		}
	case <-time.After(time.Second):
		t.Fatal("journal fsync held the relay/queue memory lock")
	}
	drained := make(chan error, 1)
	go func() {
		_, err := p.MigrationDrainBoundary(account)
		drained <- err
	}()
	select {
	case <-drained:
		t.Fatal("retirement crossed an in-flight durable admission")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("admission did not complete after fsync")
	}
	if length := p.GetTxQueueLen(); length != 1 {
		t.Fatal("durable admission missing from queue", length)
	}
	if err := <-drained; err == nil {
		t.Fatal("retirement accepted a nonempty durable queue")
	}
	fresh := NewTxPool()
	if err := fresh.EnableMigrationQueueJournal(dir, account); err != nil || fresh.GetTxQueueLen() != 1 {
		t.Fatal("recovery lost admitted work", err)
	}
}
