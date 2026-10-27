package core

import (
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestGroupProposalDrainDefersInFlightAdmission(t *testing.T) {
	accounts := []string{strings.Repeat("0", 39) + "a", strings.Repeat("0", 39) + "b"}
	p := NewTxPool()
	dir := t.TempDir()
	if err := p.EnableGroupMigrationQueueJournal(dir, accounts); err != nil {
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
	tx := NewTransaction(accounts[0], accounts[1], big.NewInt(1), 1, time.Unix(1, 0))
	go func() { p.AddTx2Pool(tx); close(admitted) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("admission did not reach disk barrier")
	}
	// Reproduce the original proposal path: it cannot return while admission
	// owns journalGate, even though there is no retirement possible yet.
	blocking := make(chan struct{})
	go func() {
		_, err := p.GroupDrainSnapshot(accounts)
		if err != nil {
			t.Error(err)
		}
		close(blocking)
	}()
	select {
	case <-blocking:
		t.Fatal("old drain crossed pending admission")
	case <-time.After(50 * time.Millisecond):
	}
	start := time.Now()
	type result struct {
		raw    []byte
		detail GroupDrainTiming
		err    error
	}
	ready := make(chan result, 1)
	go func() { raw, d, e := p.TryGroupDrainSnapshot(accounts); ready <- result{raw, d, e} }()
	select {
	case r := <-ready:
		if r.err != nil || r.raw != nil || r.detail.Status != "journal_busy" {
			t.Fatalf("unsafe drain result: %+v", r)
		}
		t.Logf("new drain deferred in %s while old drain remained blocked", time.Since(start))
	case <-time.After(time.Second):
		t.Fatal("proposal waited for admission fsync")
	}
	if len(p.PackTxs(100)) != 0 {
		t.Fatal("undurable admission became visible")
	}
	close(release)
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("admission stuck")
	}
	select {
	case <-blocking:
	case <-time.After(time.Second):
		t.Fatal("blocking drain did not finish")
	}
	raw, detail, err := p.TryGroupDrainSnapshot(accounts)
	if err != nil || raw != nil || detail.Status != "pending" {
		t.Fatalf("pending queue certified empty: %+v %v", detail, err)
	}
	if len(p.PackTxs(100)) != 0 {
		t.Fatal("migration fence lost")
	}
	fresh := NewTxPool()
	if err = fresh.EnableGroupMigrationQueueJournal(dir, accounts); err != nil || fresh.GetTxQueueLen() != 1 {
		t.Fatal("recovery lost held work", err)
	}
}

func TestGroupProposalDrainReadyAndPoolBusy(t *testing.T) {
	accounts := []string{strings.Repeat("0", 39) + "a", strings.Repeat("0", 39) + "b"}
	p := NewTxPool()
	if err := p.EnableGroupMigrationQueueJournal(t.TempDir(), accounts); err != nil {
		t.Fatal(err)
	}
	p.lock.Lock()
	raw, d, err := p.TryGroupDrainSnapshot(accounts)
	p.lock.Unlock()
	if err != nil || raw != nil || d.Status != "pool_busy" {
		t.Fatalf("pool contention not deferred: %+v %v", d, err)
	}
	raw, d, err = p.TryGroupDrainSnapshot(accounts)
	if err != nil || raw == nil || d.Status != "ready" {
		t.Fatalf("idle empty queue did not progress: %+v %v", d, err)
	}
	var s migrationQueueSnapshot
	if err = json.Unmarshal(raw, &s); err != nil || len(s.Transactions) != 0 || (s.Kind != "" && s.Kind != "full") {
		t.Fatal("bad full empty generation", err)
	}
}
