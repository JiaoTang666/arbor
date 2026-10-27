package core

import (
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func publisherFixture(t *testing.T, nonce uint64) *MigrationHandoff {
	t.Helper()
	a := "0000000000000000000000000000000000000010"
	tx := NewTransaction(a, a, big.NewInt(1), nonce, time.Unix(1, 0))
	tx.RoutingFrozen = true
	h, err := BuildMigrationHandoff(tx, "publisher-test", a, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func requirePublish(t *testing.T, p *HandoffPublisher, h *MigrationHandoff) {
	t.Helper()
	if err := p.Publish(h); err != nil {
		t.Fatal(err)
	}
}

func TestHandoffPublisherWarmAndRestart(t *testing.T) {
	dir := t.TempDir()
	h := publisherFixture(t, 1)
	p := NewHandoffPublisher(dir, 8)
	for i := 0; i < 20; i++ {
		requirePublish(t, p, h)
	}
	d, c, n := p.PublicationCounts()
	if d != 1 || c != 19 || n != 1 {
		t.Fatal(d, c, n)
	}
	restarted := NewHandoffPublisher(dir, 8)
	requirePublish(t, restarted, h)
	d, c, n = restarted.PublicationCounts()
	if d != 1 || c != 0 || n != 1 {
		t.Fatal("restart bypassed durability", d, c, n)
	}
}

func TestHandoffPublisherUncertainFailureRetries(t *testing.T) {
	p := NewHandoffPublisher(t.TempDir(), 8)
	h := publisherFixture(t, 1)
	real := p.persist
	fail := true
	p.persist = func(path string, raw []byte) error {
		if err := real(path, raw); err != nil {
			return err
		}
		if fail {
			fail = false
			return errors.New("injected uncertain barrier result")
		}
		return nil
	}
	if p.Publish(h) == nil {
		t.Fatal("injected failure ignored")
	}
	if _, _, n := p.PublicationCounts(); n != 0 {
		t.Fatal("failure cached")
	}
	requirePublish(t, p, h)
	requirePublish(t, p, h)
	d, c, n := p.PublicationCounts()
	if d != 2 || c != 1 || n != 1 {
		t.Fatal(d, c, n)
	}
}

func TestHandoffPublisherConflictEvenWithRestoredMtime(t *testing.T) {
	dir := t.TempDir()
	p := NewHandoffPublisher(dir, 8)
	h := publisherFixture(t, 1)
	requirePublish(t, p, h)
	name := filepath.Join(dir, h.ID+".handoff.json")
	info, _ := os.Stat(name)
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] = ' ' // valid JSON, same size, different immutable bytes
	if err = os.WriteFile(name, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(name, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if p.Publish(h) == nil {
		t.Fatal("modified file accepted")
	}
	if _, _, n := p.PublicationCounts(); n != 0 {
		t.Fatal("conflict retained cache entry")
	}
}

func TestHandoffPublisherReplacementAndRemovalResync(t *testing.T) {
	dir := t.TempDir()
	p := NewHandoffPublisher(dir, 8)
	h := publisherFixture(t, 1)
	requirePublish(t, p, h)
	name := filepath.Join(dir, h.ID+".handoff.json")
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(name, name+".old"); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(name, raw, 0600); err != nil {
		t.Fatal(err)
	}
	requirePublish(t, p, h)
	if err = os.Remove(name); err != nil {
		t.Fatal(err)
	}
	requirePublish(t, p, h)
	d, c, n := p.PublicationCounts()
	if d != 3 || c != 0 || n != 1 {
		t.Fatal(d, c, n)
	}
}

func TestHandoffPublisherBoundedFallback(t *testing.T) {
	p := NewHandoffPublisher(t.TempDir(), 1)
	a, b := publisherFixture(t, 1), publisherFixture(t, 2)
	requirePublish(t, p, a)
	requirePublish(t, p, b)
	requirePublish(t, p, b)
	requirePublish(t, p, a)
	d, c, n := p.PublicationCounts()
	if d != 3 || c != 1 || n != 1 {
		t.Fatal(d, c, n)
	}
}

func TestHandoffPublisherDirectoryReplacementResync(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "outbox")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	p := NewHandoffPublisher(dir, 8)
	h := publisherFixture(t, 1)
	requirePublish(t, p, h)
	name := h.ID + ".handoff.json"
	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(dir, filepath.Join(base, "old")); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
		t.Fatal(err)
	}
	requirePublish(t, p, h)
	d, c, n := p.PublicationCounts()
	if d != 2 || c != 0 || n != 1 {
		t.Fatal(d, c, n)
	}
}

func TestHandoffPublisherConcurrentAndInvalid(t *testing.T) {
	p := NewHandoffPublisher(t.TempDir(), 8)
	h := publisherFixture(t, 1)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- p.Publish(h) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	d, c, n := p.PublicationCounts()
	if d != 1 || c != 31 || n != 1 {
		t.Fatal(d, c, n)
	}
	bad := *h
	bad.Execution = []byte("changed")
	if p.Publish(&bad) == nil {
		t.Fatal("invalid payload bypassed validation")
	}
}
