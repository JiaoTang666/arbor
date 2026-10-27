package ownership

import (
	"os"
	"testing"
	"time"
)

func TestRuntimeJournalMeasuredConcurrentRefreshAndPartialTail(t *testing.T) {
	f, view, journal := runtimeFixture(t)
	a, b, c := testAccount(1), testAccount(2), testAccount(3)
	first := runtimeCertificate(t, f, 0, 0, 2, a)
	prefix := len(appendRuntime(t, journal, first, true))
	first.Signatures[0], first.Signatures[2] = first.Signatures[2], first.Signatures[0]
	prefix += len(appendRuntime(t, journal, first, true))
	prefix += len(appendRuntime(t, journal, runtimeCertificate(t, f, 1, 2, 4, b), true))
	appendRuntime(t, journal, runtimeCertificate(t, f, 2, 4, 6, c), false)
	type result struct {
		metrics RefreshMetrics
		err     error
	}
	const workers = 8
	results := make(chan result, workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			metrics, err := view.RefreshMeasured()
			view.Status()
			view.Version()
			results <- result{metrics, err}
		}()
	}
	var consumed uint64
	for worker := 0; worker < workers; worker++ {
		r := <-results
		if r.err != nil {
			t.Error(r.err)
		}
		consumed += r.metrics.Certificates
	}
	if consumed != 3 || view.Version() != 2 || view.journalOffset != int64(prefix) {
		t.Fatalf("concurrent prefix/replay mismatch: consumed=%d version=%d offset=%d", consumed, view.Version(), view.journalOffset)
	}
	if _, exists := view.Owner(c); exists {
		t.Fatal("incomplete tail authorized an owner")
	}
	if err := view.CheckPlanDependency(b, 2, 2, digestBytes([]byte(b)), 2); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(journal, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteString("\n")
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("complete partial tail: write=%v close=%v", writeErr, closeErr)
	}
	metrics, err := view.RefreshMeasured()
	if err != nil || metrics.Certificates != 1 || view.Version() != 3 {
		t.Fatalf("completed tail not consumed exactly once: metrics=%+v err=%v", metrics, err)
	}
	info, err := os.Stat(journal)
	if err != nil || view.journalOffset != info.Size() {
		t.Fatalf("completed tail offset mismatch: %v", err)
	}
}

func TestRuntimeJournalMeasuredRejectsWithoutSkippingPrefix(t *testing.T) {
	for _, mode := range []string{"bad-signature", "wrong-base"} {
		t.Run(mode, func(t *testing.T) {
			f, view, journal := runtimeFixture(t)
			a, badAccount, laterAccount := testAccount(1), testAccount(2), testAccount(3)
			prefix := len(appendRuntime(t, journal, runtimeCertificate(t, f, 0, 0, 2, a), true))
			bad := runtimeCertificate(t, f, 1, 2, 4, badAccount)
			if mode == "bad-signature" {
				bad.Signatures[0].Value[0] ^= 1
			} else {
				bad = runtimeCertificate(t, f, 2, 2, 4, badAccount)
			}
			appendRuntime(t, journal, bad, true)
			appendRuntime(t, journal, runtimeCertificate(t, f, 1, 2, 4, laterAccount), true)
			for attempt := 0; attempt < 2; attempt++ {
				metrics, err := view.RefreshMeasured()
				wantConsumed := uint64(1 - attempt)
				if err == nil || metrics.Certificates != wantConsumed {
					t.Fatalf("invalid record not retained: metrics=%+v err=%v", metrics, err)
				}
				view.mu.RLock()
				unchanged := view.version == 1 && view.journalOffset == int64(prefix) &&
					len(view.proposals) == 1 && len(view.evidence) == 1 && view.ownerCountLocked() == len(f.genesis)+1
				view.mu.RUnlock()
				if !unchanged {
					t.Fatal("rejected record changed the authenticated prefix")
				}
				if _, exists := view.Owner(badAccount); exists {
					t.Fatal("rejected record authorized an owner")
				}
				if _, exists := view.Owner(laterAccount); exists {
					t.Fatal("refresh skipped the rejected record")
				}
				if err := view.Check(a, 1); err != nil {
					t.Fatalf("bad suffix blocked a cached authorized owner: %v", err)
				}
			}
		})
	}
}

func TestRuntimeJournalMeasuredRejectDoesNotNeedOwnerWriterLock(t *testing.T) {
	for _, mode := range []string{"bad-json", "bad-signature"} {
		t.Run(mode, func(t *testing.T) {
			f, view, journal := runtimeFixture(t)
			if mode == "bad-json" {
				if err := os.WriteFile(journal, []byte("{invalid}\n"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				cert := runtimeCertificate(t, f, 0, 0, 2, testAccount(1))
				cert.Signatures[0].Value[0] ^= 1
				appendRuntime(t, journal, cert, true)
			}
			view.mu.RLock()
			done := make(chan error, 1)
			go func() {
				_, err := view.RefreshMeasured()
				done <- err
			}()
			select {
			case err := <-done:
				view.mu.RUnlock()
				if err == nil {
					t.Fatal("invalid record was accepted")
				}
			case <-time.After(5 * time.Second):
				view.mu.RUnlock()
				t.Fatal("read/decode or verification waited for the owner-table writer lock")
			}
			if view.Version() != 0 || view.journalOffset != 0 {
				t.Fatal("rejection changed owner state or journal offset")
			}
		})
	}
}
