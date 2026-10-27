package ownership

import (
	"testing"
	"time"
)

func TestAuthorizedBindingsOnlyReadVerifiedPrefix(t *testing.T) {
	f, view, journal := runtimeFixture(t)
	account := testAccount(1)
	bindings := []Binding{{testAccount(1000), 0}, {account, 1}}
	appendRuntime(t, journal, runtimeCertificate(t, f, 0, 2, 4, account), true)
	// A refresh already in progress must not make the queue's pure check wait
	// for journal I/O, or let it adopt a complete but still unverified record.
	view.refreshMu.Lock()
	done := make(chan error, 1)
	go func() { done <- view.CheckAuthorizedBindings(bindings, 2) }()
	select {
	case err := <-done:
		view.refreshMu.Unlock()
		if err == nil || view.Version() != 0 {
			t.Fatal("pure check adopted an unverified journal record")
		}
	case <-time.After(5 * time.Second):
		view.refreshMu.Unlock()
		t.Fatal("pure check waited for the journal refresh gate")
	}
	if err := view.Refresh(); err != nil {
		t.Fatal(err)
	}
	if err := view.CheckAuthorizedBindings(bindings, 2); err != nil {
		t.Fatal("refreshed bindings were not usable", err)
	}
	if err := view.CheckAuthorizedBindings(bindings, 1); err == nil {
		t.Fatal("future authorization was accepted before its batch")
	}
	bindings[1].Shard = 0
	if err := view.CheckAuthorizedBindings(bindings, 2); err == nil {
		t.Fatal("conflicting owner was accepted")
	}
}

func TestAuthorizedBindingsRemainValidAcrossConcurrentAppend(t *testing.T) {
	f, view, journal := runtimeFixture(t)
	a, b := testAccount(1), testAccount(2)
	appendRuntime(t, journal, runtimeCertificate(t, f, 0, 0, 2, a), true)
	if err := view.Refresh(); err != nil {
		t.Fatal(err)
	}
	appendRuntime(t, journal, runtimeCertificate(t, f, 1, 2, 4, b), true)
	done := make(chan error, 1)
	go func() { done <- view.Refresh() }()
	bindings := []Binding{{testAccount(1000), 0}, {a, 1}}
	for i := 0; i < 100; i++ {
		if err := view.CheckAuthorizedBindings(bindings, 0); err != nil {
			t.Fatal("verified owner changed during append", err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if view.Version() != 2 || view.CheckAuthorizedBindings([]Binding{{b, 1}}, 2) != nil {
		t.Fatal("concurrent append failed to publish its verified owner")
	}
}
