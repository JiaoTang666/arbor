package pbft_all

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func publicationFinish(t *testing.T, p *sourcePublicationLane) *publicationResult {
	t.Helper()
	limit := time.Now().Add(2 * time.Second)
	for time.Now().Before(limit) {
		_, r := p.poll()
		if r != nil {
			return r
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("publication did not complete")
	return nil
}

func TestSourcePublicationSingleFlightDoesNotWaitForIO(t *testing.T) {
	var p sourcePublicationLane
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	if !p.start([]string{"pending"}, func() error { close(entered); <-release; return nil }) {
		t.Fatal("start")
	}
	<-entered
	for i := 0; i < 100; i++ {
		busy, r := p.poll()
		if !busy || r != nil || !p.blocks("pending") || p.blocks("previously-durable") {
			t.Fatal("incorrect pending coverage")
		}
		if p.start([]string{"second"}, func() error { t.Error("second worker started"); return nil }) {
			t.Fatal("unbounded queue")
		}
	}
	close(release)
	if r := publicationFinish(t, &p); r.err != nil || p.blocks("pending") {
		t.Fatal("success not collected", r.err)
	}
}

func TestSourcePublicationUncertainFailureKeepsExactJob(t *testing.T) {
	var p sourcePublicationLane
	var calls atomic.Int32
	if !p.start([]string{"visible-before-sync"}, func() error {
		if calls.Add(1) == 1 {
			return errors.New("link visible; barrier failed")
		}
		return nil
	}) {
		t.Fatal("start")
	}
	r := publicationFinish(t, &p)
	if r.err == nil || !p.blocks("visible-before-sync") {
		t.Fatal("uncertain file unlocked")
	}
	if p.start(nil, func() error { return nil }) {
		t.Fatal("failed immutable job overwritten")
	}
	r = publicationFinish(t, &p)
	if r.err != nil || calls.Load() != 2 || p.blocks("visible-before-sync") {
		t.Fatal("same job not durably retried")
	}
}

func TestSourcePublicationStopDoesNotWaitOrStartNewWork(t *testing.T) {
	var p sourcePublicationLane
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	p.start([]string{"x"}, func() error { close(entered); <-release; close(finished); return nil })
	<-entered
	p.close()
	if p.start(nil, func() error { return nil }) {
		t.Fatal("job after close")
	}
	if busy, r := p.poll(); busy || r != nil {
		t.Fatal("closed lane retry")
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("worker needs a proposal consumer")
	}
}
