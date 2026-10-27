package pbft_all

import (
	"sync"
	"time"
)

type publicationResult struct {
	err     error
	elapsed time.Duration
}

// One owned immutable job, never a queue of goroutines. Publication jobs touch
// only transport files. Confirmation jobs may use the thread-safe TxPool API,
// but neither kind reads chain/trie/node state or holds a PBFT lock. Jobs may
// outlive a proposal, not the process; only durable results authorize removal.
type sourcePublicationLane struct {
	mu     sync.Mutex
	job    func() error
	done   chan publicationResult
	ids    map[string]bool
	closed bool
}

func (p *sourcePublicationLane) launchLocked() {
	job := p.job
	done := make(chan publicationResult, 1)
	p.done = done
	go func() {
		start := time.Now()
		err := job()
		done <- publicationResult{err: err, elapsed: time.Since(start)}
	}()
}

func (p *sourcePublicationLane) start(ids []string, job func() error) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.job != nil || job == nil {
		return false
	}
	p.job = job
	p.ids = make(map[string]bool, len(ids))
	for _, id := range ids {
		p.ids[id] = true
	}
	p.launchLocked()
	return true
}

// An uncertain failure retains the exact job. The next proposal retries its
// durability barriers instead of treating an already-visible file as success.
// While busy/error, the source must not consume ACKs for this publication.
func (p *sourcePublicationLane) poll() (busy bool, finished *publicationResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.job == nil {
		return false, nil
	}
	if p.done == nil {
		p.launchLocked()
		return true, nil
	}
	select {
	case result := <-p.done:
		p.done = nil
		if result.err == nil {
			p.job = nil
			p.ids = nil
			return false, &result
		}
		return true, &result
	default:
		return true, nil
	}
}

func (p *sourcePublicationLane) blocks(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ids[id]
}

func (p *sourcePublicationLane) active() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.job != nil
}

func (p *sourcePublicationLane) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true // never wait for filesystem I/O while stopping consensus
}

func (n *PbftConsensusNode) sourcePublicationLane() *sourcePublicationLane {
	value, _ := n.sourcePrepareSeen.LoadOrStore("group-source-publication-lane-v1", &sourcePublicationLane{})
	return value.(*sourcePublicationLane)
}

// All source confirmation paths share ONE worker per node/pool, not one per
// group. Its inputs are copied before launch; uncertain failures retain them.
func (n *PbftConsensusNode) sourceConfirmationLane() *sourcePublicationLane {
	value, _ := n.sourcePrepareSeen.LoadOrStore("source-confirmation-lane-v1", &sourcePublicationLane{})
	return value.(*sourcePublicationLane)
}
