package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// HandoffPublisher remembers successful durable publications in this process.
// The existing trusted immutable-outbox assumption still applies. Each warm
// call checks bytes and file/directory identity; this is not a malicious-writer
// or filesystem rollback defense. No disk marker is used to bypass restart sync.
type HandoffPublisher struct {
	mu                      sync.Mutex
	directory               string
	maxEntries              int
	entries                 map[string]durableHandoffEntry
	persist                 func(string, []byte) error
	durableCalls, cacheHits uint64
}

type durableHandoffEntry struct {
	file, parent os.FileInfo
	digest       [32]byte
}

func NewHandoffPublisher(directory string, maxEntries int) *HandoffPublisher {
	return &HandoffPublisher{directory: directory, maxEntries: maxEntries,
		entries: make(map[string]durableHandoffEntry), persist: PublishSourcePrepareFile}
}

func samePublishedFile(a, b os.FileInfo) bool {
	return os.SameFile(a, b) && a.Size() == b.Size() && a.Mode() == b.Mode() && a.ModTime().Equal(b.ModTime())
}

// Publish never skips the first successful durable publication. Failure,
// changed metadata, replaced files/directories and cache exhaustion all use the
// original persistence implementation. Exhaustion affects speed, not admission.
func (p *HandoffPublisher) Publish(h *MigrationHandoff) error {
	if err := ValidateMigrationHandoff(h); err != nil {
		return err
	}
	raw, err := json.Marshal(h)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	digest := sha256.Sum256(raw)
	name := filepath.Join(p.directory, h.ID+".handoff.json")
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, hit := p.entries[h.ID]; hit {
		// Discard before checking: no error path leaves a durable-success entry.
		delete(p.entries, h.ID)
		before, e1 := os.Lstat(name)
		parent, e2 := os.Stat(p.directory)
		if e1 == nil && e2 == nil && before.Mode().IsRegular() &&
			entry.digest == digest && samePublishedFile(entry.file, before) && os.SameFile(entry.parent, parent) {
			current, readErr := os.ReadFile(name)
			after, statErr := os.Lstat(name)
			parentAfter, parentErr := os.Stat(p.directory)
			if readErr == nil && statErr == nil && parentErr == nil && bytes.Equal(current, raw) &&
				samePublishedFile(before, after) && os.SameFile(parent, parentAfter) {
				p.entries[h.ID] = entry
				p.cacheHits++
				return nil
			}
		}
	}
	p.durableCalls++
	if err := p.persist(name, raw); err != nil {
		return err
	}
	if len(p.entries) >= p.maxEntries {
		return nil
	}
	before, err := os.Lstat(name)
	if err != nil {
		return err
	}
	parent, err := os.Stat(p.directory)
	if err != nil {
		return err
	}
	// Preserve the old publisher's behavior for nonregular paths, without caching.
	if !before.Mode().IsRegular() {
		return nil
	}
	current, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	after, err := os.Lstat(name)
	if err != nil {
		return err
	}
	parentAfter, err := os.Stat(p.directory)
	if err != nil {
		return err
	}
	if !bytes.Equal(current, raw) || !samePublishedFile(before, after) || !os.SameFile(parent, parentAfter) {
		return fmt.Errorf("handoff changed after durable publication")
	}
	p.entries[h.ID] = durableHandoffEntry{file: after, parent: parentAfter, digest: digest}
	return nil
}

// PublicationCounts is read-only accounting, not synchronous per-record logging.
func (p *HandoffPublisher) PublicationCounts() (durableCalls, cacheHits uint64, entries int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.durableCalls, p.cacheHits, len(p.entries)
}
