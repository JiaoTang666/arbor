package pbft_all

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type retirementEvidenceBinding struct {
	path                      string
	record                    [32]byte
	height, shard, node, view uint64
}

// One process-local I/O memo, not a chain or migration state machine. It holds
// only the current retirement/path/leader binding, so historical groups cannot
// grow the cache. Restart/new leader must prove its own durable publication.
type retirementEvidencePublication struct {
	mu           sync.Mutex
	binding      retirementEvidenceBinding
	raw          []byte
	file, parent os.FileInfo
	durable      bool
}

func sameRetirementEvidenceFile(a, b os.FileInfo) bool {
	return a != nil && b != nil && os.SameFile(a, b) && a.Mode() == b.Mode() &&
		a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}

// Check exact bytes as well as metadata under the EXISTING trusted immutable
// evidence-file contract. Neither file existence nor a previous failed Sync is
// a durable-success entry. Replacement/deletion uses the original publisher.
func retirementEvidenceFile(path string, raw []byte) (os.FileInfo, os.FileInfo, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return nil, nil, err
	}
	current, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	after, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	parentAfter, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return nil, nil, err
	}
	if !bytes.Equal(current, raw) || !sameRetirementEvidenceFile(before, after) || !os.SameFile(parent, parentAfter) {
		return nil, nil, fmt.Errorf("retirement evidence changed during verification")
	}
	return after, parentAfter, nil
}

func (p *retirementEvidencePublication) publish(binding retirementEvidenceBinding, build func() ([]byte, error), persist func(string, []byte) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.raw == nil || p.binding != binding {
		p.binding, p.raw, p.file, p.parent, p.durable = binding, nil, nil, nil, false
		raw, err := build()
		if err != nil {
			return err
		}
		p.raw = append([]byte(nil), raw...)
	}
	if p.durable {
		file, parent, err := retirementEvidenceFile(binding.path, p.raw)
		if err == nil && sameRetirementEvidenceFile(p.file, file) && os.SameFile(p.parent, parent) {
			return nil // No historical block traversal or durability barrier.
		}
		p.durable = false
	}
	// An uncertain result retains the exact attempted bytes for this binding.
	// The original publisher re-syncs an identical visible final link on retry.
	if err := persist(binding.path, p.raw); err != nil {
		return err
	}
	file, parent, err := retirementEvidenceFile(binding.path, p.raw)
	if err != nil {
		return err
	}
	if file.Mode().IsRegular() {
		p.file, p.parent, p.durable = file, parent, true
	}
	return nil
}
