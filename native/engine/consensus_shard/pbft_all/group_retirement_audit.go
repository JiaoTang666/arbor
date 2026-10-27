package pbft_all

import (
	"blockEmulator/core"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type retirementAuditResult struct {
	binding      string
	audits       map[string]core.HandoffAudit
	files        *retirementAuditFiles
	filesChanged bool
	err          error
	elapsed      time.Duration
}

// One read-only file audit, not a migration state machine. The dedicated index
// prevents the ordinary handoff path waiting on an audit's long index lock.
// Neither the worker nor its result owns chain, pool or journal state.
type retirementAuditLane struct {
	mu        sync.Mutex
	done      chan retirementAuditResult
	closed    bool
	directory string
	index     *core.HandoffIndex
}

func (w *retirementAuditLane) indexFor(directory string) *core.HandoffIndex {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.index == nil || w.directory != directory {
		w.directory = directory
		w.index = core.NewHandoffIndex(directory, 32<<20)
	}
	return w.index
}

func (w *retirementAuditLane) start(binding string, work func() retirementAuditResult) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.done != nil || work == nil {
		return false
	}
	done := make(chan retirementAuditResult, 1)
	w.done = done
	go func() {
		started := time.Now()
		r := work()
		r.binding, r.elapsed = binding, time.Since(started)
		done <- r // A late result cannot block shutdown or access closed storage.
	}()
	return true
}

func (w *retirementAuditLane) poll() (bool, *retirementAuditResult) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.done == nil {
		return false, nil
	}
	select {
	case result := <-w.done:
		w.done = nil
		return false, &result
	default:
		return true, nil
	}
}

func (w *retirementAuditLane) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true // No filesystem wait under the PBFT stop/commit locks.
}

func retirementAuditBinding(p *core.GroupPreparation, receipt []byte, directory string, journalDirs ...string) (string, error) {
	raw, err := json.Marshal(struct {
		Preparation        *core.GroupPreparation
		Activation         []byte
		Directory          string
		JournalDirectories []string `json:",omitempty"`
	}{p, receipt, directory, journalDirs})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

// Metadata is a freshness check under the EXISTING trusted immutable-file
// contract, not a cryptographic proof against metadata-preserving tampering.
// Compare exactly the final artifact kinds read by AuditMembers, never temp
// files or directory mtime alone. Payload validation still uses AuditMembers.
type retirementAuditFiles struct {
	directory          os.FileInfo
	files              map[string]os.FileInfo
	journalDirectories map[string]os.FileInfo
}

func readRetirementAuditFiles(directory string, journalDirs ...string) (*retirementAuditFiles, error) {
	info, err := os.Stat(directory)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	snapshot := &retirementAuditFiles{directory: info, files: make(map[string]os.FileInfo), journalDirectories: make(map[string]os.FileInfo)}
	for _, entry := range entries {
		name := entry.Name()
		if !(strings.HasSuffix(name, ".handoff.json") || strings.HasSuffix(name, ".handoff.json.bundle") ||
			strings.HasPrefix(name, "batch-confirmed-") && strings.HasSuffix(name, ".json") ||
			strings.HasSuffix(name, ".confirmed.json") || strings.HasSuffix(name, ".relay-confirmed.json")) {
			continue
		}
		file, err := entry.Info()
		if err != nil {
			return nil, err
		}
		snapshot.files[filepath.Join(directory, name)] = file
	}
	for _, journalDir := range journalDirs {
		if journalDir == "" {
			continue
		}
		journalDir = filepath.Clean(journalDir)
		info, err := os.Stat(journalDir)
		if err != nil {
			return nil, err
		}
		snapshot.journalDirectories[journalDir] = info
		// Full/projection generations can be published by the drain check itself.
		// Track only durable confirmation facts, or normalization would make every
		// completed multi-group audit stale before it can authorize a proposal.
		paths, err := core.CompletedMigrationGenerationFiles(journalDir)
		if err != nil {
			return nil, err
		}
		for _, path := range paths {
			file, err := os.Stat(path)
			if err != nil {
				return nil, err
			}
			snapshot.files[path] = file
		}
	}
	return snapshot, nil
}

func (a *retirementAuditFiles) same(b *retirementAuditFiles) bool {
	if a == nil || b == nil || !os.SameFile(a.directory, b.directory) || len(a.files) != len(b.files) {
		return false
	}
	if len(a.journalDirectories) != len(b.journalDirectories) {
		return false
	}
	for path, first := range a.journalDirectories {
		last := b.journalDirectories[path]
		if last == nil || !os.SameFile(first, last) {
			return false
		}
	}
	for name, first := range a.files {
		last := b.files[name]
		if last == nil || !os.SameFile(first, last) || first.Mode() != last.Mode() ||
			first.Size() != last.Size() || !first.ModTime().Equal(last.ModTime()) {
			return false
		}
	}
	return true
}

func readRetirementAudit(index *core.HandoffIndex, directory string, members map[string]string, journalDirs ...string) retirementAuditResult {
	before, err := readRetirementAuditFiles(directory, journalDirs...)
	if err != nil {
		return retirementAuditResult{err: err}
	}
	audits, auditErr := index.AuditMembers(members, journalDirs...)
	after, err := readRetirementAuditFiles(directory, journalDirs...)
	if err != nil {
		return retirementAuditResult{err: err}
	}
	return retirementAuditResult{audits: audits, files: after, filesChanged: !before.same(after), err: auditErr}
}
