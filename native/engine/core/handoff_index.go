package core

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
)

// Optimization for the existing trusted, immutable, append-only outbox.
// Metadata checks are NOT a defense against a malicious writer preserving file
// metadata. Consensus execution and ACK validation remain independent.
type HandoffIndex struct {
	mu             sync.Mutex
	auditMu        sync.Mutex
	auditConfirmed map[string][32]byte
	directory      string
	limit, bytes   int64
	entries        map[string]indexedHandoff
	sequence       uint64
}
type indexedHandoff struct {
	info    os.FileInfo
	records []MigrationHandoff
}
type HandoffIndexObservation struct {
	Sequence   uint64    `json:"observation_sequence"`
	StartedAt  time.Time `json:"observed_start_at"`
	FinishedAt time.Time `json:"observed_end_at"`
	DurationNS int64     `json:"duration_ns"`
	Files      int       `json:"files_scanned"`
	CacheHits  int       `json:"cache_hits"`
	Validated  int       `json:"records_validated"`
	BytesRead  int64     `json:"bytes_read"`
	CacheBytes int64     `json:"cache_payload_accounted_bytes"`
	Returned   int       `json:"records_returned"`
}

func NewHandoffIndex(directory string, maxBytes int64) *HandoffIndex {
	return &HandoffIndex{directory: directory, limit: maxBytes, entries: map[string]indexedHandoff{}}
}

// Load scans names/metadata but only reads and validates uncached payloads.
// Capacity exhaustion falls back to validation, never drops or blocks input.
// Returned records are owned copies. A fresh index rebuilds from disk.
func (x *HandoffIndex) Load(accept func(*MigrationHandoff) bool) (result []*MigrationHandoff, obs HandoffIndexObservation, err error) {
	start := time.Now()
	obs.StartedAt = start.UTC()
	x.mu.Lock()
	defer x.mu.Unlock()
	x.sequence++
	obs.Sequence = x.sequence
	defer func() {
		obs.FinishedAt = time.Now().UTC()
		obs.DurationNS = time.Since(start).Nanoseconds()
		obs.CacheBytes = x.bytes
		obs.Returned = len(result)
		if err != nil {
			result = nil
			obs.Returned = 0
		}
	}()
	files, e := os.ReadDir(x.directory)
	if e != nil {
		return nil, obs, e
	}
	visited := map[string]bool{}
	identities := map[string]MigrationHandoff{}
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".handoff.json") && !strings.HasSuffix(f.Name(), ".handoff.json.bundle") {
			continue
		}
		info, e := f.Info()
		if e != nil {
			return nil, obs, e
		}
		if !info.Mode().IsRegular() || info.Size() > artifactBatchLimit {
			return nil, obs, fmt.Errorf("nonregular handoff file %s", f.Name())
		}
		obs.Files++
		visited[f.Name()] = true
		entry, hit := x.entries[f.Name()]
		if hit {
			if !os.SameFile(entry.info, info) || entry.info.Size() != info.Size() || !entry.info.ModTime().Equal(info.ModTime()) {
				return nil, obs, fmt.Errorf("immutable handoff changed: %s", f.Name())
			}
			obs.CacheHits++
		} else {
			path := filepath.Join(x.directory, f.Name())
			raw, e := os.ReadFile(path)
			if e != nil {
				return nil, obs, e
			}
			obs.BytesRead += int64(len(raw))
			after, e := os.Stat(path)
			if e != nil {
				return nil, obs, e
			}
			if !os.SameFile(info, after) || info.Size() != after.Size() || !info.ModTime().Equal(after.ModTime()) {
				return nil, obs, fmt.Errorf("handoff changed during read")
			}
			artifacts, e := decodeMigrationArtifacts(f.Name(), raw, ".handoff.json")
			if e != nil {
				return nil, obs, e
			}
			for _, id := range artifacts.IDs() {
				var h MigrationHandoff
				if e = json.Unmarshal(artifacts[id], &h); e != nil {
					return nil, obs, e
				}
				if e = ValidateMigrationHandoff(&h); e != nil {
					return nil, obs, e
				}
				entry.records = append(entry.records, h)
				obs.Validated++
			}
			entry.info = after
			cost := int64(len(raw) + 512*len(entry.records)) // conservative accounting, not RSS
			if cost <= x.limit-x.bytes {
				x.entries[f.Name()] = entry
				x.bytes += cost
			}
		}
		for _, record := range entry.records {
			if prior, ok := identities[record.ID]; ok {
				if !reflect.DeepEqual(prior, record) {
					return nil, obs, fmt.Errorf("conflicting handoff %s", record.ID)
				}
				continue
			}
			identities[record.ID] = record
			// Return owned slices, including to the predicate.
			h := record
			h.Original = append([]byte(nil), h.Original...)
			h.Execution = append([]byte(nil), h.Execution...)
			h.GroupAccounts = append([]string(nil), h.GroupAccounts...)
			if accept == nil || accept(&h) {
				result = append(result, &h)
			}
		}
	}
	for name := range x.entries {
		if !visited[name] {
			return nil, obs, fmt.Errorf("immutable handoff removed: %s", name)
		}
	}
	return result, obs, nil
}
