package core

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Index for the existing trusted, append-only transport. Metadata is checked
// on EVERY poll; only immutable payload decoding is cached. This is not a
// defense against a malicious writer preserving inode/size/mtime. Consumers
// still validate the transaction, ownership, activation and completion ACK.
type MigrationArtifactIndex struct {
	mu              sync.Mutex
	directory, kind string
	limit, used     int64
	files           map[string]artifactIndexEntry
}
type artifactIndexEntry struct {
	info    os.FileInfo
	records MigrationArtifacts
}
type ArtifactIndexStats struct {
	Files, Hits int
	BytesRead   int64
}

func NewMigrationArtifactIndex(directory, kind string, limit int64) *MigrationArtifactIndex {
	return &MigrationArtifactIndex{directory: directory, kind: kind, limit: limit, files: map[string]artifactIndexEntry{}}
}

func (x *MigrationArtifactIndex) Load() (MigrationArtifacts, ArtifactIndexStats, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	result, stats := MigrationArtifacts{}, ArtifactIndexStats{}
	if !validArtifactKind(x.kind) {
		return nil, stats, fmt.Errorf("unknown artifact kind")
	}
	if x.directory == "" {
		return result, stats, nil
	}
	entries, err := os.ReadDir(x.directory)
	if err != nil {
		return nil, stats, err
	}
	visited := map[string]bool{}
	for _, f := range entries {
		name := f.Name()
		if !(strings.HasPrefix(name, "batch-") && strings.HasSuffix(name, x.kind+".bundle")) && !strings.HasSuffix(name, x.kind) {
			continue
		}
		info, err := f.Info()
		if err != nil {
			return nil, stats, err
		}
		if !info.Mode().IsRegular() || info.Size() > artifactBatchLimit {
			return nil, stats, fmt.Errorf("invalid artifact file %s", name)
		}
		stats.Files++
		visited[name] = true
		entry, hit := x.files[name]
		if hit {
			if !sameArtifactFile(entry.info, info) {
				return nil, stats, fmt.Errorf("immutable artifact changed: %s", name)
			}
			stats.Hits++
		} else {
			path := filepath.Join(x.directory, name)
			raw, err := os.ReadFile(path)
			if err != nil {
				return nil, stats, err
			}
			stats.BytesRead += int64(len(raw))
			after, err := os.Stat(path)
			if err != nil {
				return nil, stats, err
			}
			if !sameArtifactFile(info, after) {
				return nil, stats, fmt.Errorf("artifact changed during read")
			}
			records, err := decodeMigrationArtifacts(name, raw, x.kind)
			if err != nil {
				return nil, stats, err
			}
			entry = artifactIndexEntry{after, records}
			cost := int64(len(raw) + 256*len(records))
			if cost <= x.limit-x.used {
				x.files[name] = entry
				x.used += cost
			}
		}
		for id, raw := range entry.records {
			if prior, ok := result[id]; ok {
				if !bytes.Equal(prior, raw) {
					return nil, stats, fmt.Errorf("conflicting migration artifact %s", id)
				}
				continue
			}
			result[id] = append([]byte(nil), raw...) // caller cannot mutate cache
		}
	}
	for name := range x.files {
		if !visited[name] {
			return nil, stats, fmt.Errorf("immutable artifact removed: %s", name)
		}
	}
	return result, stats, nil
}

func sameArtifactFile(a, b os.FileInfo) bool {
	return os.SameFile(a, b) && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}
