package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const artifactBatchSchema = "sef_migration_artifacts_v1"
const artifactBatchLimit = 32 << 20

type MigrationArtifacts map[string]json.RawMessage
type migrationArtifact struct {
	ID      string
	Payload json.RawMessage
}
type migrationArtifactBatch struct {
	Schema string
	Kind   string
	Items  []migrationArtifact
}

func validArtifactKind(kind string) bool {
	return kind == ".ack.json" || kind == ".relay-ack.json" || kind == ".relay.json" || kind == ".handoff.json" || kind == ".redirect.json"
}
func artifactIdentity(raw []byte, kind string) (string, error) {
	var v struct {
		ID      string
		Handoff struct{ ID string }
		Record  struct {
			First struct{ Handoff struct{ ID string } }
		}
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return "", err
	}
	id := v.ID
	if kind == ".redirect.json" {
		id = v.Record.First.Handoff.ID
	}
	if kind == ".relay.json" {
		id = v.Handoff.ID
	}
	b, err := hex.DecodeString(id)
	if err != nil || len(b) != 32 || id != strings.ToLower(id) {
		return "", fmt.Errorf("invalid migration artifact identity")
	}
	return id, nil
}
func (a MigrationArtifacts) add(id string, raw []byte, kind string) error {
	actual, err := artifactIdentity(raw, kind)
	if err != nil {
		return err
	}
	if actual != id {
		return fmt.Errorf("migration artifact identity mismatch")
	}
	var compact bytes.Buffer
	if err = json.Compact(&compact, raw); err != nil {
		return err
	}
	if old, ok := a[id]; ok && !bytes.Equal(old, compact.Bytes()) {
		return fmt.Errorf("conflicting migration artifact %s", id)
	}
	a[id] = append([]byte(nil), compact.Bytes()...)
	return nil
}
func (a MigrationArtifacts) Read(id string) ([]byte, error) {
	raw, ok := a[id]
	if !ok {
		return nil, os.ErrNotExist
	}
	return raw, nil
}
func (a MigrationArtifacts) IDs() []string {
	ids := make([]string, 0, len(a))
	for id := range a {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// A content-addressed immutable envelope is the publication unit. Partial
// temporary files are invisible. Retries verify content and re-sync durability.
func PublishMigrationArtifacts(directory, kind string, items MigrationArtifacts) error {
	if !validArtifactKind(kind) || directory == "" || len(items) == 0 {
		return fmt.Errorf("invalid artifact batch")
	}
	batch := migrationArtifactBatch{Schema: artifactBatchSchema, Kind: kind}
	for _, id := range items.IDs() {
		check := MigrationArtifacts{}
		if err := check.add(id, items[id], kind); err != nil {
			return err
		}
		batch.Items = append(batch.Items, migrationArtifact{ID: id, Payload: check[id]})
	}
	raw, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	if len(raw) > artifactBatchLimit {
		return fmt.Errorf("artifact batch exceeds size limit")
	}
	hash := sha256.Sum256(raw)
	name := "batch-" + hex.EncodeToString(hash[:]) + kind + ".bundle"
	return PublishSourcePrepareFile(filepath.Join(directory, name), raw)
}

// Load legacy records and/or new envelopes once per poll, not once per item.
// Structural checks do not replace each consumer's state/route/ACK validation.
func LoadMigrationArtifacts(directory, kind string, bundlesOnly bool) (MigrationArtifacts, error) {
	result := MigrationArtifacts{}
	if !validArtifactKind(kind) {
		return nil, fmt.Errorf("unknown artifact kind")
	}
	if directory == "" {
		return result, nil
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		bundle := strings.HasPrefix(entry.Name(), "batch-") && strings.HasSuffix(entry.Name(), kind+".bundle")
		legacy := !bundlesOnly && strings.HasSuffix(entry.Name(), kind)
		if !bundle && !legacy {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Size() > artifactBatchLimit {
			return nil, fmt.Errorf("invalid artifact file %s", entry.Name())
		}
		raw, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, err
		}
		records, err := decodeMigrationArtifacts(entry.Name(), raw, kind)
		if err != nil {
			return nil, err
		}
		for id, payload := range records {
			if err = result.add(id, payload, kind); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func decodeMigrationArtifacts(name string, raw []byte, kind string) (MigrationArtifacts, error) {
	result := MigrationArtifacts{}
	if len(raw) > artifactBatchLimit {
		return nil, fmt.Errorf("artifact batch exceeds size limit")
	}
	if strings.HasSuffix(name, kind) {
		err := result.add(strings.TrimSuffix(name, kind), raw, kind)
		return result, err
	}
	sum := sha256.Sum256(raw)
	if name != "batch-"+hex.EncodeToString(sum[:])+kind+".bundle" {
		return nil, fmt.Errorf("artifact batch content hash mismatch")
	}
	var b migrationArtifactBatch
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, err
	}
	if b.Schema != artifactBatchSchema || b.Kind != kind || len(b.Items) == 0 {
		return nil, fmt.Errorf("invalid artifact batch schema/kind/count")
	}
	seen := map[string]bool{}
	for _, item := range b.Items {
		if seen[item.ID] {
			return nil, fmt.Errorf("duplicate batch identity")
		}
		seen[item.ID] = true
		if err := result.add(item.ID, item.Payload, kind); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func ReadMigrationArtifact(directory, id, kind string) ([]byte, error) {
	records, err := LoadMigrationArtifacts(directory, kind, false)
	if err != nil {
		return nil, err
	}
	return records.Read(id)
}
