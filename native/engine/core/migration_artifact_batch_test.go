package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func artifactTestName(raw []byte, kind string) string {
	s := sha256.Sum256(raw)
	return "batch-" + hex.EncodeToString(s[:]) + kind + ".bundle"
}

func TestMigrationArtifactsRoundTripRetryAndLegacy(t *testing.T) {
	for _, kind := range []string{".ack.json", ".relay-ack.json", ".relay.json", ".handoff.json"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			items := MigrationArtifacts{}
			for _, c := range []string{"a", "b"} {
				id := strings.Repeat(c, 64)
				raw := `{"ID":"` + id + `"}`
				if kind == ".relay.json" {
					raw = `{"Handoff":` + raw + `}`
				}
				items[id] = json.RawMessage(raw)
			}
			for i := 0; i < 2; i++ {
				if e := PublishMigrationArtifacts(dir, kind, items); e != nil {
					t.Fatal(e)
				}
			}
			files, _ := os.ReadDir(dir)
			if len(files) != 1 {
				t.Fatal("retry created extra files", len(files))
			}
			// A temp fragment must not hide the already durable batch.
			if e := os.WriteFile(filepath.Join(dir, ".sef-prepare-partial"), []byte("{"), 0600); e != nil {
				t.Fatal(e)
			}
			id := items.IDs()[0]
			if e := os.WriteFile(filepath.Join(dir, id+kind), items[id], 0600); e != nil {
				t.Fatal(e)
			}
			got, e := LoadMigrationArtifacts(dir, kind, false)
			if e != nil || len(got) != 2 {
				t.Fatal(e, len(got))
			}
			if _, e = got.Read(strings.Repeat("c", 64)); !os.IsNotExist(e) {
				t.Fatal("missing identity did not remain missing")
			}
			// Well-formed but conflicting legacy content cannot mask the bundle.
			raw := strings.TrimSuffix(string(items[id]), "}") + `,"extra":1}`
			if e = os.WriteFile(filepath.Join(dir, id+kind), []byte(raw), 0600); e != nil {
				t.Fatal(e)
			}
			if got, e = LoadMigrationArtifacts(dir, kind, false); e == nil || got != nil {
				t.Fatal("conflicting publication accepted")
			}
		})
	}
}

func TestMigrationArtifactRejectsCorruptionAndSchema(t *testing.T) {
	id := strings.Repeat("a", 64)
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"batch-false.ack.json.bundle", []byte(`{}`)},
		{id + ".ack.json", []byte(`{"ID":"bad"}`)},
		{id + ".ack.json", []byte(`{`)},
	} {
		if _, e := decodeMigrationArtifacts(tc.name, tc.raw, ".ack.json"); e == nil {
			t.Fatal("accepted malformed artifact", tc.name)
		}
	}
	if e := PublishMigrationArtifacts(t.TempDir(), ".ack.json", MigrationArtifacts{id: json.RawMessage(`{"ID":"` + strings.Repeat("b", 64) + `"}`)}); e == nil {
		t.Fatal("wrong identity accepted")
	}
	for _, batch := range []migrationArtifactBatch{
		{Schema: "wrong", Kind: ".ack.json", Items: []migrationArtifact{{ID: id, Payload: json.RawMessage(`{"ID":"` + id + `"}`)}}},
		{Schema: artifactBatchSchema, Kind: ".relay.json"},
		{Schema: artifactBatchSchema, Kind: ".ack.json"},
		{Schema: artifactBatchSchema, Kind: ".ack.json", Items: []migrationArtifact{{ID: id, Payload: json.RawMessage(`{"ID":"` + id + `"}`)}, {ID: id, Payload: json.RawMessage(`{"ID":"` + id + `"}`)}}},
	} {
		raw, _ := json.Marshal(batch)
		// Matching content hash must not bypass schema or duplicate validation.
		name := artifactTestName(raw, ".ack.json")
		if _, e := decodeMigrationArtifacts(name, raw, ".ack.json"); e == nil {
			t.Fatal("invalid envelope accepted")
		}
	}
}

func TestHandoffBundleIndexRecovery(t *testing.T) {
	dir := t.TempDir()
	h := indexFixture(t, dir, 1)
	raw, _ := json.Marshal(h)
	if e := PublishMigrationArtifacts(dir, ".handoff.json", MigrationArtifacts{h.ID: raw}); e != nil {
		t.Fatal(e)
	}
	if e := os.Remove(filepath.Join(dir, h.ID+".handoff.json")); e != nil {
		t.Fatal(e)
	}
	index := NewHandoffIndex(dir, 1<<20)
	for i := 0; i < 2; i++ {
		rows, obs, e := index.Load(nil)
		if e != nil || len(rows) != 1 {
			t.Fatal(obs, e)
		}
		if i == 1 && (obs.BytesRead != 0 || obs.CacheHits != 1) {
			t.Fatal("bundle cache not used", obs)
		}
		rows[0].Original[0] ^= 1
	}
	rows, e := LoadMigrationOutbox(dir)
	if e != nil || len(rows) != 1 {
		t.Fatal(e)
	}
	fresh, _, e := NewHandoffIndex(dir, 1<<20).Load(nil)
	if e != nil || len(fresh) != 1 {
		t.Fatal(e)
	}
	if e = ValidateMigrationHandoff(fresh[0]); e != nil {
		t.Fatal(e)
	}
}
