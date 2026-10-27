package core

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSourceHandoffBatchSinglePublicationAndRestart(t *testing.T) {
	dir := t.TempDir()
	hs := []*MigrationHandoff{}
	for i := uint64(1); i <= 142; i++ {
		hs = append(hs, publisherFixture(t, i))
	}
	calls := 0
	publish := func(d, k string, items MigrationArtifacts) error {
		calls++
		return PublishMigrationArtifacts(d, k, items)
	}
	if err := publishSourceHandoffBatch(dir, hs, publish); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.handoff.json.bundle"))
	if calls != 1 || len(files) != 1 {
		t.Fatal("not a single durable batch", calls, len(files))
	}
	index := NewHandoffIndex(dir, 32<<20)
	records, _, err := index.Load(nil)
	if err != nil || len(records) != 142 {
		t.Fatal("bundle not recoverable", len(records), err)
	}
	if err = publishSourceHandoffBatch(dir, hs, publish); err != nil || calls != 2 {
		t.Fatal("retry skipped durability", calls, err)
	}
}

func TestSourceHandoffBatchPrevalidatesTailAndConflicts(t *testing.T) {
	dir := t.TempDir()
	a, b := publisherFixture(t, 1), publisherFixture(t, 2)
	b.ID = "invalid"
	calls := 0
	spy := func(string, string, MigrationArtifacts) error { calls++; return nil }
	if publishSourceHandoffBatch(dir, []*MigrationHandoff{a, b}, spy) == nil || calls != 0 {
		t.Fatal("invalid tail partially published")
	}
	if PublishSourceHandoffBatch(dir, []*MigrationHandoff{a, a}) == nil {
		t.Fatal("duplicate accepted")
	}
	if err := os.WriteFile(filepath.Join(dir, a.ID+".handoff.json"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if publishSourceHandoffBatch(dir, []*MigrationHandoff{a}, spy) == nil || calls != 0 {
		t.Fatal("corrupt legacy ignored")
	}
}

func TestSourceHandoffBatchUncertainPersistenceRetries(t *testing.T) {
	dir := t.TempDir()
	hs := []*MigrationHandoff{publisherFixture(t, 1)}
	calls := 0
	publish := func(d, k string, items MigrationArtifacts) error {
		calls++
		if err := PublishMigrationArtifacts(d, k, items); err != nil {
			return err
		}
		if calls == 1 {
			return errors.New("uncertain directory barrier")
		}
		return nil
	}
	if publishSourceHandoffBatch(dir, hs, publish) == nil {
		t.Fatal("failure swallowed")
	}
	if err := publishSourceHandoffBatch(dir, hs, publish); err != nil || calls != 2 {
		t.Fatal("retry failed", calls, err)
	}
}
