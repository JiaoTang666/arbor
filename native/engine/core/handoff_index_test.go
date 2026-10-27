package core

import (
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func indexFixture(t *testing.T, dir string, n uint64) *MigrationHandoff {
	t.Helper()
	a := "0000000000000000000000000000000000000010"
	tx := NewTransaction(a, a, big.NewInt(1), n, time.Unix(1, 0))
	tx.RoutingFrozen = true
	tx.IngressSequence = n
	h, e := BuildMigrationHandoff(tx, "index", a, 0, 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	if e = PersistMigrationHandoff(dir, h); e != nil {
		t.Fatal(e)
	}
	return h
}
func TestHandoffIndexIncrementalRecoveryAndIsolation(t *testing.T) {
	dir := t.TempDir()
	indexFixture(t, dir, 1)
	indexFixture(t, dir, 2)
	idx := NewHandoffIndex(dir, 1<<20)
	first, s, e := idx.Load(nil)
	if e != nil || len(first) != 2 || s.Validated != 2 {
		t.Fatal(s, e)
	}
	first[0].Original[0] ^= 1
	second, s, e := idx.Load(nil)
	if e != nil || s.Validated != 0 || s.BytesRead != 0 || s.CacheHits != 2 {
		t.Fatal(s, e)
	}
	if e = ValidateMigrationHandoff(second[0]); e != nil {
		t.Fatal("caller corrupted cache", e)
	}
	h := indexFixture(t, dir, 3)
	selected, s, e := idx.Load(func(x *MigrationHandoff) bool { return x.ID == h.ID })
	if e != nil || s.Validated != 1 || len(selected) != 1 {
		t.Fatal(s, e)
	}
	none, s, e := idx.Load(func(*MigrationHandoff) bool { return false })
	if e != nil || len(none) != 0 || s.BytesRead != 0 || s.Validated != 0 {
		t.Fatal(s, e)
	}
	legacy, e := LoadMigrationOutbox(dir)
	if e != nil {
		t.Fatal(e)
	}
	fresh, s, e := NewHandoffIndex(dir, 1<<20).Load(nil)
	if e != nil || s.Validated != 3 || !reflect.DeepEqual(legacy, fresh) {
		t.Fatal("restart differs", s, e)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, e := idx.Load(nil); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
}
func TestHandoffIndexFailureAndCapacity(t *testing.T) {
	t.Run("failed new file retries", func(t *testing.T) {
		dir := t.TempDir()
		indexFixture(t, dir, 1)
		idx := NewHandoffIndex(dir, 1<<20)
		if _, _, e := idx.Load(nil); e != nil {
			t.Fatal(e)
		}
		bad := filepath.Join(dir, "bad.handoff.json")
		if e := os.WriteFile(bad, []byte("broken"), 0600); e != nil {
			t.Fatal(e)
		}
		if rows, _, e := idx.Load(nil); e == nil || rows != nil {
			t.Fatal("partial result accepted")
		}
		if e := os.Remove(bad); e != nil {
			t.Fatal(e)
		}
		if rows, _, e := idx.Load(nil); e != nil || len(rows) != 1 {
			t.Fatal("retry failed", e)
		}
	})
	t.Run("changed or removed cached file rejected", func(t *testing.T) {
		dir := t.TempDir()
		h := indexFixture(t, dir, 1)
		idx := NewHandoffIndex(dir, 1<<20)
		if _, _, e := idx.Load(nil); e != nil {
			t.Fatal(e)
		}
		path := filepath.Join(dir, h.ID+".handoff.json")
		if e := os.Chtimes(path, time.Unix(1, 0), time.Unix(1, 0)); e != nil {
			t.Fatal(e)
		}
		if _, _, e := idx.Load(nil); e == nil {
			t.Fatal("changed file accepted")
		}
		if e := os.Remove(path); e != nil {
			t.Fatal(e)
		}
		if _, _, e := idx.Load(nil); e == nil {
			t.Fatal("removed file accepted")
		}
	})
	t.Run("zero capacity preserves work", func(t *testing.T) {
		dir := t.TempDir()
		indexFixture(t, dir, 1)
		idx := NewHandoffIndex(dir, 0)
		for i := 0; i < 2; i++ {
			r, s, e := idx.Load(nil)
			if e != nil || len(r) != 1 || s.Validated != 1 || s.CacheBytes != 0 {
				t.Fatal(s, e)
			}
		}
	})
}
func TestHandoffIndexArtifactReplay(t *testing.T) {
	dir := os.Getenv("HANDOFF_INDEX_REPLAY_DIR")
	if dir == "" {
		t.Skip("explicit saved artifact replay")
	}
	legacy, e := LoadMigrationOutbox(dir)
	if e != nil {
		t.Fatal(e)
	}
	idx := NewHandoffIndex(dir, 32<<20)
	cold, first, e := idx.Load(nil)
	if e != nil || !reflect.DeepEqual(cold, legacy) {
		t.Fatal("cold replay mismatch", e)
	}
	warm, second, e := idx.Load(nil)
	if e != nil || !reflect.DeepEqual(warm, legacy) || second.Validated != 0 || second.BytesRead != 0 {
		t.Fatal("warm replay mismatch", e)
	}
	completed, third, e := idx.Load(func(*MigrationHandoff) bool { return false })
	if e != nil || len(completed) != 0 || third.Validated != 0 {
		t.Fatal(e)
	}
	fresh, fourth, e := NewHandoffIndex(dir, 32<<20).Load(nil)
	if e != nil || !reflect.DeepEqual(fresh, legacy) {
		t.Fatal("restart replay mismatch", e)
	}
	report := map[string]interface{}{"status": "PASS", "records": len(legacy), "cold": first, "warm": second, "completed_filter": third, "restart": fourth, "scope": "saved native outbox replay, not a new native performance run"}
	raw, _ := json.MarshalIndent(report, "", "  ")
	if path := os.Getenv("HANDOFF_INDEX_REPLAY_REPORT"); path != "" {
		if e = os.WriteFile(path, raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
	t.Log(string(raw))
}
