package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestArtifactIndexIncrementalAndOwnedResults(t *testing.T) {
	dir := t.TempDir()
	kind := ".ack.json"
	write := func(i int) {
		id := fmt.Sprintf("%064x", i)
		raw := json.RawMessage(fmt.Sprintf(`{"ID":%q}`, id))
		if err := PublishMigrationArtifacts(dir, kind, MigrationArtifacts{id: raw}); err != nil {
			t.Fatal(err)
		}
	}
	write(1)
	x := NewMigrationArtifactIndex(dir, kind, 1<<20)
	a, first, err := x.Load()
	if err != nil || first.BytesRead == 0 {
		t.Fatal(first, err)
	}
	a[fmt.Sprintf("%064x", 1)][0] = 'x'
	b, second, err := x.Load()
	if err != nil || second.BytesRead != 0 || second.Hits != 1 {
		t.Fatal(second, err)
	}
	if b[fmt.Sprintf("%064x", 1)][0] != '{' {
		t.Fatal("cache mutated by caller")
	}
	write(2)
	c, third, err := x.Load()
	if err != nil || len(c) != 2 || third.Hits != 1 || third.BytesRead == 0 {
		t.Fatal(third, err)
	}
	reference, err := LoadMigrationArtifacts(dir, kind, false)
	if err != nil || !reflect.DeepEqual(c, reference) {
		t.Fatal("reference mismatch", err)
	}
	uncached := NewMigrationArtifactIndex(dir, kind, 0)
	d, _, err := uncached.Load()
	if err != nil || !reflect.DeepEqual(c, d) {
		t.Fatal("capacity changed result", err)
	}
}

func TestArtifactIndexRejectsMutationRemovalAndConflicts(t *testing.T) {
	for _, mode := range []string{"mutation", "removal", "conflict"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			id := fmt.Sprintf("%064x", 1)
			kind := ".ack.json"
			raw := []byte(fmt.Sprintf(`{"ID":%q,"x":1}`, id))
			path := filepath.Join(dir, id+kind)
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			x := NewMigrationArtifactIndex(dir, kind, 1<<20)
			if _, _, err := x.Load(); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "mutation":
				if err := os.WriteFile(path, append(raw, ' '), 0600); err != nil {
					t.Fatal(err)
				}
			case "removal":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "conflict":
				if err := PublishMigrationArtifacts(dir, kind, MigrationArtifacts{id: json.RawMessage(bytes.Replace(raw, []byte(`"x":1`), []byte(`"x":2`), 1))}); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := x.Load(); err == nil {
				t.Fatal("accepted", mode)
			}
		})
	}
}

func TestAuditCacheBindsProofAndDoesNotCacheEmptyQueue(t *testing.T) {
	dir := t.TempDir()
	h := indexFixture(t, dir, 1)
	x := NewHandoffIndex(dir, 1<<20)
	digest := sha256.Sum256(h.Execution)
	ack := HandoffAck{ID: h.ID, ProposalID: h.ProposalID, TargetShard: h.TargetShard, ExecutionHash: hex.EncodeToString(digest[:]), Block: 1, BlockHash: strings.Repeat("1", 64), StateRoot: strings.Repeat("2", 64), Executed: true}
	path := filepath.Join(dir, h.ID+".confirmed.json")
	write := func() {
		raw, _ := json.Marshal(ack)
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	members := map[string]string{h.Account: h.ProposalID}
	for i := 0; i < 2; i++ {
		a, err := x.AuditMembers(members)
		if err != nil || a[h.Account].Confirmed != 1 {
			t.Fatal(a, err)
		}
	}
	indexFixture(t, dir, 2)
	a, err := x.AuditMembers(members)
	if err != nil || len(a[h.Account].Pending) != 1 {
		t.Fatal("late work hidden", a, err)
	}
	ack.Executed = false
	write()
	if _, err := x.AuditMembers(members); err == nil {
		t.Fatal("changed invalid proof reused cached success")
	}
}

// Optional read-only replay of a real completed outbox; never launches nodes.
func TestCompletedRunAuditIndexReplay(t *testing.T) {
	dir := os.Getenv("SAA_READONLY_AUDIT_REPLAY_DIR")
	if dir == "" {
		t.Skip("no saved outbox specified")
	}
	x := NewHandoffIndex(dir, 32<<20)
	records, _, err := x.Load(nil)
	if err != nil {
		t.Fatal(err)
	}
	members := map[string]string{}
	for _, h := range records {
		members[h.Account] = h.ProposalID
	}
	start := time.Now()
	want, err := AuditMigrationHandoffsForMembers(dir, members)
	if err != nil {
		t.Fatal(err)
	}
	reference := time.Since(start)
	start = time.Now()
	got, err := x.AuditMembers(members)
	elapsed := time.Since(start)
	if err != nil || !reflect.DeepEqual(want, got) {
		t.Fatal("audit mismatch", err)
	}
	start = time.Now()
	warm, err := x.AuditMembers(members)
	warmTime := time.Since(start)
	if err != nil || !reflect.DeepEqual(want, warm) {
		t.Fatal("warm audit mismatch", err)
	}
	t.Logf("records=%d members=%d full_audit=%s indexed_cold=%s indexed_warm=%s exact_equal=true", len(records), len(members), reference, elapsed, warmTime)
}
