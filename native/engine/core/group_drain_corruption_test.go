package core

import (
	"bytes"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGroupDrainDoesNotNormalizeCorruptKind(t *testing.T) {
	for _, kind := range []string{"bogus", "append", "prepend"} {
		t.Run(kind, func(t *testing.T) {
			accounts := []string{strings.Repeat("0", 39) + "a", strings.Repeat("0", 39) + "b"}
			p, dir := NewTxPool(), t.TempDir()
			if err := p.EnableGroupMigrationQueueJournal(dir, accounts); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "00000000000000000001.queue.json")
			bad, _ := json.Marshal(migrationQueueSnapshot{Accounts: accounts, Sequence: 1, Kind: kind})
			if err := os.WriteFile(path, bad, 0600); err != nil {
				t.Fatal(err)
			}
			raw, _, err := p.TryGroupDrainSnapshot(accounts)
			if err == nil || raw != nil {
				t.Fatal("corrupt journal was certified drained")
			}
			after, _ := os.ReadFile(path)
			files, _ := os.ReadDir(dir)
			if !bytes.Equal(after, bad) || len(files) != 1 || p.migrationJournal.sequence != 1 {
				t.Fatal("failure concealed original corruption")
			}
		})
	}
}

func TestGroupDrainDeltaToFullDoesNotRetainKind(t *testing.T) {
	a := "0000000000000000000000000000000000000010"
	b := "0000000000000000000000000000000000000020"
	p, dir := NewTxPool(), t.TempDir()
	if err := p.EnableGroupMigrationQueueJournal(dir, []string{a, b}); err != nil {
		t.Fatal(err)
	}
	tx := NewTransaction("0000000000000000000000000000000000000030", a, big.NewInt(1), 1, time.Unix(1, 0))
	tx.RoutingFrozen = true
	tx.RoutedSenderShard, tx.RoutedRecipientShard = 6, 2
	p.AddTx2Pool(tx) // last generation is an append delta
	if len(p.PackTxs(100)) != 1 {
		t.Fatal("ordinary new-route transaction not packed")
	}
	// As in the existing drain test, the next proposal follows block commit.
	raw, detail, err := p.TryGroupDrainSnapshot([]string{a, b})
	if err != nil || raw == nil || detail.Status != "ready" {
		t.Fatalf("delta Kind leaked into full snapshot: %+v %v", detail, err)
	}
	fresh := NewTxPool()
	if err = fresh.EnableGroupMigrationQueueJournal(dir, []string{a, b}); err != nil || fresh.GetTxQueueLen() != 0 {
		t.Fatal("full snapshot failed recovery", err)
	}
}
